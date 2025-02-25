# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import uvicorn
from crewai import Agent, Task, Crew, Process
import requests
import logging
import re
from db import DatabaseConnection, execute_sql

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class NaturalQuery(BaseModel):
    query: str

class SQLResponse(BaseModel):
    sql_query: str
    is_valid: Optional[bool] = None
    results: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None

def extract_sql_query(response: str) -> Optional[str]:
    """Extract SQL query from LLM response with enhanced support for all query types"""
    logger.info(f"Raw LLM response: {response}")
    
    # Remove backticks and code blocks
    clean_response = response.replace('```sql', '').replace('```', '').strip()
    
    # Enhanced SQL patterns with more flexible matching
    patterns = [
        # SELECT patterns
        r'(SELECT\s+[\w\s,\*\.]+\s+FROM\s+[\w\s,\.]+(?:\s+WHERE\s+.+?)?(?:\s+ORDER\s+BY\s+.+?)?(?:\s+LIMIT\s+\d+)?;)',
        r'(SELECT\s+.+?;)',
        
        # UPDATE patterns - enhanced to better capture various formats
        r'(UPDATE\s+[\w\.]+\s+SET\s+[\w\s=,\'"\-\+\.]+(?:\s+WHERE\s+.+?)?;)',
        r'(UPDATE\s+.+?;)',
        
        # INSERT patterns
        r'(INSERT\s+INTO\s+[\w\.]+\s*\([^)]+\)\s*VALUES\s*\([^)]+\);)',
        r'(INSERT\s+INTO\s+[\w\.]+\s+VALUES\s*\([^)]+\);)',
        r'(INSERT\s+INTO\s+.+?;)',
        
        # DELETE patterns
        r'(DELETE\s+FROM\s+[\w\.]+(?:\s+WHERE\s+.+?)?;)',
        r'(DELETE\s+.+?;)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, clean_response, re.IGNORECASE | re.DOTALL)
        if match:
            query = match.group(1).strip()
            logger.info(f"Extracted query: {query}")
            return query
    
    # If no pattern matches but response still looks like SQL
    sql_keywords = ['SELECT', 'UPDATE', 'INSERT', 'DELETE']
    for keyword in sql_keywords:
        if clean_response.upper().startswith(keyword):
            # Add semicolon if missing
            if not clean_response.rstrip().endswith(';'):
                clean_response += ';'
            logger.info(f"Using cleaned response as {keyword} query: {clean_response}")
            return clean_response
    
    logger.warning("No SQL query found in response")
    return None

def identify_query_type(query: str) -> str:
    """Identify the type of SQL query"""
    if not query:
        return "UNKNOWN"
    
    query_upper = query.upper().strip()
    
    if query_upper.startswith("SELECT"):
        return "SELECT"
    elif query_upper.startswith("UPDATE"):
        return "UPDATE"
    elif query_upper.startswith("INSERT"):
        return "INSERT"
    elif query_upper.startswith("DELETE"):
        return "DELETE"
    else:
        return "UNKNOWN"

def remove_unwanted_where_clause(sql_query: str) -> str:
    """Remove WHERE clauses from SQL when not needed"""
    if sql_query is None:
        return None
        
    # For simple SELECT queries with unnecessary WHERE clauses
    if sql_query.upper().startswith("SELECT"):
        # Find the position of the FROM clause
        from_pos = sql_query.upper().find("FROM")
        if from_pos != -1:
            # Find the position of the WHERE clause
            where_pos = sql_query.upper().find("WHERE", from_pos)
            if where_pos != -1:
                # Return just the part before the WHERE
                clean_query = sql_query[:where_pos].strip()
                # Make sure it ends with a semicolon
                if not clean_query.endswith(";"):
                    clean_query += ";"
                logger.info(f"Removed unwanted WHERE clause. New query: {clean_query}")
                return clean_query
    
    # Return original if no changes needed
    return sql_query

def validate_query_intent(query_text: str, sql_query: str) -> str:
    """Validate that the SQL query matches the natural language intent with improved filter handling"""
    if not query_text or not sql_query:
        return sql_query
        
    query_lower = query_text.lower()
    
    # Look for keywords that indicate filtering is expected
    filter_keywords = [
        "whose", "where", "with", "has", "contains", "starts with", "ends with",
        "above", "below", "greater than", "less than", "equal to", "is", "are",
        "before", "after", "between", "like", "matches"
    ]
    
    has_filter_intent = any(keyword in query_lower for keyword in filter_keywords)
    
    # Only remove WHERE clauses for "show all" when there's no filter intent
    if ("show all" in query_lower or "get all" in query_lower):
        if has_filter_intent and "where" not in sql_query.lower():
            # Missing WHERE clause when filtering was requested
            logger.warning(f"Query requested filtering but no WHERE clause found: {sql_query}")
            # Here we could attempt to add a WHERE clause, but that's more complex
            return sql_query
        elif not has_filter_intent and "where" in sql_query.lower():
            # Unwanted WHERE clause when no filtering was requested
            logger.warning(f"Query did not request filtering but WHERE clause found: {sql_query}")
            return remove_unwanted_where_clause(sql_query)
    
    return sql_query

def generate_sql_with_llm(query: str) -> str:
    """Generate SQL using TinyLlama via Ollama with improved natural language understanding"""
    try:
        # Read schema from file
        schema_context = ""
        try:
            with open('db_schema.json', 'r') as f:
                import json
                schema = json.load(f)
                schema_context = "Database Schema:\n"
                for table, details in schema.items():
                    schema_context += f"\nTable: {table}\nColumns:\n"
                    for col in details['columns']:
                        nullable = "NULL" if col['nullable'] else "NOT NULL"
                        pk = "PRIMARY KEY" if col.get('is_primary_key') else ""
                        schema_context += f"- {col['name']} ({col['type']}) {nullable} {pk}\n"
        except Exception as e:
            logger.warning(f"Failed to load schema: {e}")

        # Add explicit examples for different filter scenarios
        examples = """
EXAMPLE QUERIES:
1. "Show all schools"
   SELECT * FROM schools;

2. "Show schools with rating above 4"
   SELECT * FROM schools WHERE rating > 4;
   
3. "Show all schools whose name starts with A"
   SELECT * FROM schools WHERE name LIKE 'A%';
   
4. "Show all schools that have rating equal to 5"
   SELECT * FROM schools WHERE rating = 5;

5. "Update rating to 5 for school with id 3"
   UPDATE schools SET rating = 5 WHERE id = 3;

6. "Add a new school named 'Excellence Academy' with rating 4.8"
   INSERT INTO schools (name, rating) VALUES ('Excellence Academy', 4.8);

7. "Delete the school with id 10"
   DELETE FROM schools WHERE id = 10;

IMPORTANT RULES:
- Only include WHERE clauses when a filter condition is specified in the query
- If the query mentions "starts with", "whose", "with", etc., a WHERE clause is needed
- If the user just asks for "all schools" without any conditions, do not add a WHERE clause
- Pay close attention to filter words like "whose", "where", "with", etc.
"""

        prompt = f"""
{schema_context}

{examples}

Convert this natural language query to SQL. Use ONLY the tables and columns from the schema above.
The query might require a SELECT, UPDATE, INSERT, or DELETE statement.

Query: {query}

Rules:
1. Only use tables and columns that exist in the schema
2. Use exact column names as shown in schema
3. Ensure proper SQL syntax
4. Handle UPDATE, INSERT, and DELETE operations correctly
5. Pay special attention to filtering criteria:
   - Include WHERE clauses when the user specifies conditions (e.g., "whose name", "with rating", etc.)
   - Do NOT include WHERE clauses when the user just wants all records without conditions
6. Return ONLY the SQL query, no explanations

SQL Query:"""

        response = requests.post(
            "http://10.253.1.172:8000/generate",
            headers={"Content-Type": "application/json"},
            json={
                "model": "tinyllama",
                "prompt": prompt,
                "system": "You are an advanced SQL query generator. Generate SQL queries that match exactly what was asked. Pay careful attention to when filters are needed vs. when all records are requested.",
                "temperature": 0.1,
                "max_tokens": 250
            }
        )
        
        if response.status_code == 200:
            llm_response = response.json().get('response', '').strip()
            sql_query = extract_sql_query(llm_response)
            
            if sql_query:
                # Apply additional validation
                sql_query = validate_query_intent(query, sql_query)
                
            return sql_query if sql_query else None
        return None
    except Exception as e:
        logger.error(f"SQL generation error: {str(e)}")
        return None

# Create specialized agents
def create_agents():
    nlu_agent = Agent(
        name="NLU Agent",
        role="Natural Language Understanding Specialist",
        goal="Understand and preprocess natural language queries for all SQL operations",
        backstory="Expert in parsing and understanding natural language queries for database operations",
        allow_delegation=True
    )

    sql_agent = Agent(
        name="SQL Generator",
        role="SQL Query Generator",
        goal="Generate accurate SQL queries from processed natural language for all operation types",
        backstory="Specialist in converting natural language to SELECT, UPDATE, INSERT, and DELETE SQL queries using TinyLlama",
        allow_delegation=True
    )

    validation_agent = Agent(
        name="Query Validator",
        role="SQL Query Validator",
        goal="Validate and optimize all types of SQL queries",
        backstory="Expert in SQL validation and optimization for all database operations",
        allow_delegation=True
    )

    execution_agent = Agent(
        name="DB Executor",
        role="Database Query Executor",
        goal="Safely execute and handle all types of SQL queries",
        backstory="Specialist in database operations and error handling for all query types",
        allow_delegation=True
    )

    return [nlu_agent, sql_agent, validation_agent, execution_agent]

# Create crew tasks
def create_tasks(agents):
    tasks = [
        Task(
            description="Analyze and preprocess the natural language query, identifying operation type",
            agent=agents[0],
            expected_output="Preprocessed query with identified operation type",
            output_format="Text with identified intent and key parameters"
        ),
        Task(
            description="Generate appropriate SQL query (SELECT, UPDATE, INSERT, or DELETE)",
            agent=agents[1],
            expected_output="Valid SQL query for the required operation",
            output_format="SQL query string matching the database schema"
        ),
        Task(
            description="Validate and optimize the generated SQL query",
            agent=agents[2],
            expected_output="Validated and optimized SQL query",
            output_format="Validated SQL query string with optimizations"
        ),
        Task(
            description="Execute the validated SQL query and handle results",
            agent=agents[3],
            expected_output="Query results or operation status",
            output_format="JSON formatted results or status message"
        )
    ]
    return tasks

@app.post("/query/", response_model=SQLResponse)
async def process_query(query: NaturalQuery):
    try:
        # Generate SQL
        sql_query = generate_sql_with_llm(query.query)
        
        if not sql_query:
            return SQLResponse(
                sql_query="",
                is_valid=False,
                error="Failed to generate valid SQL query"
            )

        # Additional validation step
        sql_query = validate_query_intent(query.query, sql_query)

        # Identify query type for logging
        query_type = identify_query_type(sql_query)
        logger.info(f"Query type: {query_type}")
        
        # Execute query
        try:
            results = execute_sql(sql_query)
            
            # For non-SELECT queries, results might be empty, so add feedback
            if query_type != "SELECT" and (not results or len(results) == 0):
                results = [{"message": f"{query_type} operation completed successfully"}]
                
            return SQLResponse(
                sql_query=sql_query,
                is_valid=True,
                results=results
            )
        except Exception as db_error:
            return SQLResponse(
                sql_query=sql_query,
                is_valid=False,
                error=str(db_error)
            )

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return SQLResponse(
            sql_query="",
            is_valid=False,
            error=str(e)
        )

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)