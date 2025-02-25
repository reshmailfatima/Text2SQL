# db.py
import pyodbc
from typing import List, Dict, Any
from fastapi import HTTPException
import logging
import re

logger = logging.getLogger(__name__)

class DatabaseConnection:
    def __init__(self):
        self.connection_string = (
            "DRIVER={SQL Server};"
            "SERVER=PKLAHLTPG3A;"
            "DATABASE=school;"  # Connecting to school database
            "Trusted_Connection=yes;"
        )
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute SQL query with simple syntax corrections
        """
        try:
            # Store original query for logging
            original_query = query
            
            # Remove backticks (MySQL style) from the query
            query = query.replace('`', '')
            
            # Simplify query - replace 'schools.details' with just 'details'
            simplified_query = re.sub(
                r'schools\.([a-zA-Z0-9_]+)', 
                r'\1', 
                query, 
                flags=re.IGNORECASE
            )
            
            # Log the transformation if query was changed
            if simplified_query != original_query:
                logger.info(f"Transformed query from: {original_query}")
                logger.info(f"To: {simplified_query}")
                query = simplified_query
            
            with pyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                
                # Execute the query
                cursor.execute(query)
                
                # Process results if it's a SELECT query
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()
                    results = []
                    for row in rows:
                        processed_row = [
                            str(item) if isinstance(item, (bytes, bytearray)) else 
                            item.strftime('%Y-%m-%d') if hasattr(item, 'strftime') else 
                            item 
                            for item in row
                        ]
                        results.append(dict(zip(columns, processed_row)))
                    return results
                
                # For non-SELECT queries, return success message
                conn.commit()  # Ensure changes are committed
                return [{"message": "Query executed successfully"}]
                
        except pyodbc.Error as e:
            # Log the full error with query details
            logger.error(f"Database error executing query: {query}")
            logger.error(f"Error details: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Database error: {str(e)}")

# Create database instance
db = DatabaseConnection()

def execute_sql(query: str) -> List[Dict[str, Any]]:
    """Execute SQL query against the database"""
    return db.execute_query(query)