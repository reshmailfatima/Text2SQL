# school_schema.py
import pyodbc
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_school_schema():
    """Extract schema information specifically from the school database"""
    connection_string = (
        "DRIVER={SQL Server};"
        "SERVER=PKLAHLTPG3A;"
        "DATABASE=school;"  # Connect directly to school database
        "Trusted_Connection=yes;"
    )
    
    try:
        schema = {}
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            
            # Get all tables in the school database
            cursor.execute("""
            SELECT 
                t.TABLE_SCHEMA,
                t.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.IS_NULLABLE,
                CASE 
                    WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES'
                    ELSE 'NO'
                END AS IS_PRIMARY_KEY
            FROM 
                INFORMATION_SCHEMA.TABLES t
            INNER JOIN 
                INFORMATION_SCHEMA.COLUMNS c 
                ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
            LEFT JOIN 
                (
                    SELECT 
                        ku.TABLE_SCHEMA,
                        ku.TABLE_NAME,
                        ku.COLUMN_NAME
                    FROM 
                        INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                            ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                            AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
                    WHERE 
                        tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ) pk 
                ON t.TABLE_NAME = pk.TABLE_NAME 
                AND c.COLUMN_NAME = pk.COLUMN_NAME
                AND t.TABLE_SCHEMA = pk.TABLE_SCHEMA
            WHERE 
                t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY 
                t.TABLE_SCHEMA,
                t.TABLE_NAME,
                c.ORDINAL_POSITION;
            """)
            
            rows = cursor.fetchall()
            
            # Process results
            for row in rows:
                schema_name = row[0]
                table_name = row[1]
                full_table_name = table_name
                
                column_info = {
                    'name': row[2],
                    'type': row[3],
                    'max_length': row[4],
                    'nullable': row[5] == 'YES',
                    'is_primary_key': row[6] == 'YES'
                }
                
                if full_table_name not in schema:
                    schema[full_table_name] = {'columns': []}
                schema[full_table_name]['columns'].append(column_info)
            
            # Prepare formatted schema for LLM prompting
            formatted_schema = "Database Schema for 'school':\n"
            for table_name, details in schema.items():
                formatted_schema += f"\nTable: {table_name}\nColumns:\n"
                for col in details['columns']:
                    nullable = "NULL" if col['nullable'] else "NOT NULL"
                    pk = "PRIMARY KEY" if col['is_primary_key'] else ""
                    type_info = f"{col['type']}"
                    if col['max_length']:
                        type_info += f"({col['max_length']})"
                    formatted_schema += f"- {col['name']} ({type_info}) {nullable} {pk}\n"
            
            # Print the formatted schema for easy copying
            logger.info("\nFormatted Schema for LLM Prompting:")
            logger.info(formatted_schema)
        
        # Write schema to file
        with open('school_schema.json', 'w') as f:
            json.dump(schema, f, indent=2)
        logger.info("\nSchema saved to school_schema.json")
        
        return schema
            
    except Exception as e:
        logger.error(f"Error fetching schema: {str(e)}")
        return None

if __name__ == "__main__":
    extract_school_schema()