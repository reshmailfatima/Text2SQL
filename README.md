# Text-to-SQL Multi-Agent System

A FastAPI-based system that converts natural language queries to SQL using CrewAI multi-agent orchestration and a local LLM (TinyLlama).

## Overview

This project implements a natural language to SQL conversion system that allows users to query a SQL Server database using plain English. The system uses:

- **FastAPI** for the backend API
- **CrewAI** for multi-agent orchestration
- **TinyLlama** (via Ollama) for natural language understanding and SQL generation
- **pyodbc** for SQL Server connectivity

## Key Features

- Natural language query processing
- SQL generation for SELECT, UPDATE, INSERT, and DELETE operations
- Support for basic filtering conditions
- Simple pattern matching for common query types
- SQL validation and error handling
- Database schema extraction and utilization

## Project Structure

```
text-to-sql/
│
├── main.py                  # FastAPI application and main logic
├── db.py                    # Database connection and query execution
├── explore_schema.py        # Database schema extraction utility
├── requirements.txt         # Project dependencies
├── db_schema.json           # Extracted database schema (generated)
└── README.md                # This file
```

## Setup Instructions

### Prerequisites

- Python 3.7+
- SQL Server database
- [Ollama](https://ollama.ai/) running with TinyLlama model

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/text-to-sql.git
   cd text-to-sql
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure your database connection in `db.py`:
   ```python
   self.connection_string = (
       "DRIVER={SQL Server};"
       "SERVER=your_server_name;"
       "DATABASE=your_database_name;"
       "Trusted_Connection=yes;"
   )
   ```

4. Make sure Ollama is running with TinyLlama:
   ```bash
   ollama run tinyllama
   ```

5. Extract your database schema:
   ```bash
   python explore_schema.py
   ```

6. Start the API server:
   ```bash
   python main.py
   ```

## Usage

### API Endpoint

The main endpoint is:

```
POST /query/
```

### Sample Request

```bash
curl -X 'POST' \
  'http://127.0.0.1:8000/query/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "query": "show all teachers"
}'
```

### Sample Response

```json
{
  "sql_query": "SELECT * FROM teachers;",
  "is_valid": true,
  "results": [
    {"id": 1, "name": "John Smith", "subject": "Math"},
    {"id": 2, "name": "Jane Doe", "subject": "Science"}
  ],
  "error": null
}
```

## CrewAI Agents

The system uses four specialized agents:

1. **NLU Agent**: Handles natural language understanding
2. **SQL Generator**: Creates SQL queries from natural language
3. **Validator Agent**: Validates and optimizes queries
4. **Executor Agent**: Executes SQL and processes results


## License

MIT

## Contributors

- Reshmail Fatima

## Acknowledgements

- [CrewAI](https://github.com/joaomdmoura/crewAI)
- [FastAPI](https://fastapi.tiangolo.com/)
- [Ollama](https://ollama.ai/)
