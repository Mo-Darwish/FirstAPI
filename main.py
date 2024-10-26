from fastapi import FastAPI
from typing import List
import clickhouse_connect
import os
from dotenv import load_dotenv
#init
app = FastAPI()
@app.get("/")
async def root():
    return {"message": "Hello World"}

load_dotenv()

# Retrieve environment variables
host = os.getenv('CLICKHOUSE_HOST')
user = os.getenv('CLICKHOUSE_USER')
port = os.getenv('CLICKHOUSE_PORT')
password = os.getenv('CLICKHOUSE_PASSWORD')
database = os.getenv('CLICKHOUSE_DATABASE')

client = clickhouse_connect.get_client(
    host=host,
    user=user,
    port=int(port),  # Convert port to int if necessary
    password=password,
    database=database
)

# Function to fetch data from ClickHouse based on a provided SQL query
def fetch_data(query: str):
    try:
        # Execute the query using clickhouse_connect
        result = client.query(query)

        # Get column names and result rows
        columns = result.column_names
        rows = result.result_rows

        return columns, rows
    except Exception as e:
        return {"error": str(e)}

# Function to convert the result set to a list of dictionaries
def result_to_dict(columns: List[str], data: List):
    return [dict(zip(columns, row)) for row in data]

### Define API Endpoints for Specific Tables

# Endpoint for table1
@app.get("/financials_sample_data")
async def get_financial():
    query = "SELECT * FROM financials_sample_data LIMIT 100"
    result = fetch_data(query)

    # Check for any error in the result
    if isinstance(result, dict) and "error" in result:
        return result

    # Extract columns and data
    columns, data = result

    # Return the result as JSON (list of dictionaries)
    return columns, data

@app.get("/employee_sample_data")
async def get_employee():
    query = "SELECT * FROM default_raw__stream_employeesampledata LIMIT 100"
    result = fetch_data(query)

    # Check for any error in the result
    if isinstance(result, dict) and "error" in result:
        return result

    # Extract columns and data
    columns, data = result

    # Return the result as JSON (list of dictionaries)
    return columns, data


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
