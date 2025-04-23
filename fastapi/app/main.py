from fastapi import FastAPI, HTTPException
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from pydantic import BaseModel
import os

app = FastAPI()

class QueryRequest(BaseModel):
    sql: str
    catalog: str = "iceberg"
    schema: str = "default"

def get_trino_connection():
    return connect(
        host=os.getenv("TRINO_HOST"),
        port=os.getenv("TRINO_PORT"),
        user=os.getenv("TRINO_USER"),
        auth=BasicAuthentication(os.getenv("TRINO_USER"), os.getenv("TRINO_PASSWORD")) 
        if os.getenv("TRINO_PASSWORD") else None,
        catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        schema=os.getenv("TRINO_SCHEMA", "default"),
    )

@app.post("/query")
async def execute_query(query: QueryRequest):
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute(query.sql)
        
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        
        results = [dict(zip(columns, row)) for row in rows]
        
        return {
            "status": "success",
            "columns": columns,
            "data": results,
            "row_count": len(results)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/health")
async def health_check():
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        return {"status": "healthy"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))