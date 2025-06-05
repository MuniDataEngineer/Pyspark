from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import snowflake.connector
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()


app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict to specific frontend URLs
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API key from .env
API_KEY = os.getenv("API_KEY")

# Snowflake connection function
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

# API route with API key check
@app.get("/get-product-name/{product_id}")
def get_product_name(product_id: str, request: Request):
    # Validate API key
    client_key = request.headers.get("x-api-key")
    if client_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid API Key")

    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        # Execute Snowflake query
        cur.execute(f"SELECT product_name FROM product_table WHERE product_id = '{product_id}'")
        row = cur.fetchone()

        cur.close()
        conn.close()

        if row:
            return {"product_name": row[0]}
        else:
            raise HTTPException(status_code=404, detail="Product not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
