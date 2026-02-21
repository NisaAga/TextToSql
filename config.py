# config.py
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
# Load variables from the .env file into the environment
load_dotenv()

# --- Text-to-SQL Configuration ---
CURRENT_TEXT2SQL_PROVIDER = 'SQLAI'

# Use os.getenv to retrieve the secret key
SQLAI_API_KEY = os.getenv("SQLAI_API_KEY")

# --- MySQL Configuration ---
# Use os.getenv for all sensitive credentials
DATABASE_URL = os.getenv("MYSQL_URL")

if not DATABASE_URL:
    # Fallback for local development
    DATABASE_URL = "mysql+pymysql://root:password@localhost:3306/mydatabase"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# NOTE: The second argument in os.getenv is a default value if the variable is not found.