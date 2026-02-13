# config.py
import os
from dotenv import load_dotenv

# Load variables from the .env file into the environment
load_dotenv()

# --- Text-to-SQL Configuration ---
CURRENT_TEXT2SQL_PROVIDER = 'SQLAI'

# Use os.getenv to retrieve the secret key
SQLAI_API_KEY = os.getenv("SQLAI_API_KEY")

# --- MySQL Configuration ---
# Use os.getenv for all sensitive credentials
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "dsr")

# NOTE: The second argument in os.getenv is a default value if the variable is not found.