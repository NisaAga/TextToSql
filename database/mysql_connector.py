# database/mysql_connector.py

import mysql.connector
import sys
import os

# Allow importing config.py from parent directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE


# --- SCHEMA DEFINITION FOR TABLE CREATION ---
SCHEMA_SQL_DDL = """
CREATE TABLE IF NOT EXISTS dsr_table (
    report_date DATETIME,
    station_name TEXT,
    call_category TEXT,
    reinforcement_reattended TEXT,
    time_out TEXT, 
    time_in TEXT,
    vehicle_no TEXT,
    lost_human TEXT,
    saved_human TEXT,
    lost_animal TEXT,
    saved_animal TEXT,
    lost_value_rs TEXT,
    saved_value_rs TEXT,
    dsr_activity TEXT,
    near_location TEXT,
    at_location TEXT,
    attended_by TEXT,
    sub_category TEXT,
    taluka TEXT,
    city_village TEXT,
    additional_note_dsr TEXT, 
    additional_remarks TEXT,
    todays_dsr TEXT,
    dsr_time_text TEXT,
    lives_saved TEXT,
    lives_lost TEXT,
    total_lives_lost TEXT,
    latitude TEXT,
    longitude TEXT,
    month_year VARCHAR(10),
    zone VARCHAR(50),
    weekday VARCHAR(10),
    hour_on_day VARCHAR(50),
    numerical_year INT,
    zone_and_city_village TEXT,
    overview_of_record TEXT,
    taluka_village VARCHAR(100),
    dsr_activity_and_note TEXT,
    near_and_at TEXT,
    near_at_and_by TEXT,
    date_and_time DATETIME,
    filter_reinforcement VARCHAR(50)
);
"""


def create_dsr_table():
    """Creates dsr_table if it does not exist."""
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = conn.cursor()
        cursor.execute(SCHEMA_SQL_DDL)
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ dsr_table verified/created successfully.")
    except mysql.connector.Error as err:
        print(f"❌ Failed to create dsr_table: {err}")


# --- MySQL Executor Class (Unified & Corrected) ---
class MySQLExecutor:
    HIGH_TIMEOUT_SECONDS = 600  # 10 minutes

    def __init__(self):
        self.connection = None
        self.host = MYSQL_HOST
        self.user = MYSQL_USER
        self.password = MYSQL_PASSWORD
        self.database = MYSQL_DATABASE

        # Ensure table exists
        create_dsr_table()
        self.db_is_ready = self._test_connection()

    def _test_connection(self):
        print("\n[DB STATUS] Attempting to connect to MySQL database...")
        try:
            temp_conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                connection_timeout=self.HIGH_TIMEOUT_SECONDS
            )
            if temp_conn.is_connected():
                print(f"✅ DB Connection SUCCESS: Connected to '{self.database}' at {self.host}")
                temp_conn.close()
                return True
            return False
        except mysql.connector.Error as err:
            print(f"❌ DB Connection FAILED: {err}")
            return False

    def _ensure_connection(self):
        """Open connection if not connected."""
        if not self.connection or not self.connection.is_connected():
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                connection_timeout=self.HIGH_TIMEOUT_SECONDS,
                read_timeout=self.HIGH_TIMEOUT_SECONDS,
                write_timeout=self.HIGH_TIMEOUT_SECONDS
            )

    # Context manager support
    def __enter__(self):
        self._ensure_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection and self.connection.is_connected():
            self.connection.close()

    # Execute SELECT query
    def execute(self, sql_query: str):
        if not self.connection or not self.connection.is_connected():
            raise ConnectionError("Database connection is not open.")

        if not sql_query or not sql_query.upper().strip().startswith("SELECT"):
            raise ValueError("Only SELECT queries are allowed for data retrieval.")

        cursor = self.connection.cursor()
        try:
            cursor.execute(sql_query)
            results = cursor.fetchall()
            headers = [i[0] for i in cursor.description]
            return headers, results
        except mysql.connector.Error as err:
            raise RuntimeError(f"Failed to execute SQL query: {err}")
        finally:
            cursor.close()

    # Batch insert
    def insert_data(self, insert_query: str, data_tuples: list, chunk_size: int = 500):
        self._ensure_connection()
        cursor = self.connection.cursor()
        total_inserted = 0

        try:
            for i in range(0, len(data_tuples), chunk_size):
                batch = data_tuples[i:i + chunk_size]
                cursor.executemany(insert_query, batch)
                self.connection.commit()
                total_inserted += cursor.rowcount
            return total_inserted
        except mysql.connector.Error as err:
            self.connection.rollback()
            raise err
        finally:
            cursor.close()

# --- Schema Description Generator (Used by the Text-to-SQL Agent) ---

def get_db_schema_description():
    """
    A helper function to get the schema for the Text-to-SQL API prompt.
    Returns the database schema as a descriptive string for the AI model.
    """

    schema_template = f"""
You are a highly skilled Text-to-SQL translator operating on a single MySQL table named `dsr_table`.
Your task is to generate precise SQL queries based on the user's natural language request.

-- CRITICAL GENERATION RULES:
-- 1. DIALECT: All queries MUST be valid **MySQL 8.0** syntax.
-- 2. QUALIFICATION: ALWAYS prefix column names with the table name (e.g., `dsr_table.station_name`).
-- 3. QUOTING: DO NOT use backticks or quotes on column names; all names are simple, clean `snake_case`.
-- 4. STRINGS: ALWAYS enclose string/text values (like names, categories) in **single quotes ('')**.
-- 5. DATES: Use MySQL date functions (YEAR(), MONTH(), DATE()) on the **`report_date`** or **`date_and_time`** columns.

-- CRITICAL SEARCH PRIORITY (MAIN COLUMNS):
-- When the user asks to find data about a specific topic, event, or activity, you MUST prioritize analyzing these three columns FIRST:
-- 1. `dsr_table.call_category`
-- 2. `dsr_table.sub_category`
-- 3. `dsr_table.dsr_activity`
-- Check these columns before looking at others.

-- CRITICAL DATA HANDLING (REDUNDANCY & DUPLICATES):
-- When a keyword (e.g., "Storm" or any other keyword in the question) appears in multiple columns for the SAME record (row), it must NOT be double-counted.
-- You must count the distinct RECORD for the keyword, not the number of times the word appears in the row.
-- Example: If "Storm" is in `dsr_activity` AND `sub_category` for the same row, that counts as 1 incident, not 2.
-- CORRECT QUERY PATTERN: `SELECT COUNT(*) FROM dsr_table WHERE call_category LIKE '%Storm%' OR sub_category LIKE '%Storm%' OR dsr_activity LIKE '%Storm%'`
-- DO NOT use separate counts and sum them up. Use `OR` logic to capture the row once.
-- If the user asks for a 'total number', 'sum', 'min', 'average', 'max', or 'count', use the appropriate aggregate function (COUNT, SUM, MIN, MAX, AVG) and return ONLY the single numeric result.

DATABASE NAME: tableqa_db
TABLE NAME: dsr_table (Daily Situation Report)

TABLE DEFINITION (Use this DDL to understand the structure):
{SCHEMA_SQL_DDL}

--- NOTE ON COLUMNS (50 Columns – Detailed Descriptions) ---
- `report_date`: The original datetime when the report was created (DATETIME).
- `station_name`: Full name of the fire station responding to the call.
- `call_category`: High-level classification of the incident (e.g., 'Fire related', 'Emergency').
- `reinforcement_reattended`: Indicates if the call required reinforcement (e.g., 'Yes', 'No').
- `time_out`: Time (text format) when the team departed the station.
- `time_in`: Time (text format) when the team returned to base.
- `vehicle_no`: Registration number of the fire or rescue vehicle.
- `lost_human`: Number or text count of human lives lost.
- `saved_human`: Number or text count of human lives saved.
- `lost_animal`: Number or text count of animals lost.
- `saved_animal`: Number or text count of animals rescued.
- `lost_value_rs`: Estimated financial loss due to the incident in rupees (TEXT/VARCHAR).
- `saved_value_rs`: Estimated property or value saved in rupees (TEXT/VARCHAR).
- `dsr_activity`: Detailed textual activity or description of the operations performed.
- `near_location`: Landmark or general area near the site of incident.
- `at_location`: Exact address or spot where the incident occurred.
- `attended_by`: Names or ranks of personnel attending the call.
- `sub_category`: Sub-classification under main call category (e.g., 'Dry Grass & field fires').
- `taluka`: Administrative region (taluka/tehsil) where the incident occurred.
- `city_village`: Specific city or village name of the incident location.
- `additional_note_dsr`: Additional notes related to the DSR.
- `additional_remarks`: Other remarks not officially part of the DSR.
- `todays_dsr`: Summary of the DSR entry for that specific day.
- `dsr_time_text`: Textual representation of time context for the DSR (e.g., 'Morning', 'Evening').
- `lives_saved`: Text field indicating number or time context of lives saved.
- `lives_lost`: Text field representing human lives lost.
- `total_lives_lost`: Cumulative total of all lives lost across incidents or updates.
- `latitude`: Latitude coordinate of the incident.
- `longitude`: Longitude coordinate of the incident.
- `month_year`: Month and Year combined (e.g., 'Dec-2022').
- `zone`: Geographical or administrative zone (e.g., 'North Zone', '3. South Zone').
- `weekday`: Day of the week (e.g., 'Saturday').
- `hour_on_day`: Time range or specific hour (e.g., '0:00 to 2:00').
- `numerical_year`: The year stored as a four-digit integer (INT).
- `zone_and_city_village`: Merged text field combining geographical zone and the specific city/village.
- `overview_of_record`: A general summary or descriptive overview of the incident record.
- `taluka_village`: Merged field for administrative location details (Taluka and Village).
- `dsr_activity_and_note`: Merged field of the detailed DSR activity description and any additional notes.
- `near_and_at`: Merged field combining the near location landmark and the exact incident location.
- `near_at_and_by`: Merged field combining location details (near/at) and the attending personnel.
- `date_and_time`: A complete DATETIME field derived from date and time components (use this for temporal filtering).
- `filter_reinforcement`: A simplified or filtered version of the reinforcement status.
---
"""
    return schema_template.strip()