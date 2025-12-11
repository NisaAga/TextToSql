# executors/mysql_executor.py

import mysql.connector
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

# --- DSR Table Creation SQL ---
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

# --- MySQL Executor Class ---
class MySQLExecutor:
    HIGH_TIMEOUT_SECONDS = 600  # 10 minutes

    def __init__(self):
        self.connection = None
        self.host = MYSQL_HOST
        self.user = MYSQL_USER
        self.password = MYSQL_PASSWORD
        self.database = MYSQL_DATABASE

        # Ensure table exists
        self._create_dsr_table()
        self._test_connection()

    # ----------------------------
    # Connection / Context Manager
    # ----------------------------
    def _ensure_connection(self):
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

    def __enter__(self):
        self._ensure_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection and self.connection.is_connected():
            self.connection.close()

    # ----------------------------
    # Table Creation
    # ----------------------------
    def _create_dsr_table(self):
        try:
            conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            cursor = conn.cursor()
            cursor.execute(SCHEMA_SQL_DDL)
            conn.commit()
            cursor.close()
            conn.close()
            print("✅ dsr_table verified/created successfully.")
        except mysql.connector.Error as err:
            print(f"❌ Failed to create dsr_table: {err}")

    # ----------------------------
    # Test Connection
    # ----------------------------
    def _test_connection(self):
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

    # ----------------------------
    # Execute SELECT Queries
    # ----------------------------
    def execute(self, sql_query: str):
        """
        Execute SELECT query and return headers + results
        """
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

    # ----------------------------
    # Batch Insert
    # ----------------------------
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


# ----------------------------
# Example Usage / Testing
# ----------------------------
if __name__ == "__main__":
    test_query = "SELECT station_name FROM dsr_table LIMIT 5;"

    try:
        with MySQLExecutor() as executor:
            headers, results = executor.execute(test_query)
            print("Headers:", headers)
            print("Results:", results)

    except (ConnectionError, RuntimeError, ValueError) as e:
        print(f"An error occurred: {e}")
