# import_data.py

import mysql.connector
import pandas as pd  # <-- NEW: For Excel handling
import os
import sys
# Assume config.py is accessible
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

# --- Configuration ---
# 1. Update this path to your actual Excel file.
EXCEL_FILE_PATH = 'DSR_2025.xlsx'  # Changed extension from .csv to .xlsx
# 2. Update this to the sheet name containing your data.
SHEET_NAME = 'Sheet1'
TABLE_NAME = 'dsr_table'

# --- 1. CREATE TABLE DDL (uses clean snake_case names) ---
# VARCHAR sizes are general; you can adjust them for efficiency if needed.
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
    `sr_no` BIGINT(20),
    `fault_code` VARCHAR(50),
    `stn_no` BIGINT(20),
    `call_no` BIGINT(20),
    `report_date` DATETIME,
    `station_name` VARCHAR(255),
    `call_category` VARCHAR(255),
    `reinforcement_reattended` VARCHAR(50),
    `time_out` VARCHAR(50), 
    `time_in` VARCHAR(50),
    `vehicle_no` VARCHAR(50),
    `lost_human` TEXT,
    `saved_human` TEXT,
    `lost_animal` TEXT,
    `saved_animal` TEXT,
    `lost_value_rs` TEXT,
    `saved_value_rs` TEXT,
    `dsr_activity` TEXT,
    `near_location` TEXT,
    `at_location` TEXT,
    `attended_by` TEXT,
    `sub_category` TEXT,
    `taluka` TEXT,
    `city_village` TEXT,
    `additional_note_dsr` TEXT,
    `additional_remarks` TEXT,
    `todays_dsr` TEXT,
    `dsr_time_text` TEXT,
    `lives_saved` TEXT,
    `lives_lost` TEXT,
    `total_lives_lost` TEXT,
    `latitude` TEXT,
    `longitude` TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# --- 2. COLUMN MAPPING (Excel Header -> Database Column) ---
# KEY = EXACT column name in your Excel sheet
# VALUE = Clean column name in your database
COLUMN_MAPPING = {
    'Sr No': 'sr_no',
    'Fault Code': 'fault_code',
    'Stn No': 'stn_no',
    'Call No': 'call_no',
    'Report Date (7 to 7)': 'report_date',
    'Station Name': 'station_name',
    'Call Category': 'call_category',
    'Reinforcement/ Reattended': 'reinforcement_reattended',
    'Time Out': 'time_out',
    'Time In': 'time_in',
    'Vehicle No': 'vehicle_no',
    'Lost Human': 'lost_human',
    'Saved Human': 'saved_human',
    'Lost Animal': 'lost_animal',
    'Saved Animal': 'saved_animal',
    'Lost (Rs)': 'lost_value_rs',
    'Saved (Rs)': 'saved_value_rs',
    'DSR Activity': 'dsr_activity',
    'Near': 'near_location',
    'At': 'at_location',
    'By': 'attended_by',
    'Sub Category': 'sub_category',
    'Taluka': 'taluka',  # Added based on the 33-column list
    'City/ Village': 'city_village',
    'Additional NOTE on DSR': 'additional_note_dsr',
    'Additional Remarks (Not on Report)': 'additional_remarks',
    "Today's DSR": 'todays_dsr',
    'DSR Time Text': 'dsr_time_text',
    'Lives Saved': 'lives_saved',
    'Lives Lost': 'lives_lost',
    'Total Lives Lost': 'total_lives_lost',
    'Latitude': 'latitude',
    'Longitude': 'longitude'
}

# The target list ensures the data is inserted in the correct order
TARGET_COLUMN_NAMES = list(COLUMN_MAPPING.values())

# Create the dynamic INSERT query string
COLUMN_STRING = ", ".join(TARGET_COLUMN_NAMES)
VALUE_PLACEHOLDERS = ", ".join(["%s"] * len(TARGET_COLUMN_NAMES))
INSERT_QUERY = f"INSERT INTO {TABLE_NAME} ({COLUMN_STRING}) VALUES ({VALUE_PLACEHOLDERS})"


def connect_to_db(connect_to_database=True):
    """Utility function to handle database connection."""
    try:
        if connect_to_database:
            return mysql.connector.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE
            )
        else:
            # Connect without specifying database to create the DB if needed
            return mysql.connector.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD
            )
    except mysql.connector.Error as err:
        print(f"❌ Connection Error: {err}")
        print("Please ensure your MySQL server is running and credentials are correct.")
        sys.exit(1)


def create_dsr_table(conn):
    """Ensures the database and the dsr_table exist."""
    cursor = None
    try:
        cursor = conn.cursor()

        # 1. Ensure the database exists
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` DEFAULT CHARACTER SET utf8mb4;")
        print(f"✅ Database '{MYSQL_DATABASE}' ensured to exist.")

        # 2. Switch to the target database
        conn.database = MYSQL_DATABASE

        # 3. Execute the CREATE TABLE statement
        print(f"Executing CREATE TABLE for {TABLE_NAME}...")
        cursor.execute(CREATE_TABLE_SQL)
        conn.commit()
        print(f"✅ Table '{TABLE_NAME}' ensured to exist.")

    finally:
        if cursor:
            cursor.close()


def import_excel_to_mysql():
    """
    Reads Excel, remaps columns, and inserts data into MySQL.
    """
    if not os.path.exists(EXCEL_FILE_PATH):
        print(f"❌ Error: Excel file not found at path: {EXCEL_FILE_PATH}")
        return

    conn = None
    try:
        # 1. Connect and ensure table exists
        conn = connect_to_db(connect_to_database=False)
        create_dsr_table(conn)

        # Reconnect to the database now that we know it exists
        conn = connect_to_db(connect_to_database=True)
        cursor = conn.cursor()

        print(f"Reading data from {EXCEL_FILE_PATH} (Sheet: {SHEET_NAME})...")

        # 2. Read the Excel file using pandas
        df = pd.read_excel(EXCEL_FILE_PATH, sheet_name=SHEET_NAME, header=0)

        # 3. Rename columns using the mapping dictionary
        df = df.rename(columns=COLUMN_MAPPING)

        # 4. Select and reorder columns to match the target database table
        df = df[TARGET_COLUMN_NAMES]

        # 5. Handle missing values (NaN/NaT)
        df = df.fillna('')

        # 6. Convert the DataFrame to a list of tuples
        data_to_insert = [tuple(row) for row in df.values]
        records_to_insert = len(data_to_insert)

        if not data_to_insert:
            print("⚠️ Warning: Excel sheet contains no data rows to insert.")
            return

        # 7. Execute bulk insert
        print(f"Attempting to insert {records_to_insert} records...")
        cursor.executemany(INSERT_QUERY, data_to_insert)
        conn.commit()
        print(f"✅ Success: {cursor.rowcount} rows inserted into {TABLE_NAME}.")


    except mysql.connector.Error as err:
        print(f"❌ MySQL Error (Insertion Failed): {err}")
        if conn:
            conn.rollback()
    except KeyError as e:
        print(
            f"❌ Column Mapping Error: Your Excel sheet is missing a column or your mapping key is incorrect. Check key: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("Connection closed.")


if __name__ == '__main__':
    import_excel_to_mysql()