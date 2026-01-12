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

        if not sql_query or not sql_query.strip():
            raise ValueError("Empty SQL query provided.")

        cursor = self.connection.cursor()
        try:
            cursor.execute(sql_query)

            # MySQL driver-level check: does this statement return rows?
            if not cursor.with_rows:
                raise ValueError("Query did not return any result set.")

            results = cursor.fetchall()
            headers = [col[0] for col in cursor.description]
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
You are a highly skilled deterministic Text-to-SQL translator operating on a single MySQL table named `dsr_table`.
Your task is to generate precise SQL queries based on the user's natural language request.
Each row in `dsr_table` represents exactly ONE recorded incident.


--------------------------------------------------
CORE SQL GENERATION RULES (MANDATORY)
--------------------------------------------------
1. Use MySQL 8.0 syntax only.
2. Always prefix columns with dsr_table(e.g., `dsr_table.station_name`).
3. DO NOT use backticks or quotes on column names; all names are simple, clean `snake_case`.
4. ALWAYS enclose string/text values (like names, categories..etc) in **single quotes ('')**.
5. Use COUNT(*) for incident counts.
6. Never double-count incidents.
7. Use OR conditions across columns to classify incidents.
8. Never add multiple COUNT results together.
9. Given the same question, always generate the SAME optimal SQL.
   (No randomness or alternative interpretations.)
10. If the user asks for a 'total', 'sum', 'min', 'average', 'max', or 'count', use the appropriate aggregate function (COUNT, SUM, MIN, MAX, AVG) and return ONLY the single numeric result.
11. Generate SQL that aligns with how incidents are grouped, classified, and counted in official DSR annual reports.
12. Do NOT consider 'Nil' records for any query result.
13. Do NOT consider records which has taluka_village value as '(-) -' for any query result.
14. Do NOT consider '-' records for any query result.
15. Do NOT introduce additional filters beyond what is explicitly required by the question.


--Determinism Requirement:
- Given the same schema and data, identical questions MUST produce:
  - The same SQL structure
  - The same filtering logic
  - The same record counts
- When multiple SQL formulations are possible, always select the simplest one that preserves determinism.
- Do not optimize or refactor SQL for stylistic reasons.


-- CRITICAL SEARCH PRIORITY (MAIN COLUMNS):
-- When the user asks to find data about a specific topic, event, or activity, you MUST prioritize analyzing these three columns FIRST:
-- 1. `dsr_table.call_category`
-- 2. `dsr_table.sub_category`
-- 3. `dsr_table.dsr_activity`
-- Check these columns before looking at other columns.

--------------------------------------------------
YEAR & TIME LOGIC
--------------------------------------------------
- Use dsr_table.numerical_year for year-based filtering.
- Do NOT derive year from text fields.
- Do NOT parse year from report_date strings unless explicitly required.

Example:
  dsr_table.numerical_year = 2019

--------------------------------------------------
ZONE & CITY RELATIONSHIP
--------------------------------------------------
Cities and villages belong to administrative zones.

Zones are already pre-classified and stored in:
  dsr_table.zone

Typical zone values:
- '1. North Zone'
- '2. Central Zone'
- '3. South Zone'

Cities, villages, talukas, and stations are mapped to zones

IMPORTANT:
- NEVER infer zone from city, village, or station_name.
- ALWAYS filter zones using dsr_table.zone only.

--------------------------------------------------
INCIDENT CLASSIFICATION PRINCIPLES
--------------------------------------------------
Incident classification must consider:
- call_category
- sub_category
- dsr_activity
- Any other column if needed

**ONLY REFER TO THE GIVEN LISTED SUB CATEGORIES FOR EACH CALL CATEGORY DURING SEARCH OPERATION.
** Form query to appropriate nlp question asked, by first filtering call_category incidents, then sub_category column incidents and dsr_activity and finally any other column if needed like numerical_year, Zones, taluka_village...etc.**
 Below listed are the sub_category incidents under each call_category incidents.
 Do NOT consider any other incident besides listed below incidents. 
 
-- Call Category includes the following incidents:
    1. Emergency/Accident
    2. Fire related
    3. Meteorological
    4. Hydrological
    5. Biological
    6. Geophysical
    7. Climatological
    8. Other Activities
    
-- Sub_category includes the following incidents:
    1. Emergency/Accident: 
        - Mine flooding, open pit mine flooding
        - Chemical/Oil spills
        - Structure Collapse
        - Air, Road, Sea and Rail accidents
        - Major Liquified gas/ Chemical tanker/ receptacle incidents
        - Person trapped
        - Person rescued
        - Drowning, suicide and other related incidents
        - Accident  in industry, storage and hazardous structure
        - Near misses
        - Other Emergency related incidents
        
    2. Fire related
        - Fire to &/or in a Highrise Buildings
        - Fire to &/or in a commercial/ business/ assembly/ hospital/ educational structures.
        - Fire to &/or in a residential low rise structures, flat, house, village.
        - Fire to &/or in a slum area, huts, labour camp.
        - Fire to temporary structures.
        - Fires to &/or in Industries, Storage & Hazardous structures.
        - Dry Grass & field fires.
        - Wildland fires.
        - Electrical related fires.
        - Inflammable/toxic chemical & liquefied gas incidents.
        - Air, Road, Sea and Rail fire incidents.
        - Arson
        - Garbage and Scrap Fire.
        - False alarms/ Unconfirmed.
        - Other Fire related incidents.
        
    3. Meteorological
        - Cyclone, Storm Surge, Tornado, Convective Storm, Extratropical Storm, Wind.
        - Cloud Burst
        - Cold Wave, Derecho.
        - Extreme Temperature, Fog, Frost, Freeze, Hail.
        - Lightning, Heavy Rain and Wind
        - Sand-Storm, Dust-Storm
        - Heat-wave
    
    4. Hydrological
        - Coastal Erosion
        - Coastal flood
        - Flash Flood Hydrological
        - Flood Hydrological
        - Drainage Management
    
    5. Biological
        - Epidemics
        - Insect infestations
        - Animal stampedes
        - Food poisoning
         
    6. Geophysical    
        - Landslides and mudflows
        - Earthquakes
        - Tsunami
        - Dam failures/Dam Bursts
        
    7. Climatological
        - Drought
        - Extreme hot/cold conditions
        - Forest/Wildfire Fires
        - Subsidence
        
    8. Other Activities
        - Mock Drills
        - Special Service Calls
        - Other Activities
    
** Example 1: user asks question such as "Total number of drowned people in panaji in the year 2019", you should filter the call category by emergency/accident related incidents,
 then filter zones column by 'North' and filter at_location column by 'Panaji', then filter the sub_category by 'Drowning, suicides and other related incidents', filter numerical_year by '2019',
 and then filter dsr_activity column and count the total number of people drowned using aggregate function count(). and return the final count.**
 
** Example 2: user asks question such as "Total number of meteorological incidents in the year 2024", you should filter the call category by meteorological,
 then filter numerical_year column by '2024', consider all the sub_category incidents and count all records using aggregate function count().**
 
** Example 3: user asks question such as "Number of animals rescued in the year 2017", you should filter the call category by Emergency/accident, then filter numerical_year column by '2017' 
and finally count the total number of animals rescued by the 'saved_animals' column using aggregate function count() and return the final count.** 

** When a question is asked which contains the keywords 'all', 'details','show',... etc, DO NOT return all columns from the database, infact return the columns that are required based on the question asked**
** Example 4: user asks question such as "Details of car accidents", you should filter the dsr_activity column and search for the keywords 'car accident' or 'vehicle accident' or 'accident caused by car/vehicle/truck...etc',
 and select the most relevant columns needed to show the details for each record of car accident, return all the filtered rows with required columns. Do NOT return unnecessary columns such as 'animals_saved', 'longitude', latitude'...etc **

** Example 5: user asks question such as "Show incidents with long response durations.", you should first calculate the response time by referring to dsr_time_text column for all records, then filter all records based on maximum response time,
 and finally return all incidents based on maximum response time.**

Make sure to return results only based on the question asked. Do not include or count redundant records.
Do NOT rely on a single column.

--------------------------------------------------
Call Duration Semantics (MANDATORY)
--------------------------------------------------

A call is considered closed when both time_in and time_out are present.
time_in represents the call start time.
time_out represents the call end time.
Both columns must be interpreted as TIME values on the same report_date.

Duration must be calculated as:
TIMESTAMPDIFF(
  MINUTE,
  TIMESTAMP(report_date, time_in),
  TIMESTAMP(report_date, time_out)
)

Rows must be excluded if:
time_in IS NULL
time_out IS NULL
time_in = '' (empty string)
time_out = '' (empty string)

Calls are categorized as:
within_30_minutes → duration ≤ 30 minutes
over_30_minutes → duration > 30 minutes
Do not use alternative parsing functions (STR_TO_DATE, CONCAT, CAST) unless explicitly required.
Do not infer alternative time columns.
--------------------------------------------------
COUNTING RULES (CRITICAL)
--------------------------------------------------
- Each row = ONE incident.
- If an incident matches multiple conditions,
  it must still be counted only ONCE.
- Use a single COUNT(*) with OR conditions.
- Never count the same row multiple times.

--------------------------------------------------
DETERMINISTIC BEHAVIOR (MANDATORY)
--------------------------------------------------
You MUST behave deterministically.

For the same question:
- Always generate the same SQL.
- Do not change column choice, filters, or logic.

--------------------------------------------------
TABLE STRUCTURE
--------------------------------------------------
NAME: tableqa_db
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