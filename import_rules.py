import os
import glob
import pandas as pd
import mysql.connector
import numpy as np

# -------------------------------
# CONFIGURATION
# -------------------------------
DATA_DIR = "new_data/"
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "dsr"
}

TABLE_NAME = "dsr_table"

COLUMN_ORDER = [
    'report_date', 'station_name', 'call_category', 'reinforcement_reattended',
    'time_out', 'time_in', 'vehicle_no', 'lost_human', 'saved_human',
    'lost_animal', 'saved_animal', 'lost_value_rs', 'saved_value_rs',
    'dsr_activity', 'near_location', 'at_location', 'attended_by',
    'sub_category', 'taluka', 'city_village', 'lives_saved',
    'lives_lost', 'total_lives_lost', 'zone',
    'weekday', 'numerical_year', 'taluka_village', 'date_and_time'
]

# -------------------------------
# DATABASE CONNECTION
# -------------------------------
def get_db():
    return mysql.connector.connect(**MYSQL_CONFIG)

# -------------------------------
# DATA CLEANING FUNCTION
# -------------------------------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalize column names
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )

    # Ensure all required columns exist
    for col in COLUMN_ORDER:
        if col not in df.columns:
            df[col] = None

    # Reorder columns
    df = df[COLUMN_ORDER]

    # --------------------------------
    # Clean ONLY critical columns
    # --------------------------------
    CRITICAL_COLS = [
        "station_name",
        "call_category",
        "sub_category",
        "report_date"
    ]

    # Replace invalid values ONLY in these columns
    df[CRITICAL_COLS] = df[CRITICAL_COLS].replace(
        ["NIL", "nil", "-", ""],
        np.nan
    )

    # Drop rows where ANY critical column is null
    df.dropna(subset=CRITICAL_COLS, inplace=True)

    # -------------------------------
    # call_category rules
    # -------------------------------
    df["call_category"] = df["call_category"].astype(str).str.strip()
    df["call_category"] = df["call_category"].str.replace(
        "reinforcement-", "reinforcement", regex=False
    )

    # -------------------------------
    # station_name rules
    # -------------------------------
    df["station_name"] = df["station_name"].replace(
        "Curchorm", "Curchorem"
    )

    # -------------------------------
    # sub_category rules
    # -------------------------------
    df["sub_category"] = df["sub_category"].replace(
        "Drowning incidents",
        "Drowning, suicide and other related incidents"
    )

    df["sub_category"] = df["sub_category"].replace(
        "Fire to &/or in a commercial/ bussiness/ assembly/ hospital/ educational structures",
        "Fire to &/or in a commercial/ business/ assembly/ hospital/ educational structures"
    )

    df["sub_category"] = df["sub_category"].replace(
        "Fire to &/or in a residential low rise structures, house, village",
        "Fire to &/or in a residential low rise structures, flat, house, village"
    )

    # -------------------------------
    # saved_human rules
    # -------------------------------
    # def clean_saved_human(x):
    #     if str(x).strip() == "R11":
    #         return x
    #     try:
    #         return int(x)
    #     except:
    #         return None
    #
    # df["saved_human"] = df["saved_human"].apply(clean_saved_human)
    #
    # # -------------------------------
    # # saved_animal rules
    # # -------------------------------
    # def clean_saved_animal(x):
    #     if str(x).strip() == "R2B":
    #         return x
    #     if str(x).strip() == "01 L":
    #         return 1
    #     try:
    #         return int(x)
    #     except:
    #         return None
    #
    # df["saved_animal"] = df["saved_animal"].apply(clean_saved_animal)

    # -------------------------------
    # Numeric coercion
    # -------------------------------
    for col in [
        "lost_human", "lost_animal",
        "lost_value_rs", "saved_value_rs",
        "total_lives_lost"
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # -------------------------------
    # Ensure date_and_time exists
    # -------------------------------
    if "date_and_time" not in df.columns:
        df["date_and_time"] = df["report_date"].astype(str)

    # 🔥 FINAL CRITICAL FIX
    # Convert ALL NaN → None for MySQL compatibility
    df = df.astype(object).where(pd.notnull(df), None)

    return df

# -------------------------------
# INSERT INTO MYSQL
# -------------------------------
def insert_to_mysql(df: pd.DataFrame, chunk_size: int = 500):
    if df.empty:
        print("⚠ No valid rows after cleaning. Skipping insert.")
        return

    conn = get_db()
    cursor = conn.cursor()

    placeholders = ", ".join(["%s"] * len(COLUMN_ORDER))
    columns = ", ".join(COLUMN_ORDER)

    sql = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"

    total_inserted = 0

    try:
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            cursor.executemany(
                sql,
                chunk[COLUMN_ORDER].values.tolist()
            )
            conn.commit()
            total_inserted += len(chunk)

        print(f"✅ Inserted {total_inserted} rows into MySQL")

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()

# -------------------------------
# MAIN PROCESS
# -------------------------------
def run_import():
    files = glob.glob(os.path.join(DATA_DIR, "*.xlsx"))

    print(f"📁 Found {len(files)} Excel files")

    for file in files:
        filename = os.path.basename(file)

        if filename.startswith("~$"):
            print(f"⏭ Skipping temp file: {filename}")
            continue

        print(f"➡ Processing {filename}")

        df = pd.read_excel(file)
        df_cleaned = clean_dataframe(df)

        print(f"📊 Rows ready for insert: {len(df_cleaned)}")

        insert_to_mysql(df_cleaned)
        print(f"✔ Inserted {len(df_cleaned)} rows")

    print("🎉 All files imported successfully")

# -------------------------------
if __name__ == "__main__":
    run_import()
