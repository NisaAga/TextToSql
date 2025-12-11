import os
import glob
import sys
import openpyxl

# Find mysql_connector
sys.path.append(os.path.join(os.path.dirname(__file__), 'database'))
from database.mysql_connector import MySQLExecutor


DATA_DIR = 'data/'
FILE_PATTERN = '*.xlsx'


# DB column names
COLUMN_NAMES = [
    'report_date', 'station_name',
    'call_category', 'reinforcement_reattended', 'time_out', 'time_in',
    'vehicle_no', 'lost_human', 'saved_human', 'lost_animal', 'saved_animal',
    'lost_value_rs', 'saved_value_rs', 'dsr_activity', 'near_location',
    'at_location', 'attended_by', 'sub_category', 'taluka', 'city_village',
    'additional_note_dsr', 'additional_remarks', 'todays_dsr', 'dsr_time_text',
    'lives_saved', 'lives_lost', 'total_lives_lost', 'latitude', 'longitude',
    'month_year', 'zone', 'weekday', 'hour_on_day', 'numerical_year',
    'zone_and_city_village',
    'overview_of_record',
    'taluka_village',
    'dsr_activity_and_note',
    'near_and_at',
    'near_at_and_by',
    'date_and_time',
    'filter_reinforcement'
]

PLACEHOLDERS = ', '.join(['%s'] * len(COLUMN_NAMES))
INSERT_QUERY = f"INSERT INTO dsr_table ({', '.join(COLUMN_NAMES)}) VALUES ({PLACEHOLDERS})"


# Header alias mapping
HEADER_ALIAS = {
    "date(yyyy-mm-dd)_and_time(hh:mm)": "date_and_time",
    "zone and city village": "zone_and_city_village",
    "taluka village": "taluka_village"
}


def normalize_header(h):
    if h is None:
        return None

    h = str(h).strip().lower().replace(" ", "_").replace("/", "_")

    # alias correction
    return HEADER_ALIAS.get(h, h)


# Manual Excel Import
def import_excel_manual(file_path, COLUMN_NAMES, db_executor):
    print(f"  ➡ Reading Excel manually: {os.path.basename(file_path)}")

    wb = openpyxl.load_workbook(file_path, data_only=True)
    sheet = wb.active

    # Read + normalize excel headers
    excel_headers = [normalize_header(cell.value) for cell in sheet[1]]

    print("     → Excel Headers:", excel_headers)

    # Map headers to DB column indices
    col_index_map = []
    for col in COLUMN_NAMES:
        col_lower = col.lower()
        if col_lower in excel_headers:
            col_index_map.append(excel_headers.index(col_lower))
        else:
            col_index_map.append(None)

    rows = []

    for row in sheet.iter_rows(min_row=2, values_only=True):
        row_data = []
        for idx in col_index_map:
            if idx is None:
                row_data.append(None)
            else:
                v = row[idx]
                if v == "" or v is None:
                    row_data.append(None)
                else:
                    row_data.append(v)
        rows.append(tuple(row_data))

    # Insert rows
    inserted = db_executor.insert_data(INSERT_QUERY, rows)
    print(f"  ✔ Imported {inserted} rows from {os.path.basename(file_path)}")
    return inserted


# Main Batch Import
def batch_import_data():
    print("\n🔍 Searching for Excel files...")

    db_executor = MySQLExecutor()
    if not db_executor.db_is_ready:
        print("❌ Database connection failed. Cannot proceed.")
        return

    file_list = glob.glob(os.path.join(DATA_DIR, FILE_PATTERN))

    if len(file_list) == 0:
        print("❌ No Excel files found in /data/")
        return

    print(f"📁 Found {len(file_list)} Excel file(s). Starting import...")

    total_inserted = 0
    success = 0

    for file_path in file_list:
        file_name = os.path.basename(file_path)
        print(f"\n➡ Processing: {file_name}")

        try:
            inserted = import_excel_manual(file_path, COLUMN_NAMES, db_executor)
            total_inserted += inserted
            success += 1

        except Exception as e:
            print(f"   ❌ ERROR importing {file_name}: {e}")

    if success == len(file_list):
        print("\n===================================================")
        print("🎉 ALL FILES IMPORTED SUCCESSFULLY!")
        print(f"📌 Total rows inserted: {total_inserted}")
        print("===================================================")
    else:
        print("\n⚠ Some files failed to import. Check errors above.")


if __name__ == "__main__":
    batch_import_data()
