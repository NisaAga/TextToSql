from flask import Flask, request, jsonify, render_template, send_file
from database.mysql_connector import MySQLExecutor, get_db_schema_description
from service.sqlai_api import SQLAIAPI
from config import CURRENT_TEXT2SQL_PROVIDER
from datetime import timedelta
from openpyxl import Workbook
from tempfile import NamedTemporaryFile

app = Flask(__name__)

# ---------------- UTILS ----------------

def _json_safe(value):
    if isinstance(value, timedelta):
        return value.total_seconds()
    return value


def generate_excel(headers, rows, sheet_name="Report"):
    wb = Workbook(write_only=True)
    ws = wb.create_sheet(title=sheet_name)

    ws.append(headers)
    for row in rows:
        ws.append(row)

    temp_file = NamedTemporaryFile(delete=False, suffix=".xlsx")
    wb.save(temp_file.name)
    wb.close()
    return temp_file.name


def is_safe_select_sql(sql: str) -> bool:
    if not sql:
        return False
    sql_clean = sql.strip().lower()
    return sql_clean.startswith("select") or sql_clean.startswith("with")


# ---------------- AGENT ----------------

class APIKeyMissingStrategy:
    def execute_text_to_sql(self, natural_language_query, db_schema):
        return "ERROR: AI API key missing."


class Text2SQLAgent:
    def __init__(self, generator):
        self.generator = generator
        self.db_schema = get_db_schema_description()

    def run(self, nl_query):
        sql = self.generator.execute_text_to_sql(nl_query, self.db_schema)

        if sql.startswith("ERROR"):
            return "API_ERROR", sql, None

        if not is_safe_select_sql(sql):
            return "SECURITY_ERROR", sql, "Only SELECT/WITH queries allowed."

        try:
            with MySQLExecutor() as db:
                headers, results = db.execute(sql)

            return "SUCCESS", sql, {
                "headers": headers,
                "results": results
            }

        except Exception as e:
            return "DB_ERROR", sql, f"{type(e).__name__}: {str(e)}"


# ---------------- INIT ----------------

try:
    text2sql_strategy = SQLAIAPI()
    provider_name = "AI2SQL (Active)"
except Exception:
    text2sql_strategy = APIKeyMissingStrategy()
    provider_name = "AI2SQL (Key Missing)"

agent = Text2SQLAgent(text2sql_strategy)


# ---------------- ROUTES ----------------

@app.route("/")
def index():
    return render_template("index.html", provider_name=provider_name)


@app.route("/api/query", methods=["POST"])
def query():
    nl_query = request.json.get("query")

    if not nl_query:
        return jsonify({"error": "Query missing"}), 400

    status, sql, data = agent.run(nl_query)

    if status != "SUCCESS":
        return jsonify({
            "status": status,
            "generated_sql": sql,
            "headers": ["Error"],
            "results": [[data]]
        }), 500

    return jsonify({
        "status": "SUCCESS",
        "generated_sql": sql,
        "headers": data["headers"],
        "results": [[_json_safe(v) for v in row] for row in data["results"]]
    })


@app.route("/api/test_db_records", methods=["GET"])
def test_db():
    sql = "SELECT * FROM dsr_table LIMIT 50"
    with MySQLExecutor() as db:
        headers, results = db.execute(sql)

    return jsonify({
        "generated_sql": sql,
        "headers": headers,
        "results": results
    })


@app.route("/api/query/export/excel", methods=["POST"])
def export_excel():
    sql = request.json.get("sql")

    if not sql:
        return jsonify({"error": "SQL missing"}), 400

    if not is_safe_select_sql(sql):
        return jsonify({"error": "Unsafe SQL"}), 400

    try:
        with MySQLExecutor() as db:
            headers, rows = db.execute(sql)

        file_path = generate_excel(headers, rows)

        return send_file(
            file_path,
            as_attachment=True,
            download_name="ai_generated_report.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        return jsonify({
            "error": "Export failed",
            "details": str(e)
        }), 500


if __name__ == "__main__":
    app.run(debug=True)
