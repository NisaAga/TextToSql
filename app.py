from flask import Flask, request, jsonify, render_template
from database.mysql_connector import MySQLExecutor, get_db_schema_description
from service.sqlai_api import SQLAIAPI
from config import CURRENT_TEXT2SQL_PROVIDER


# --- AGGREGATE INTENT (NL FALLBACK ONLY) ---
def is_aggregate_query_from_nl(nl_query: str) -> bool:
    """
    Weak signal: checks if NL query *suggests* aggregation.
    Used only as a fallback.
    """
    nl = nl_query.lower()

    aggregate_keywords = [
        "total",
        "number of",
        "how many",
        "count",
        "sum",
        "average",
        "avg",
        "maximum",
        "minimum",
        "max",
        "min"
    ]

    return any(keyword in nl for keyword in aggregate_keywords)


# --- AGGREGATE INTENT (AUTHORITATIVE: SQL-BASED) ---
def is_aggregate_query_from_sql(sql: str) -> bool:
    """
    Strong signal: checks actual SQL for aggregate functions.
    """
    if not sql:
        return False

    sql_lower = sql.lower()
    aggregate_functions = ["count(", "sum(", "avg(", "min(", "max("]

    return any(fn in sql_lower for fn in aggregate_functions)


# --- FALLBACK STRATEGY CLASS ---
class APIKeyMissingStrategy:
    def __init__(self):
        print("WARNING: API Key Missing Strategy active.")

    def execute_text_to_sql(self, natural_language_query: str, db_schema: str) -> str:
        return "ERROR: AI2SQL API key not found or invalid. Please check config.py."


app = Flask(__name__)


# --- AGENT ORCHESTRATION CLASS ---
class Text2SQLAgent:
    def __init__(self, sql_generator_strategy):
        self.generator = sql_generator_strategy
        self.executor = MySQLExecutor()
        self.db_schema = get_db_schema_description()

    def process_query(self, nl_query: str):
        if not self.executor.db_is_ready:
            return "DB_ERROR", None, "DB not ready. Check startup logs."

        # Phase 1: Generate SQL
        generated_sql = self.generator.execute_text_to_sql(
            nl_query,
            self.db_schema
        )

        if generated_sql.startswith("ERROR"):
            return "API_ERROR", generated_sql, None

        # Phase 2: Execute SQL
        try:
            with self.executor as db:
                headers, results = db.execute(generated_sql)

            return "SUCCESS", generated_sql, {
                "headers": headers,
                "results": results
            }

        except Exception as e:
            return "DB_ERROR", generated_sql, f"{type(e).__name__}: {str(e)}"


# --- STRATEGY INITIALIZATION ---
try:
    text2sql_strategy = SQLAIAPI()
    active_provider_name = "AI2SQL (Active)"
    print("AI2SQL API initialized successfully.")
except (ValueError, ImportError) as e:
    text2sql_strategy = APIKeyMissingStrategy()
    active_provider_name = "AI2SQL (Key Missing)"
    print(f"Fallback mode activated: {e}")

text2sql_agent = Text2SQLAgent(text2sql_strategy)
print(f"Active Provider: {active_provider_name}")


# --- ROUTES ---
@app.route('/')
def index():
    return render_template("index.html", provider_name=active_provider_name)


@app.route('/api/provider_name', methods=['GET'])
def get_provider_name():
    return jsonify({"provider": active_provider_name})


@app.route('/api/query', methods=['POST'])
def handle_query():
    data = request.json
    natural_language_query = data.get("query")

    if not natural_language_query:
        return jsonify({"error": "No query provided"}), 400

    status, generated_sql, result_data = text2sql_agent.process_query(
        natural_language_query
    )

    # --- API ERROR ---
    if status == "API_ERROR":
        return jsonify({
            "show_generated_sql": False,
            "generated_sql": None,
            "headers": ["AI Generation Error"],
            "results": [[generated_sql]],
            "provider": active_provider_name
        })

    # --- AGGREGATE DECISION (FINAL) ---
    aggregate_intent = (
        is_aggregate_query_from_sql(generated_sql)
        or is_aggregate_query_from_nl(natural_language_query)
    )

    # --- DB ERROR ---
    if status == "DB_ERROR":
        return jsonify({
            "show_generated_sql": aggregate_intent,
            "generated_sql": generated_sql if aggregate_intent else None,
            "headers": ["DB Execution Error"],
            "results": [[result_data]],
            "provider": active_provider_name
        })

    # --- SUCCESS ---
    return jsonify({
        "show_generated_sql": aggregate_intent,
        "generated_sql": generated_sql if aggregate_intent else None,
        "headers": result_data["headers"],
        "results": result_data["results"],
        "provider": active_provider_name
    })


@app.route('/api/test_db_records', methods=['GET'])
def test_db_records():
    test_query = "SELECT * FROM dsr_table LIMIT 50;"

    if not text2sql_agent.executor.db_is_ready:
        return jsonify({
            "show_generated_sql": True,
            "generated_sql": test_query,
            "headers": ["DB Connection Error"],
            "results": [["DB not ready"]],
            "provider": active_provider_name
        }), 500

    try:
        with text2sql_agent.executor as db:
            headers, results = db.execute(test_query)

        return jsonify({
            "show_generated_sql": True,
            "generated_sql": test_query,
            "headers": headers,
            "results": results,
            "provider": active_provider_name
        })

    except Exception as e:
        return jsonify({
            "show_generated_sql": True,
            "generated_sql": test_query,
            "headers": ["DB Execution Error"],
            "results": [[str(e)]],
            "provider": active_provider_name
        }), 500


if __name__ == "__main__":
    app.run(debug=True)
