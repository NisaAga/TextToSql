# app.py

from flask import Flask, request, jsonify, render_template
from database.mysql_connector import MySQLExecutor, get_db_schema_description
from service.sqlai_api import SQLAIAPI
import sys
from config import CURRENT_TEXT2SQL_PROVIDER # Kept this import just in case the template relies on it


# --- NEW FALLBACK STRATEGY CLASS ---
# Implements the strategy interface without needing an actual API key
class APIKeyMissingStrategy:
    """
    A placeholder strategy used when the AI2SQL API key is missing or invalid.
    It prevents the app from crashing and returns a specific error message.
    """
    def __init__(self):
        print("⚠️ WARNING: API Key Missing Strategy is active. AI queries will be blocked.")
        pass

    # Must implement the exact method name required by the Text2SQLAgent
    def execute_text_to_sql(self, natural_language_query: str, db_schema: str) -> str:
        """
        Returns a specific ERROR message that the Text2SQLAgent will catch and display.
        """
        # The agent checks for 'ERROR:' prefix, so we use it here.
        return "ERROR: AI2SQL API key not found/invalid. Please check your config.py."


app = Flask(__name__)


# --- AGENT ORCHESTRATION CLASS (No Change) ---
class Text2SQLAgent:
    """
    Orchestrates the entire flow: Generation -> Execution -> Results.
    """

    def __init__(self, sql_generator_strategy):
        self.generator = sql_generator_strategy
        self.executor = MySQLExecutor()
        self.db_schema = get_db_schema_description()

    def process_query(self, nl_query: str):
        if not self.executor.db_is_ready:
            return "DB_ERROR", "DB not ready. Check console for startup connection errors.", None

        # --- PHASE 1: GENERATION (NL -> SQL) ---
        generated_sql = self.generator.execute_text_to_sql(nl_query, self.db_schema)

        if generated_sql.startswith("ERROR"):
            # Catches the API key error or any other generation error
            return "API_ERROR", generated_sql, None

        # --- PHASE 2: EXECUTION (SQL -> Data) ---
        try:
            with self.executor as db:
                headers, results = db.execute(generated_sql)

            return "SUCCESS", generated_sql, {"headers": headers, "results": results}

        except (RuntimeError, ConnectionError) as e:
            return "DB_ERROR", generated_sql, f"Execution Failed: {str(e)}"
        except Exception as e:
            return "DB_ERROR", generated_sql, f"Unexpected Error: {type(e).__name__}: {str(e)}"


# --- END AGENT ORCHESTRATION CLASS ---


# --- Strategy Initialization (Handling API Key Gracefully) ---

try:
    # 1. Attempt to initialize the live API
    text2sql_strategy = SQLAIAPI()
    active_provider_name = "AI2SQL (Active)"
    print("AI2SQL API key successfully loaded. Full functionality enabled.")
except (ValueError, ImportError) as e:
    # 2. If ValueError (missing key) or ImportError (missing dependencies) occurs, fall back
    text2sql_strategy = APIKeyMissingStrategy()
    active_provider_name = "AI2SQL (Key Missing)"
    print(f"❗ FALLBACK MODE ACTIVATED: Failed to initialize AI2SQL API. {e}. The app will continue to run.")


# 3. Initialize the main Orchestrator Agent
text2sql_agent = Text2SQLAgent(text2sql_strategy)
print(f"Active Text-to-SQL Provider: {active_provider_name}")


# --- FLASK ROUTES (Minor update to API_ERROR handling) ---

@app.route('/api/provider_name', methods=['GET'])
def get_provider_name():
    return jsonify({"provider": active_provider_name})

@app.route('/')
def index():
    return render_template('index.html', provider_name=active_provider_name)

@app.route('/api/query', methods=['POST'])
def handle_query():
    data = request.json
    natural_language_query = data.get('query')

    if not natural_language_query:
        return jsonify({"error": "No query provided"}), 400

    # 1. Process query using the Agent (NL -> SQL -> Data)
    status, generated_sql, result_data = text2sql_agent.process_query(natural_language_query)

    # 2. Handle Errors (API or DB Execution)
    if status == "API_ERROR":
        # The generated_sql contains the detailed ERROR message (e.g., 'API key not found')
        return jsonify({
            "generated_sql": "Failed to generate SQL.",
            "results": [[generated_sql]], # Display the error message to the user
            "headers": ["AI Generation Error"],
            "provider": active_provider_name
        })

    if status == "DB_ERROR":
        error_message = result_data if isinstance(result_data, str) else "Unknown DB Error"
        return jsonify({
            "generated_sql": generated_sql,
            "results": [[error_message]],
            "headers": ["DB Execution Error"],
            "provider": active_provider_name
        })

    # 3. Handle Success
    return jsonify({
        "generated_sql": generated_sql,
        "results": result_data["results"],
        "headers": result_data["headers"],
        "provider": active_provider_name
    })


@app.route('/api/test_db_records', methods=['GET'])
def test_db_records():
    """
    Executes a simple SELECT query to test data retrieval and connection integrity.
    """
    test_query = "SELECT * FROM dsr_table LIMIT 50;"

    if not text2sql_agent.executor.db_is_ready:
        return jsonify({
            "generated_sql": test_query,
            "results": [["DB not ready. Please check console logs for startup errors."]],
            "headers": ["DB Connection Error"],
            "provider": active_provider_name
        }), 500

    try:
        db = text2sql_agent.executor

        # ✅ Use execute instead of run_query
        with db as conn_db:
            headers, results = conn_db.execute(test_query)

        return jsonify({
            "generated_sql": test_query,
            "results": results,
            "headers": headers,
            "provider": active_provider_name
        })

    except Exception as e:
        error_message = f"TEST FAILED: {type(e).__name__}: {str(e)}"
        return jsonify({
            "generated_sql": test_query,
            "results": [[error_message]],
            "headers": ["DB Execution Error"],
            "provider": active_provider_name
        }), 500


if __name__ == '__main__':
    app.run(debug=True)