# service/SQLAI_api.py

from .api_strategy import Text2SQLStrategy
import requests
import os
from config import SQLAI_API_KEY

# *** FIX: UPDATED TO THE NEW SQLAI.AI V2 ENDPOINT ***
SQLAI_API_URL = "https://api.sqlai.ai/api/public/v2"


class SQLAIAPI(Text2SQLStrategy):
    """Concrete strategy for the SQLAI Text-to-SQL API."""

    def __init__(self):
        # Prefer environment variable, fall back to config file
        self.api_key = os.getenv("SQLAI_API_KEY", SQLAI_API_KEY)
        self.url = SQLAI_API_URL

        # Check for missing/placeholder key
        # We explicitly check for both placeholders to be safe
        placeholder_keys = ["your-SQLAI-api-key-here", "your-SQLAI-api-key-here"]
        if self.api_key in placeholder_keys or not self.api_key:
            raise ValueError(
                "SQLAIAPI API key is missing or is the default placeholder."
            )

        print("SQLAI API client initialized successfully.")

    def execute_text_to_sql(self, natural_language_query: str, db_schema: str) -> str:
        """
        Sends the natural language query and schema to the SQLAI.ai API (v2).
        """

        # *** FIX: UPDATED PAYLOAD KEYS FOR SQLAI.AI V2 ***
        # 'query' -> 'prompt'
        # 'dialect' -> 'engine'
        # 'schema' -> 'dataSource'
        # Added required 'mode'
        payload = {
            "prompt": natural_language_query,
            "engine": "mysql",  # Assuming your database is MySQL, based on other files
            "mode": "textToSQL",
            "dataSource": db_schema,
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(self.url, headers=headers, json=payload, timeout=120)
            response.raise_for_status()

            data = response.json()
            generated_sql = data.get('query') or data.get('sql')

            if generated_sql:
                return generated_sql.strip()
            else:
                # Includes the full response data for easier debugging if 'query' or 'sql' is missing
                return f"ERROR: SQLAI API did not return a valid 'query' in the response: {data}"

        except requests.exceptions.HTTPError as e:
            # The 500 error is caught here
            status_code = response.status_code

            # Use 'error' from the response JSON for better error details
            try:
                error_details = response.json().get('error', response.json().get('detail',
                                                                                 response.json().get('message',
                                                                                                     str(e))))
            except:
                error_details = str(e)

            # Re-check the API Key (401 is more appropriate, but handle all errors gracefully)
            if status_code == 401:
                return f"ERROR: SQLAI Authentication Failed (401). Check your API Key's validity and subscription status."

            return f"ERROR: SQLAI HTTP Failed ({status_code}): {error_details}"

        except requests.exceptions.RequestException as e:
            return f"ERROR: SQLAI Network Error: {str(e)}"