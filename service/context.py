# service/context.py

from .api_strategy import Text2SQLStrategy

class Text2SQLContext:
    """The Context class that uses the selected Text-to-SQL Strategy."""

    def __init__(self, strategy: Text2SQLStrategy):
        self._strategy = strategy

    def set_strategy(self, strategy: Text2SQLStrategy):
        """Allows switching the strategy at runtime."""
        self._strategy = strategy

    def execute_text_to_sql(self, natural_language_query: str, db_schema: str) -> str:
        """Delegates the generation to the current strategy."""
        return self._strategy.execute_text_to_sql(natural_language_query, db_schema)