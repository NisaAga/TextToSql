# text2sql_service/text2sql_strategy.py

from abc import ABC, abstractmethod


class Text2SQLStrategy(ABC):
    """
    Abstract Base Class (ABC) defining the interface for all Text-to-SQL API wrappers.
    All concrete strategy classes (e.g., PremSQLAPI, OpenAIAPI) must inherit from this
    and implement the 'execute_text_to_sql' method.
    """

    @abstractmethod
    def execute_text_to_sql(self, natural_language_query: str, db_schema: str) -> str:
        """
        Takes a natural language query and the DB schema, and returns the generated SQL string.

        Args:
            natural_language_query: The user's question (e.g., "Show me users in France").
            db_schema: The database schema description.

        Returns:
            A string containing the generated SQL query or an error message prefixed with "ERROR".
        """
        pass