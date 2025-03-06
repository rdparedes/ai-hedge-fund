import json
import sqlite3
from pathlib import Path


class Cache:
    """In-memory cache for API responses."""

    def __init__(self, db_path=None):
        self._prices_cache: dict[str, list[dict[str, any]]] = {}
        self._financial_metrics_cache: dict[str, list[dict[str, any]]] = {}
        self._line_items_cache: dict[str, list[dict[str, any]]] = {}
        self._insider_trades_cache: dict[str, list[dict[str, any]]] = {}
        self._company_news_cache: dict[str, list[dict[str, any]]] = {}
        self.db_path = db_path or Path(__file__).parent / 'cache.db'
        self._create_tables()
        self.load_from_db()

    def _create_tables(self):
        """Create tables if they do not exist."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    prices_cache JSON,
                    financial_metrics_cache JSON,
                    line_items_cache JSON,
                    insider_trades_cache JSON,
                    company_news_cache JSON
                )
            ''')
            conn.commit()

    def save_to_db(self):
        """Save cache to SQLite database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO cache (key, prices_cache, financial_metrics_cache, line_items_cache, insider_trades_cache, company_news_cache)
                VALUES (?, json(?), json(?), json(?), json(?), json(?))
            ''', (
                'cache',
                json.dumps(self._prices_cache),
                json.dumps(self._financial_metrics_cache),
                json.dumps(self._line_items_cache),
                json.dumps(self._insider_trades_cache),
                json.dumps(self._company_news_cache)
            ))
            conn.commit()

    def load_from_db(self):
        """Load cache from SQLite database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM cache WHERE key = ?', ('cache',))
            row = cursor.fetchone()
            if row:
                self._prices_cache = json.loads(row[1])
                self._financial_metrics_cache = json.loads(row[2])
                self._line_items_cache = json.loads(row[3])
                self._insider_trades_cache = json.loads(row[4])
                self._company_news_cache = json.loads(row[5])

    def _merge_data(self, existing: list[dict] | None, new_data: list[dict], key_field: str) -> list[dict]:
        """Merge existing and new data, avoiding duplicates based on a key field."""
        if not existing:
            return new_data

        # Create a set of existing keys for O(1) lookup
        existing_keys = {item[key_field] for item in existing}

        # Only add items that don't exist yet
        merged = existing.copy()
        merged.extend([item for item in new_data if item[key_field] not in existing_keys])
        return merged

    def get_prices(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached price data if available."""
        return self._prices_cache.get(ticker)

    def set_prices(self, ticker: str, data: list[dict[str, any]]):
        """Append new price data to cache."""
        self._prices_cache[ticker] = self._merge_data(
            self._prices_cache.get(ticker),
            data,
            key_field="time"
        )
        self.save_to_db()

    def get_financial_metrics(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached financial metrics if available."""
        return self._financial_metrics_cache.get(ticker)

    def set_financial_metrics(self, ticker: str, data: list[dict[str, any]]):
        """Append new financial metrics to cache."""
        self._financial_metrics_cache[ticker] = self._merge_data(
            self._financial_metrics_cache.get(ticker),
            data,
            key_field="report_period"
        )
        self.save_to_db()

    def get_line_items(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached line items if available."""
        return self._line_items_cache.get(ticker)

    def set_line_items(self, ticker: str, data: list[dict[str, any]]):
        """Append new line items to cache."""
        self._line_items_cache[ticker] = self._merge_data(
            self._line_items_cache.get(ticker),
            data,
            key_field="report_period"
        )
        self.save_to_db()

    def get_insider_trades(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached insider trades if available."""
        return self._insider_trades_cache.get(ticker)

    def set_insider_trades(self, ticker: str, data: list[dict[str, any]]):
        """Append new insider trades to cache."""
        self._insider_trades_cache[ticker] = self._merge_data(
            self._insider_trades_cache.get(ticker),
            data,
            key_field="filing_date"  # Could also use transaction_date if preferred
        )
        self.save_to_db()

    def get_company_news(self, ticker: str) -> list[dict[str, any]] | None:
        """Get cached company news if available."""
        return self._company_news_cache.get(ticker)

    def set_company_news(self, ticker: str, data: list[dict[str, any]]):
        """Append new company news to cache."""
        self._company_news_cache[ticker] = self._merge_data(
            self._company_news_cache.get(ticker),
            data,
            key_field="date"
        )
        self.save_to_db()


# Global cache instance
_cache = Cache()


def get_cache() -> Cache:
    """Get the global cache instance."""
    return _cache
