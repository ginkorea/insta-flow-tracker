from contextlib import closing
from pathlib import Path
import sqlite3

from flask import Flask, render_template_string


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "insti_flow.db"

TABLE_CONFIG = {
    "funds": {"title": "Funds", "order_by": "id ASC", "limit": 10},
    "companies": {"title": "Companies", "order_by": "id ASC", "limit": 10},
    "holdings": {
        "title": "Latest Holdings",
        "order_by": "date DESC, pos_usd DESC",
        "limit": 5,
    },
    "awards": {
        "title": "Recent Federal Awards",
        "order_by": "date DESC, amount_usd DESC",
        "limit": 5,
    },
    "patents": {
        "title": "Recent Patents",
        "order_by": "pub_date DESC",
        "limit": 5,
    },
    "etf_trades": {
        "title": "Recent ETF Trades",
        "order_by": "date DESC, value_usd DESC",
        "limit": 5,
    },
}

app = Flask(__name__)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_table_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def get_table_preview(
    conn: sqlite3.Connection, table: str, *, order_by: str | None, limit: int
) -> list[dict]:
    query = f"SELECT * FROM {table}"
    if order_by:
        query += f" ORDER BY {order_by}"
    query += " LIMIT ?"
    rows = conn.execute(query, (limit,)).fetchall()
    return [dict(row) for row in rows]


@app.route("/")
def index():
    if not DB_PATH.exists():
        return (
            "<h1>Institutional Flow Tracker</h1>"
            "<p>No database found. Run the ETL pipeline to populate insti_flow.db.</p>"
        )

    summary = []
    with closing(get_connection()) as conn:
        for table, config in TABLE_CONFIG.items():
            count = get_table_count(conn, table)
            preview = get_table_preview(
                conn, table, order_by=config.get("order_by"), limit=config["limit"]
            )
            summary.append(
                {
                    "table": table,
                    "title": config["title"],
                    "count": count,
                    "rows": preview,
                }
            )

    template = """
    <!doctype html>
    <html lang="en">
        <head>
            <meta charset="utf-8">
            <title>Institutional Flow Tracker</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 2rem; background: #f5f5f5; }
                h1 { margin-bottom: 0.25rem; }
                h2 { margin-top: 2rem; }
                .summary { margin-bottom: 1.5rem; padding: 1rem; background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
                table { border-collapse: collapse; width: 100%; margin-top: 0.5rem; }
                th, td { border: 1px solid #ddd; padding: 0.5rem; text-align: left; }
                th { background: #efefef; }
                tbody tr:nth-child(even) { background: #fafafa; }
                code { background: #eee; padding: 0.125rem 0.25rem; border-radius: 4px; }
            </style>
        </head>
        <body>
            <h1>Institutional Flow Tracker</h1>
            <p>Preview of the latest records loaded into <code>insti_flow.db</code>.</p>
            {% for section in summary %}
            <div class="summary">
                <h2>{{ section.title }} <small>({{ section.count }} records)</small></h2>
                {% if section.rows %}
                <table>
                    <thead>
                        <tr>
                            {% for key in section.rows[0].keys() %}
                            <th>{{ key }}</th>
                            {% endfor %}
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in section.rows %}
                        <tr>
                            {% for value in row.values() %}
                            <td>{{ value }}</td>
                            {% endfor %}
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
                {% else %}
                <p>No records available.</p>
                {% endif %}
            </div>
            {% endfor %}
        </body>
    </html>
    """

    return render_template_string(template, summary=summary)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
