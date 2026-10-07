"""ETL project: World's Largest Banks by Market Capitalization.

Implements the IBM Skills Network lab workflow:
extract -> transform -> load to CSV -> load to SQLite -> query -> log.
"""

from datetime import datetime
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Project configuration
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
LIVE_URL = "https://en.wikipedia.org/wiki/List_of_largest_banks"
EXCHANGE_RATE_URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
EXCHANGE_RATE_FILE = BASE_DIR / "exchange_rate.csv"
OUTPUT_CSV = BASE_DIR / "Largest_banks_data.csv"
DATABASE = BASE_DIR / "Banks.db"
TABLE_NAME = "Largest_banks"
LOG_FILE = BASE_DIR / "code_log.txt"
ATTRIBUTES = ["Name", "MC_USD_Billion"]


def log_progress(message: str) -> None:
    """Append a timestamped progress message to ``code_log.txt``."""
    timestamp = datetime.now().strftime("%Y-%b-%d-%H:%M:%S")
    with LOG_FILE.open("a", encoding="utf-8") as file:
        file.write(f"{timestamp} : {message}\n")


def _get_html(url: str) -> str:
    """Download the source page, falling back to Wikipedia if the archive is unavailable."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; BanksETL/1.0)"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def extract(url: str, table_attribs: list[str]) -> pd.DataFrame:
    """Extract the first 'By market capitalization' table into a DataFrame.

    The archived page used by the lab contains the desired table first. The
    implementation identifies the table by its header where possible, then
    falls back to the first table containing a market-capitalization column.
    """
    try:
        html = _get_html(url)
    except requests.RequestException:
        if url != LIVE_URL:
            html = _get_html(LIVE_URL)
        else:
            raise

    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    if not tables:
        raise ValueError("No HTML tables were found on the source page.")

    target = None
    for table in tables:
        headers = [cell.get_text(" ", strip=True) for cell in table.find_all("th")]
        header_text = " | ".join(headers).lower()
        if "market cap" in header_text or "market capitalization" in header_text:
            target = table
            break

    if target is None:
        target = tables[0]

    rows = target.find_all("tr")
    records = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        # In the lab table the second <td> contains country + bank links.
        links = cells[1].find_all("a")
        if len(links) < 2:
            continue

        name = links[-1].get("title") or links[-1].get_text(" ", strip=True)
        market_cap_text = cells[2].get_text(" ", strip=True)
        market_cap_text = market_cap_text.replace(",", "").replace("$", "").strip()
        # The lab notes that the source value has a trailing newline/character.
        market_cap_text = market_cap_text.rstrip("\n")

        try:
            market_cap = float(market_cap_text)
        except ValueError:
            continue

        records.append({"Name": name, "MC_USD_Billion": market_cap})
        if len(records) == 10:
            break

    df = pd.DataFrame(records, columns=table_attribs)
    if len(df) != 10:
        raise ValueError(f"Expected 10 banks, extracted {len(df)} rows.")
    return df


def download_exchange_rates(csv_path: Path = EXCHANGE_RATE_FILE) -> Path:
    """Download the lab exchange-rate CSV if it is not already present."""
    if csv_path.exists():
        return csv_path

    response = requests.get(EXCHANGE_RATE_URL, timeout=30)
    response.raise_for_status()
    csv_path.write_bytes(response.content)
    return csv_path


def transform(df: pd.DataFrame, csv_path: str | Path) -> pd.DataFrame:
    """Add GBP, EUR and INR market-capitalization columns."""
    rates = pd.read_csv(csv_path)
    exchange_rate = rates.set_index("Currency")["Rate"].to_dict()

    required = {"GBP", "EUR", "INR"}
    missing = required - set(exchange_rate)
    if missing:
        raise ValueError(f"Missing exchange rates: {sorted(missing)}")

    df = df.copy()
    df["MC_USD_Billion"] = pd.to_numeric(df["MC_USD_Billion"], errors="raise")
    gbp_rate = float(exchange_rate["GBP"])
    eur_rate = float(exchange_rate["EUR"])
    inr_rate = float(exchange_rate["INR"])

    df["MC_GBP_Billion"] = [np.round(x * gbp_rate, 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [np.round(x * eur_rate, 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(x * inr_rate, 2) for x in df["MC_USD_Billion"]]
    return df


def load_to_csv(df: pd.DataFrame, output_path: str | Path) -> None:
    """Save the final DataFrame as a CSV file without the pandas index."""
    df.to_csv(output_path, index=False)


def load_to_db(df: pd.DataFrame, sql_connection: sqlite3.Connection, table_name: str) -> None:
    """Save the final DataFrame as a SQLite table."""
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement: str, sql_connection: sqlite3.Connection) -> pd.DataFrame:
    """Execute a SQL query, print it and its result, and return the result."""
    print(f"\n{query_statement}")
    query_output = pd.read_sql_query(query_statement, sql_connection)
    print(query_output.to_string(index=False))
    return query_output


# Alias matching the lab wording in Task 6.
run_queries = run_query


def main() -> None:
    """Run the complete ETL pipeline."""
    log_progress("Preliminaries complete. Initiating ETL process")

    # Download the supplied exchange-rate file when needed.
    download_exchange_rates()

    df = extract(URL, ATTRIBUTES)
    log_progress("Data extraction complete. Initiating Transformation process")

    df = transform(df, EXCHANGE_RATE_FILE)
    print("\nTransformed data:")
    print(df.to_string(index=False))
    log_progress("Data transformation complete. Initiating Loading process")

    load_to_csv(df, OUTPUT_CSV)
    log_progress("Data saved to CSV file")

    sql_connection = sqlite3.connect(DATABASE)
    log_progress("SQL Connection initiated")

    try:
        load_to_db(df, sql_connection, TABLE_NAME)
        log_progress("Data loaded to Database as a table, Executing queries")

        run_query(f"SELECT * FROM {TABLE_NAME}", sql_connection)
        run_query(f"SELECT AVG(MC_GBP_Billion) FROM {TABLE_NAME}", sql_connection)
        run_query(f"SELECT Name FROM {TABLE_NAME} LIMIT 5", sql_connection)
        log_progress("Process Complete")
    finally:
        sql_connection.close()
        log_progress("Server Connection closed")


if __name__ == "__main__":
    main()
