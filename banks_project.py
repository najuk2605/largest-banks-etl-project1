"""ETL project: World's Largest Banks by Market Capitalization."""

from datetime import datetime
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


BASE_DIR = Path(__file__).resolve().parent

URL = (
    "https://web.archive.org/web/20230908091635/"
    "https://en.wikipedia.org/wiki/List_of_largest_banks"
)

LIVE_URL = "https://en.wikipedia.org/wiki/List_of_largest_banks"

EXCHANGE_RATE_URL = (
    "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
    "IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
)

EXCHANGE_RATE_FILE = BASE_DIR / "exchange_rate.csv"
OUTPUT_CSV = BASE_DIR / "Largest_banks_data.csv"
DATABASE = BASE_DIR / "Banks.db"
TABLE_NAME = "Largest_banks"
LOG_FILE = BASE_DIR / "code_log.txt"

ATTRIBUTES = ["Name", "MC_USD_Billion"]


# ---------------------------------------------------------
# Task 1: Logging
# ---------------------------------------------------------
def log_progress(message):
    """Log a timestamped message to code_log.txt."""

    timestamp = datetime.now().strftime("%Y-%b-%d-%H:%M:%S")

    with LOG_FILE.open("a", encoding="utf-8") as file:
        file.write(f"{timestamp} : {message}\n")


# ---------------------------------------------------------
# Helper function
# ---------------------------------------------------------
def _get_html(url):
    """Download HTML from the supplied URL."""

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BanksETL/1.0)"
    }

    response = requests.get(
        url,
        headers=headers,
        timeout=30
    )

    response.raise_for_status()

    return response.text


# ---------------------------------------------------------
# Task 2: Extract
# ---------------------------------------------------------
def extract(url, table_attribs=None):
    """Extract bank names and market capitalization into a DataFrame."""

    if table_attribs is None:
        table_attribs = ATTRIBUTES

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
        raise ValueError("No HTML tables were found.")

    target_table = None

    for table in tables:
        headers = [
            cell.get_text(" ", strip=True)
            for cell in table.find_all("th")
        ]

        header_text = " | ".join(headers).lower()

        if (
            "market cap" in header_text
            or "market capitalization" in header_text
        ):
            target_table = table
            break

    if target_table is None:
        target_table = tables[0]

    rows = target_table.find_all("tr")

    records = []

    for row in rows:

        cells = row.find_all("td")

        if len(cells) < 3:
            continue

        links = cells[1].find_all("a")

        if len(links) < 2:
            continue

        name = (
            links[-1].get("title")
            or links[-1].get_text(" ", strip=True)
        )

        market_cap_text = cells[2].get_text(
            " ",
            strip=True
        )

        market_cap_text = (
            market_cap_text
            .replace(",", "")
            .replace("$", "")
            .strip()
        )

        try:
            market_cap = float(market_cap_text)

        except ValueError:
            continue

        records.append(
            {
                "Name": name,
                "MC_USD_Billion": market_cap
            }
        )

        if len(records) == 10:
            break

    df = pd.DataFrame(
        records,
        columns=table_attribs
    )

    if len(df) != 10:
        raise ValueError(
            f"Expected 10 banks, extracted {len(df)} rows."
        )

    return df


# ---------------------------------------------------------
# Download exchange rates
# ---------------------------------------------------------
def download_exchange_rates(
    csv_path=EXCHANGE_RATE_FILE
):
    """Download exchange-rate CSV if it does not exist."""

    if csv_path.exists():
        return csv_path

    response = requests.get(
        EXCHANGE_RATE_URL,
        timeout=30
    )

    response.raise_for_status()

    csv_path.write_bytes(response.content)

    return csv_path


# ---------------------------------------------------------
# Task 3: Transform
# ---------------------------------------------------------
def transform(df, csv_path):
    """Add GBP, EUR and INR market-capitalization columns."""

    rates = pd.read_csv(csv_path)

    exchange_rate = (
        rates
        .set_index("Currency")["Rate"]
        .to_dict()
    )

    required = {"GBP", "EUR", "INR"}

    missing = required - set(exchange_rate)

    if missing:
        raise ValueError(
            f"Missing exchange rates: {sorted(missing)}"
        )

    df = df.copy()

    df["MC_USD_Billion"] = pd.to_numeric(
        df["MC_USD_Billion"],
        errors="raise"
    )

    gbp_rate = float(exchange_rate["GBP"])
    eur_rate = float(exchange_rate["EUR"])
    inr_rate = float(exchange_rate["INR"])

    df["MC_GBP_Billion"] = [
        np.round(
            x * gbp_rate,
            2
        )
        for x in df["MC_USD_Billion"]
    ]

    df["MC_EUR_Billion"] = [
        np.round(
            x * eur_rate,
            2
        )
        for x in df["MC_USD_Billion"]
    ]

    df["MC_INR_Billion"] = [
        np.round(
            x * inr_rate,
            2
        )
        for x in df["MC_USD_Billion"]
    ]

    return df


# ---------------------------------------------------------
# Task 4: Load to CSV
# ---------------------------------------------------------
def load_to_csv(df, output_path):
    """Save DataFrame to CSV and log the action."""

    df.to_csv(
        output_path,
        index=False
    )

    log_progress("Data saved to CSV file")


# ---------------------------------------------------------
# Task 5: Load to Database
# ---------------------------------------------------------
def load_to_db(
    df,
    sql_connection,
    table_name
):
    """Save DataFrame to SQLite database table."""

    df.to_sql(
        table_name,
        sql_connection,
        if_exists="replace",
        index=False
    )

    log_progress(
        "Data loaded to Database as a table, Executing queries"
    )


# ---------------------------------------------------------
# Task 6: Run SQL Query
# ---------------------------------------------------------
def run_query(
    query_statement,
    sql_connection
):
    """Execute SQL query, print results and log execution."""

    print(f"\n{query_statement}")

    query_output = pd.read_sql_query(
        query_statement,
        sql_connection
    )

    print(
        query_output.to_string(index=False)
    )

    log_progress(
        f"Executed query: {query_statement}"
    )

    return query_output


# Also provide the name used in some versions of the lab.
run_queries = run_query


# ---------------------------------------------------------
# Main ETL Process
# ---------------------------------------------------------
def main():

    log_progress(
        "Preliminaries complete. Initiating ETL process"
    )

    download_exchange_rates()

    df = extract(
        URL,
        ATTRIBUTES
    )

    print("\nExtracted data:")
    print(
        df.to_string(index=False)
    )

    log_progress(
        "Data extraction complete. Initiating Transformation process"
    )

    df = transform(
        df,
        EXCHANGE_RATE_FILE
    )

    print("\nTransformed data:")
    print(
        df.to_string(index=False)
    )

    log_progress(
        "Data transformation complete. Initiating Loading process"
    )

    load_to_csv(
        df,
        OUTPUT_CSV
    )

    sql_connection = sqlite3.connect(
        DATABASE
    )

    log_progress(
        "SQL Connection initiated"
    )

    try:

        load_to_db(
            df,
            sql_connection,
            TABLE_NAME
        )

        run_query(
            f"SELECT * FROM {TABLE_NAME}",
            sql_connection
        )

        run_query(
            f"SELECT AVG(MC_GBP_Billion) FROM {TABLE_NAME}",
            sql_connection
        )

        run_query(
            f"SELECT Name FROM {TABLE_NAME} LIMIT 5",
            sql_connection
        )

        log_progress(
            "Process Complete"
        )

    finally:

        sql_connection.close()

        log_progress(
            "Server Connection closed"
        )


if __name__ == "__main__":
    main()