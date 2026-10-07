import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3


# Task 1: Logging function
def log_progress(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open("code_log.txt", "a") as f:
        f.write(timestamp + " : " + message + "\n")


# Task 2: Extraction function
def extract(url, table_attribs):

    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    tables = soup.find_all("table")

    for table in tables:

        headers = [
            th.get_text(strip=True)
            for th in table.find_all("th")
        ]

        if "Market cap (US$ billion)" in headers:

            rows = []

            for row in table.find_all("tr")[1:]:
                cells = row.find_all(["td", "th"])

                if cells:
                    rows.append([
                        cell.get_text(strip=True)
                        for cell in cells
                    ])

            df = pd.DataFrame(rows, columns=headers)

            df = df[table_attribs]

            df["MC_USD_Billion"] = (
                df["MC_USD_Billion"]
                .str.replace("\n", "", regex=False)
                .astype(float)
            )

            return df


# Task 3: Transformation function
def transform(df, csv_path):

    exchange_rate = pd.read_csv(csv_path)

    exchange_rate = exchange_rate.set_index(
        exchange_rate.columns[0]
    ).to_dict()[exchange_rate.columns[1]]

    gbp_rate = float(exchange_rate["GBP"])
    eur_rate = float(exchange_rate["EUR"])
    inr_rate = float(exchange_rate["INR"])

    df["MC_GBP_Billion"] = [
        np.round(x * gbp_rate, 2)
        for x in df["MC_USD_Billion"]
    ]

    df["MC_EUR_Billion"] = [
        np.round(x * eur_rate, 2)
        for x in df["MC_USD_Billion"]
    ]

    df["MC_INR_Billion"] = [
        np.round(x * inr_rate, 2)
        for x in df["MC_USD_Billion"]
    ]

    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)

# Task 5: Load to Database
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


# Task 6: Run SQL query
def run_query(query_statement, sql_connection):

    print("\nQuery:")
    print(query_statement)

    print("\nResult:")
    print(pd.read_sql(query_statement, sql_connection))


# -------------------------------------------------
# Main Program
# -------------------------------------------------

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"

exchange_rate_csv = "exchange_rate.csv"

output_csv = "./Largest_banks_data.csv"

database_name = "Banks.db"

table_name = "Largest_banks"

table_attribs = [
    "Name",
    "MC_USD_Billion"
]


# Task 1
log_progress(
    "Preliminaries complete. Initiating ETL process"
)


def extract(url, table_attribs):

    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    tables = soup.find_all("table")

    for table in tables:

        rows = table.find_all("tr")

        for row in rows:

            cells = row.find_all(["th", "td"])

            if cells:
                row_text = " ".join(
                    cell.get_text(" ", strip=True)
                    for cell in cells
                )

                if "Market cap" in row_text and "US$ billion" in row_text:

                    data = []

                    for data_row in rows[1:]:
                        columns = data_row.find_all("td")

                        if len(columns) >= 3:

                            name = columns[1].get_text(
                                " ", strip=True
                            )

                            market_cap = columns[2].get_text(
                                " ", strip=True
                            )

                            if name and market_cap:
                                try:
                                    market_cap = float(
                                        market_cap.replace(",", "")
                                    )

                                    data.append([
                                        name,
                                        market_cap
                                    ])

                                except ValueError:
                                    pass

                    df = pd.DataFrame(
                        data,
                        columns=table_attribs
                    )

                    return df
               

df = extract(url, table_attribs)

print("\nExtracted Data:")
print(df)

log_progress(
    "Data extraction complete. Initiating Transformation process"
)

df = transform(df, exchange_rate_csv)             
print("\nTransformed Data:")
print(df)

log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, output_csv)

log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect(database_name)

log_progress("SQL Connection initiated")

load_to_db(df, sql_connection, table_name)

log_progress("Data loaded to Database as a table, Executing queries")

run_query("SELECT * FROM Largest_banks", sql_connection)

run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", sql_connection)

run_query("SELECT Name FROM Largest_banks LIMIT 5", sql_connection)

log_progress("Process Complete")

sql_connection.close()

log_progress("Server Connection closed")

 
