# Largest Banks ETL Project

## Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline for processing data on the world's largest banks by market capitalization.

The project extracts the top 10 largest banks by market capitalization, transforms their market capitalization values from USD into GBP, EUR, and INR using the provided exchange rates, and loads the processed data into a CSV file and a SQLite database.

## ETL Process

### 1. Extract
- Extracts the top 10 banks from the largest banks data source.
- Retrieves:
  - Bank Name
  - Market Capitalization in USD billion

### 2. Transform
The USD market capitalization values are converted into:
- GBP billion
- EUR billion
- INR billion

The converted values are rounded to two decimal places.

### 3. Load
The transformed data is saved to:

- `Largest_banks_data.csv`
- `Banks.db`

The SQLite database contains the table:

- `Largest_banks`

## Project Files

| File | Description |
|---|---|
| `banks_project.py` | Main Python ETL script |
| `Largest_banks_data.csv` | Processed bank data |
| `Banks.db` | SQLite database containing the processed data |
| `code_log.txt` | ETL process log |
| `requirements.txt` | Required Python packages |
| `README.md` | Project documentation |

## Technologies Used

- Python
- Pandas
- NumPy
- BeautifulSoup
- Requests
- SQLite
- lxml

## Database Queries

The project executes SQL queries to:

1. Display all records from the `Largest_banks` table.
2. Calculate the average market capitalization in GBP.
3. Display the first five bank names.

## Author

Najuk Lanjewar
