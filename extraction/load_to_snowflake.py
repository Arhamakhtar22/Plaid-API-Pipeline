# extraction/load_to_snowflake.py
import os
import pandas as pd
import snowflake.connector
import json
import re
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def extract_field(json_str, field_name):
    """Extract a specific field from the JSON-like string using regex"""
    # Handle different data types appropriately
    pattern = f"'{field_name}':\\s*(.*?),"
    match = re.search(pattern, json_str)
    if match:
        value = match.group(1).strip()
        # Handle None values
        if value == 'None':
            return None
        # Handle numeric values
        elif value.isdigit():
            return int(value)
        elif value.replace('.', '', 1).isdigit():
            return float(value)
        # Handle boolean values
        elif value == 'True':
            return True
        elif value == 'False':
            return False
        # Handle date objects
        elif 'datetime.date' in value:
            date_pattern = r"datetime.date\((\d+),\s*(\d+),\s*(\d+)\)"
            date_match = re.search(date_pattern, value)
            if date_match:
                year, month, day = map(int, date_match.groups())
                return f"{year}-{month:02d}-{day:02d}"
            return None
        # Handle strings (removing quotes)
        elif value.startswith("'") and value.endswith("'"):
            return value[1:-1]
        else:
            return value
    return None

def load_to_snowflake(csv_filepath):
    """Load CSV data to Snowflake by using regex to extract fields"""
    # Debug: Print environment variables
    print(f"Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
    print(f"User: {os.getenv('SNOWFLAKE_USER')}")

    # Read CSV data
    df = pd.read_csv(csv_filepath)

    # Print CSV sample for debugging
    print("\nSample data from CSV:")
    print(df.head())

    # First, get column information from the target table
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

    try:
        cursor = conn.cursor()
        # Get column information from the existing table
        cursor.execute("DESC TABLE RAW_TRANSACTIONS")
        columns_info = cursor.fetchall()

        # Extract column names
        table_columns = [col[0] for col in columns_info]
        print(f"Table columns: {table_columns}")

        # Prepare data for insertion
        rows_to_insert = []

        for _, row in df.iterrows():
            # Get the JSON string
            if '0' in df.columns:
                json_string = row['0']
            elif 'transaction_data' in df.columns:
                json_string = row['transaction_data']
            else:
                print("Could not find the JSON column")
                return False

            # Extract required fields using regex
            data_row = {}
            for col in table_columns:
                if col.upper() == 'EXTRACTED_AT':
                    # Use the extracted_at from the CSV
                    data_row[col] = row['extracted_at']
                else:
                    # Extract field from JSON string
                    field_value = extract_field(json_string, col.lower())
                    data_row[col] = field_value

            # Special handling for CATEGORY which may be a list
            if 'CATEGORY' in data_row and data_row['CATEGORY'] is not None:
                # If it starts with [ it's a list
                if isinstance(data_row['CATEGORY'], str) and data_row['CATEGORY'].startswith('['):
                    # Extract items from the list using regex
                    cat_items = re.findall(r"'([^']+)'", data_row['CATEGORY'])
                    if cat_items:
                        # Join the categories with commas
                        data_row['CATEGORY'] = ', '.join(cat_items)

            rows_to_insert.append(data_row)

        # Print a sample of the parsed data
        if rows_to_insert:
            print("\nSample of parsed data:")
            for key, value in rows_to_insert[0].items():
                print(f"{key}: {value}")
        else:
            print("No rows parsed successfully")
            return False

        # Insert data into Snowflake
        inserted_count = 0
        for row_data in rows_to_insert:
            # Build INSERT statement
            columns = []
            values = []

            for col in table_columns:
                val = row_data.get(col)

                # Handle different data types for SQL
                if val is None:
                    values.append("NULL")
                elif isinstance(val, (int, float)):
                    values.append(str(val))
                elif isinstance(val, bool):
                    values.append("TRUE" if val else "FALSE")
                else:
                    # Properly escape single quotes for SQL by doubling them
                    val_str = str(val).replace("'", "''")
                    values.append(f"'{val_str}'")

                columns.append(col)

            # Create the SQL statement
            insert_sql = f"""
            INSERT INTO RAW_TRANSACTIONS ({', '.join(columns)})
            VALUES ({', '.join(values)})
            """

            try:
                cursor.execute(insert_sql)
                inserted_count += 1
                if inserted_count % 5 == 0:
                    print(f"Inserted {inserted_count} rows...")
            except Exception as e:
                print(f"Error inserting row: {e}")
                print(f"Failed values: {values}")
                continue

        print(f"\nSuccessfully inserted {inserted_count} rows into RAW_TRANSACTIONS")
        conn.commit()
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()

def main():
    """Main loading function"""
    try:
        # Get latest CSV file
        data_dir = 'data/raw'
        files = [f for f in os.listdir(data_dir) if f.startswith('transactions_')]
        if not files:
            print("No transaction files found")
            return False

        latest_file = sorted(files)[-1]
        csv_filepath = os.path.join(data_dir, latest_file)

        # Print for debugging
        print(f"Loading file: {csv_filepath}")

        # Load to Snowflake
        return load_to_snowflake(csv_filepath)
    except Exception as e:
        print(f"Error in main function: {e}")
        return False

if __name__ == "__main__":
    main()