import certifi
import os
os.environ['SSL_CERT_FILE'] = certifi.where()
import pandas as pd
import plaid
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.products import Products
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from datetime import datetime, timedelta, date
import json
import ssl
import time

# loading environment variable from the env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed. Using environment variables directly.")

# SSL verification workaround
# this line modifies the default SSL context to bypass certificate verification
ssl._create_default_https_context = ssl._create_unverified_context

# Configure Plaid client
# use sandbox environment
configuration = plaid.Configuration(
    host=plaid.Environment.Sandbox,
    api_key={
        'clientId': os.getenv('PLAID_CLIENT_ID'),
        'secret': os.getenv('PLAID_SECRET'),
    }
)

# Set verify_ssl to False
# further configuring the API client to ignore SSL certificate requirements
api_client = plaid.ApiClient(configuration)
api_client.rest_client.pool_manager.connection_pool_kw['cert_reqs'] = ssl.CERT_NONE

# initializing the actual Plaid API client that will be used to make requests to the Plaid API
client = plaid_api.PlaidApi(api_client)

def get_access_token(): # Creates a connection to a test bank (Chase) in Plaid's Sandbox environment
    """Generate a sandbox access token with sample data"""
    try:
        # Create the request object with test credentials
        request = SandboxPublicTokenCreateRequest(
            institution_id='ins_109508',  # Chase Bank in Sandbox
            initial_products=[Products('transactions')],
            options={
                'webhook': 'https://www.example.com/webhook',
                'override_username': 'user_good', # TEST CREDENTIALS
                'override_password': 'pass_good'
            }
        )

        # Send the request
        # Obtains a public token, then exchanges it for an access token
        # The access token is like a secure key that lets your app retrieve financial data

        public_token_response = client.sandbox_public_token_create(request)
        public_token = public_token_response['public_token']

        print(f"Created public token: {public_token[:10]}...")

        # Create exchange request object
        exchange_request = ItemPublicTokenExchangeRequest(
            public_token=public_token
        )

        # Exchange public token for access token
        exchange_response = client.item_public_token_exchange(exchange_request)
        access_token = exchange_response['access_token']
        item_id = exchange_response['item_id']

        print(f"Item ID: {item_id}")

        # Force transaction creation in sandbox
        print("Forcing transaction creation in sandbox environment...")
        time.sleep(5)  # Wait a bit for the sandbox to initialize

        return access_token

    except Exception as e:
        print(f"Error getting access token: {e}")
        if hasattr(e, 'body'):
            try:
                error_body = json.loads(e.body)
                print(f"Error details: {json.dumps(error_body, indent=2)}")
            except:
                print(f"Error body: {e.body}")
        return None


# Uses the access token to retrieve transaction data from Plaid

def fetch_transactions(access_token, start_date, end_date, max_retries=5, retry_delay=30):
    """Fetch transaction data from Plaid with retry mechanism and pagination"""
    # Convert datetime objects to date objects if they aren't already
    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()

    all_transactions = []
    has_more = True
    offset = 0

    print(f"Requesting transactions from {start_date} to {end_date}")

    # Continue fetching while there are more transactions
    while has_more:
        # Create options object with current offset
        options = TransactionsGetRequestOptions(
            count=500,
            offset=offset
        )

        # Create request object
        request = TransactionsGetRequest(
            access_token=access_token,
            start_date=start_date,
            end_date=end_date,
            options=options
        )

        # Implement retry mechanism
        success = False
        for attempt in range(max_retries):
            try:
                response = client.transactions_get(request)

                # Get current batch of transactions
                transactions = response['transactions']
                total_transactions = response['total_transactions']

                print(f"Retrieved {len(transactions)} transactions (batch offset: {offset}, total available: {total_transactions})")
                all_transactions.extend(transactions)

                # Update pagination info
                has_more = len(all_transactions) < total_transactions
                offset = len(all_transactions)

                success = True
                # Break out of retry loop if successful
                break

            except Exception as e:
                error_code = None

                # Extract error code if available
                if hasattr(e, 'body'):
                    try:
                        if isinstance(e.body, dict):
                            error_code = e.body.get('error_code')
                        else:
                            body_json = json.loads(e.body)
                            error_code = body_json.get('error_code')
                            print(f"Error response: {json.dumps(body_json, indent=2)}")
                    except Exception as json_error:
                        print(f"Error parsing error response: {json_error}")
                        print(f"Raw error body: {e.body}")

                # Check if it's the PRODUCT_NOT_READY error
                if error_code == "PRODUCT_NOT_READY":
                    if attempt < max_retries - 1:  # If not the last attempt
                        print(f"Data not ready, retrying in {retry_delay} seconds... (Attempt {attempt+1}/{max_retries})")
                        time.sleep(retry_delay)
                    else:
                        print(f"Error fetching transactions after {max_retries} attempts: {e}")
                        if hasattr(e, 'body'):
                            print(f"Response body: {e.body}")
                        return None
                else:
                    # For other errors, print and return None
                    print(f"Error fetching transactions: {e}")
                    if hasattr(e, 'body'):
                        print(f"Response body: {e.body}")
                    return None

        # If we couldn't retrieve this batch successfully after all retries
        if not success:
            break

        # If no more transactions in this batch, break the loop
        if not transactions:
            break

        # Allow a short pause between pagination requests
        if has_more:
            time.sleep(1)

    print(f"Total transactions retrieved: {len(all_transactions)}")

    if not all_transactions:
        print("No transactions found in the date range")
        return pd.DataFrame()

    # Convert to DataFrame
    transactions_df = pd.DataFrame(all_transactions)

    # Add extraction timestamp
    transactions_df['extracted_at'] = datetime.now().isoformat()

    # Print column names for debugging
    print(f"Columns in the transactions dataframe: {transactions_df.columns.tolist()}")

    return transactions_df

# Creates a directory structure for storing the data


def save_to_csv(df, filename):
    """Save DataFrame to CSV file"""
    output_dir = 'data/raw'
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)

    # Save both the raw transactions and a simplified version
    df.to_csv(filepath, index=False)

    # Also save the transaction data as a column for easier processing
    transaction_data_df = pd.DataFrame({
        'transaction_data': df.apply(lambda x: x.to_json(), axis=1),
        'extracted_at': df['extracted_at']
    })
    simplified_filepath = os.path.join(output_dir, f"simplified_{filename}")
    transaction_data_df.to_csv(simplified_filepath, index=False)

    print(f"Saved {len(df)} records to {filepath}")
    print(f"Saved simplified version to {simplified_filepath}")

    return filepath

def main():
    """Main extraction function"""
    print("Starting data extraction from Plaid API...")

    # Check if Plaid credentials are set
    if not os.getenv('PLAID_CLIENT_ID') or not os.getenv('PLAID_SECRET'):
        print("ERROR: Plaid credentials not found in environment variables.")
        print("Make sure PLAID_CLIENT_ID and PLAID_SECRET are set in your .env file.")
        return False

    # Setup dates (last 2 years instead of 30 days)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=730)  # 2 years of data

    print(f"Requesting transactions from {start_date} to {end_date} (2 year range)")

    # Get access token with test data
    access_token = get_access_token()

    if access_token is None:
        print("Failed to obtain access token. Check your Plaid credentials.")
        return False

    print(f"Successfully obtained access token: {access_token[:5]}...")

    # Fetch transaction data with retry mechanism and pagination
    transactions_df = fetch_transactions(access_token, start_date, end_date, max_retries=5, retry_delay=30)

    if transactions_df is not None and not transactions_df.empty:
        # Save raw data
        filename = f"transactions_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
        save_to_csv(transactions_df, filename)

        print(f"Data extraction completed successfully! Retrieved {len(transactions_df)} transactions.")

        # Print sample of the data
        print("\nSample of retrieved data:")
        print(transactions_df.head(3))

        return True

    print("No transaction data retrieved or empty dataframe returned.")
    return False

if __name__ == "__main__":
    main()
