"""
BTC Monitor System
-------------------

This script monitors the Bitcoin blockchain, processes events, and extracts transaction data based on specified filters. It provides detailed logs, stores data in a SQLite database, and generates event summaries and reports. Below is a description of the key functions used in this script.

1. **init_log()**
   - Initializes the logging configuration.
   - Sets up logging to output to a log file and configures logging levels based on the `CONFIG` settings.

2. **main_initialization()**
   - Performs the initial setup of the BTC Monitor System.
   - Calls `init_log()` to set up logging.
   - Creates `Transaction_Types.csv` with default transaction types using `create_transaction_types_csv()`.
   - Initializes the SQLite databases by calling `initialize_databases()`.

3. **print_settings()**
   - Logs the current configuration settings of the BTC Monitor System.
   - Provides details such as the RPC host and port, BTC transfer limits, and file paths.

4. **connect_rpc()**
   - Establishes a connection to the Bitcoin RPC node using the provided credentials.
   - Returns an `AuthServiceProxy` object for interacting with the Bitcoin node.
   - Logs an error and returns `None` if the connection fails.

5. **load_transaction_types(file_path="Transaction_Types.csv")**
   - Loads transaction types from a CSV file into a dictionary.
   - Each transaction type includes properties like description, whether it requires an address, and reporting options.
   - Logs the number of transaction types loaded.

6. **initialize_databases()**
   - Initializes the SQLite database for storing address data with a unified schema.
   - Creates the necessary tables (`addresses`) and indexes if they do not exist.
   - Ensures the database aligns with the updated schema, including fields like `block_number` and `vout_index`.

7. **find_block_by_time(btc_rpc, target_time)**
   - Finds the block height closest to the specified `target_time`.
   - Uses a binary search algorithm for efficient retrieval.
   - Returns the block height of the block closest to but not exceeding the `target_time`.
   - Includes error handling and logging.

8. **get_transactions_in_time_range(btc_rpc, start_time, end_time, min_btc_transfer, max_btc_transfer)**
   - Retrieves transactions between the specified `start_time` and `end_time`.
   - Filters transactions based on minimum and maximum BTC transfer amounts.
   - Utilizes block heights corresponding to the start and end times found using `find_block_by_time()`.
   - Returns a list of transactions that meet the criteria.

9. **process_events(transaction_types, btc_rpc, min_transfer=None, max_transfer=None, time_before_event=None)**
   - Processes events listed in `changeevents.csv`.
   - For each event:
     - Reads event details such as `name`, `date_time`, and `change_percent`.
     - Calculates the time range for transaction retrieval.
     - Retrieves transactions within the time range using `get_transactions_in_time_range()`.
     - Processes the transactions using `process_transactions()`.
     - Generates an event summary and writes it to `events.csv` using `write_event_summary()`.
   - Stores transaction data and addresses in the SQLite database.
   - Handles errors gracefully and logs them without stopping the entire process.

10. **process_transactions(btc_rpc, transactions, event_name, event_time, event_change, transaction_types, address_conn)**
    - Processes a list of transactions for a specific event.
    - Analyzes transaction details and categorizes them by transaction type.
    - Collects statistics such as total transaction value, fees, and transaction counts.
    - Extracts addresses from transaction outputs and inserts them into the database using `insert_address()` or `write_address_records()`.

11. **insert_address(cursor, address, value, event_time, txid, tx_time, block_number, price_change, event_name, vout_index)**
    - Inserts an individual address record into the SQLite database.
    - Includes detailed transaction information such as `txid`, `block_number`, `tx_time`, and `vout_index`.
    - Aligns with the database schema.

12. **write_address_records(cursor, address_records, table_name="addresses")**
    - Inserts multiple address records into the specified SQLite table.
    - Handles all necessary fields and aligns with the updated database schema.
    - Uses `INSERT OR IGNORE` to prevent duplicate entries based on the unique constraint.

13. **write_event_summary(event_summary)**
    - Writes the event summary to the `events.csv` file.
    - Includes statistics like total transaction value, number of addresses, fee information, and transaction type counts.
    - Ensures dynamic fields are included and data is correctly serialized.
    - Handles `Decimal` types by converting them to `float` for CSV serialization.

14. **deduplicate_addresses_db(conn, table_name)**
    - Consolidates address records in the database by aggregating data and removing duplicate entries.
    - Aggregates values for each address and updates the database to ensure only one consolidated record exists per address.
    - Ensures data integrity and avoids redundancy.
    - Considers whether deduplication is necessary based on the database's unique constraints.

15. **generate_reports(conn, enable_csv, table_name)**
    - Generates CSV reports based on the data stored in the SQLite database.
    - Exports data to CSV files if CSV reporting is enabled in the configuration.
    - Calls `export_sql_to_csv()` to perform the actual export.

16. **log(message, level)**
    - Logs messages with a specified logging level.
    - Supports different logging levels for fine-grained control over log output.
    - Logs messages to both the console (for level 1 messages) and the log file.

17. **parse_args()**
    - Parses command-line arguments for the script.
    - Supports options like enabling batch mode via the `--batch` argument.
    - Returns the parsed arguments for use in the script.

18. **read_batch_file(batch_file)**
    - Reads batch job configurations from a CSV file.
    - Each batch job specifies parameters like `name`, `min_transfer`, `max_transfer`, and `time_before_event`.
    - Returns a list of batch jobs to be processed.

19. **normalize(value)**
    - Safely normalizes numeric values, converting them to `Decimal` for consistent precision.
    - Handles `None` and invalid inputs gracefully, returning `Decimal('0.0')` in such cases.

20. **start_timer() and stop_timer(start_time)**
    - Utility functions to measure execution time.
    - `start_timer()` records the current time.
    - `stop_timer(start_time)` calculates the elapsed time since `start_time`.

21. **main()**
    - The entry point of the script.
    - Performs the following steps:
      - Declares global variables.
      - Calls `main_initialization()` to set up the system.
      - Prints the current configuration settings using `print_settings()`.
      - Parses command-line arguments.
      - Connects to the Bitcoin RPC node using `connect_rpc()`.
      - Loads transaction types using `load_transaction_types()`.
      - Retrieves the latest block height from the Bitcoin node.
      - Displays the startup screen with the current configuration.
      - Checks if batch mode is enabled and processes batch jobs if so.
      - If not in batch mode, processes events from `changeevents.csv` using `process_events()`.
      - Deduplicates addresses in the database if enabled.
      - Exports deduplicated data to CSV files if enabled.
      - Generates final reports for addresses.
      - Logs the total runtime of the script.

**Main Execution Flow:**
- The script begins by calling `main()`, which orchestrates the entire execution.
- It initializes logging and databases, and prints settings for verification.
- Connects to the Bitcoin RPC node to interact with the blockchain.
- Loads transaction types to categorize transactions during processing.
- Processes events either from batch jobs or the `changeevents.csv` file.
- For each event, transactions are retrieved, processed, and relevant data is stored in the database.
- Event summaries and reports are generated and written to CSV files.
- Addresses are deduplicated to ensure data integrity and avoid redundancy.
- Final reports are generated, and the total runtime is logged.

**Note:** The script utilizes a configuration dictionary `CONFIG` to manage settings such as file paths, RPC credentials, and feature flags. Logging levels can be adjusted through the configuration to control the verbosity of the output.

"""


import os
import csv
import time
import sys
import argparse
import sqlite3
import pdb
import traceback
import logging
import configparser
from collections import defaultdict
#from datetime import datetime, timedelta
# import datetime
from datetime import datetime, timedelta

from decimal import getcontext, Decimal, InvalidOperation
getcontext().prec = 8  # Adjust based on required precision
transaction_types = None
btc_rpc = None

# Third-party imports
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

# Local module imports
from btc_monitor_startup import btc_monitor_startup


# Configuration dictionary
CONFIG = {
    'version': '3.00',
    'rpc_user': 'wrapperband',
    'rpc_password': 'ardork54',
    'rpc_host': 'localhost',
    'rpc_port': 18332,
    'min_btc_transfer': 10.0,
    'max_btc_transfer': 100.0,
    'time_before_event': 84000,  # in seconds
    'average_transaction_time':0.12, # Average time per transaction in seconds
    'debug': 0,
    'level':logging.INFO,  # Set Logging level
    'log_to_screen': True,  # Set to True to print INFO level messages to the screen
    'log_file': 'btc_monitor.log',
    'change_events_file': 'changeevents.csv',
    'address_file': 'addresses.db',
    'coinbase_address_file': 'coinbase_addresses.db',
    'events_summary_file': 'events.csv',
    'transaction_types_file': 'Transaction_Types.csv',
    'use_sql_for_addresses': True,
    'deduplicate_addresses_db': True,
    'csv_report_enabled': True,
    'csv_output_dir': './',
    'coinbase_addresses_csv': 'coinbase_addresses.csv',
    'addresses_csv': 'addresses.csv',
    'sql_commit_batch_size': 100,
    'log_sql_errors': True,
    'log_addresses': False,
    'log_coinbase_addresses': False,
    'batch_jobs_file': 'overnight_batch.csv',
    'log_spent':False,
    'debugger': False
}


def create_or_load_config(config_file='btc_monitor.conf'):
    """
    Create or load the btc_monitor.conf file. If it doesn't exist, a new one will be created
    with the current settings from the CONFIG dictionary. If it exists, the settings will 
    be loaded from the file and override the default CONFIG settings.
    
    Parameters:
    - config_file (str): Path to the configuration file (default is 'btc_monitor.conf').
    """
    config_parser = configparser.ConfigParser()

    # Check if the config file exists
    if not os.path.exists(config_file):
        # Create config file from the default CONFIG settings
        print(f"{config_file} not found. Creating default configuration file.")

        # Write default CONFIG settings to btc_monitor.conf
        config_parser['DEFAULT'] = {key: str(value) for key, value in CONFIG.items()}
        with open(config_file, 'w') as configfile:
            config_parser.write(configfile)
        print(f"Default configuration written to {config_file}.")
    else:
        # Load the existing config file and override CONFIG with it
        config_parser.read(config_file)
        
        # Define expected data types for each config key
        CONFIG_TYPES = {
            'version': str,
            'rpc_user': str,
            'rpc_password': str,
            'rpc_host': str,
            'rpc_port': int,
            'min_btc_transfer': float,
            'max_btc_transfer': float,
            'time_before_event': int,
            'average_transaction_time': float,
            'debug': int,
            'level': int,
            'log_to_screen': bool,
            'log_file': str,
            'change_events_file': str,
            'address_file': str,
            'coinbase_address_file': str,
            'events_summary_file': str,
            'transaction_types_file': str,
            'use_sql_for_addresses': bool,
            'deduplicate_addresses_db': bool,
            'csv_report_enabled': bool,
            'csv_output_dir': str,
            'coinbase_addresses_csv': str,
            'addresses_csv': str,
            'sql_commit_batch_size': int,
            'log_sql_errors': bool,
            'log_addresses': bool,
            'log_coinbase_addresses': bool,
            'batch_jobs_file': str,
            'log_spent': bool,
            'debugger': bool
        }
        
        for key, expected_type in CONFIG_TYPES.items():
            if key in config_parser['DEFAULT']:
                try:
                    if expected_type == int:
                        CONFIG[key] = config_parser.getint('DEFAULT', key)
                    elif expected_type == bool:
                        CONFIG[key] = config_parser.getboolean('DEFAULT', key)
                    elif expected_type == float:
                        CONFIG[key] = config_parser.getfloat('DEFAULT', key)
                    else:
                        CONFIG[key] = config_parser.get('DEFAULT', key)
                except Exception as e:
                    log(f"Error reading configuration key '{key}': {e}", level=1)
                    sys.exit(1)
            else:
                # Key not found in config file; use default value from CONFIG
                pass
        print(f"Configuration loaded from {config_file}.")

def init_log():
    """Initializes the logging configuration."""
    logging.basicConfig(
        filename=CONFIG.get('log_file', 'btc_monitor.log'),
        level=CONFIG.get('level', logging.INFO),  # Use CONFIG['level'] for logging
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def log(message, level=1):
    """
    Custom logging function to control logging based on the debug level and log_to_screen flag.
    - Level 1: Info
    - Level 2: Warning
    - Level 3: Error
    - Level 4: Critical
    - Level 7 and 8: Debug levels
    """
    # Map custom levels to logging levels
    level_mapping = {
        1: logging.INFO,
        2: logging.WARNING,
        3: logging.ERROR,
        4: logging.CRITICAL,
        7: logging.DEBUG,
        8: logging.DEBUG
    }

    # If the current log level exceeds the set debug level, ignore the message
    if level > CONFIG['debug']:
        return
    
    # If log_to_screen is enabled and the message level is within the debug range, print to screen
    if CONFIG.get('log_to_screen', False) and level <= CONFIG['debug']:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")
    
    # Log to file based on the mapped logging level
    logging.log(level_mapping.get(level, logging.INFO), message)



# New Function to Create Transaction_Types.csv
def create_transaction_types_csv(transaction_types, csv_file='Transaction_Types.csv'):
    """
    Writes the transaction types to a CSV file with the required fields.
    
    Parameters:
    - transaction_types (list of tuples): The transaction types with all fields.
    - csv_file (str): The path to the output CSV file.
    """
    headers = ['type_name', 'description', 'requires_address', 'is_rejected', 
               'count_in_summary', 'report_filename', 'database_report', 'csv_report']
    
    try:
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)  # Write header
            
            for tx_type in transaction_types:
                writer.writerow(tx_type)  # Write each transaction type
                
        log(f"Transaction types successfully written to {csv_file}.", logging.INFO)
    
    except Exception as e:
        log(f"Error writing to {csv_file}: {e}", logging.ERROR)
        sys.exit(1)


# Performs the initial setup:
def main_initialization():
    """
    Performs the initial setup:
    - Initializes logging.
    - Creates the Transaction_Types.csv with default transaction types.
    - Initializes the addresses database.
    """
    # Initialize logging
    init_log()
    log("Starting BTC Monitor System Initialization.", logging.INFO)
    
    # Create Transaction Types CSV
    create_transaction_types_csv(STANDARD_TRANSACTION_TYPES, CONFIG['transaction_types_file'])
    
    # Initialize Databases
    initialize_databases()
    
    log("BTC Monitor System Initialization Completed Successfully.", logging.INFO)


def print_settings():
    log("----- BTC Monitor Configuration -----", 1)
    log(f"RPC_URL: http://{CONFIG['rpc_user']}:{CONFIG['rpc_password']}@{CONFIG['rpc_host']}:{CONFIG['rpc_port']}", 1)
    log(f"MIN_BTC_TRANSFER: {CONFIG['min_btc_transfer']} BTC", 1)
    log(f"MAX_BTC_TRANSFER: {CONFIG['max_btc_transfer'] if CONFIG['max_btc_transfer'] is not None else 'None'}", 1)
    log(f"TIME_BEFORE_EVENT: {CONFIG['time_before_event']} seconds", 1)
    log(f"CHANGE_EVENTS_FILE: {CONFIG['change_events_file']}", 1)
    log(f"ADDRESS_FILE: {CONFIG['address_file']}", 1)
    log(f"EVENTS_SUMMARY_FILE: {CONFIG['events_summary_file']}", 1)
    log(f"DEBUG_LEVEL: {CONFIG['debug']}", 1)
    log(f"Logging addresses is {'enabled' if CONFIG['log_addresses'] else 'disabled'}", level=1)
    log(f"Logging coinbase addresses is {'enabled' if CONFIG['log_coinbase_addresses'] else 'disabled'}", level=1)
    log(f"'Spent' field calculation is {'enabled' if CONFIG['log_spent'] else 'disabled'}", level=1)
    log("--------------------------------------", 1)
    sys.stdout.flush()

# Function to encode decimals safely
def EncodeDecimal(o):
    try:
        if o is None:
            log(f"None value passed to EncodeDecimal", 3)
            return None
        return float(round(Decimal(o), 8))
    except (InvalidOperation, ValueError, TypeError):
        log(f"Invalid value encountered during decimal conversion: {o}", 1)
        return None


# RPC Connection
def connect_rpc():
    """Connect to Bitcoin RPC."""
    try:
        rpc_url = f"http://{CONFIG['rpc_user']}:{CONFIG['rpc_password']}@{CONFIG['rpc_host']}:{CONFIG['rpc_port']}"
        return AuthServiceProxy(rpc_url, timeout=300)
    except Exception as e:
        log(f"Error connecting to Bitcoin RPC: {e}", 1)
        return None

# Standard transaction types (updated with new fields)
STANDARD_TRANSACTION_TYPES = [
    ('Taproot', 'Witness v1 Taproot transaction', True, True, 'Yes', 'taproot_addresses.db', False, False),
    ('OP_RETURN', 'Null data transaction', False, True, 'No', '', False, False),
    ('Coinbase', 'Block reward transaction', True, True, 'Yes', 'coinbase_addresses.db', True, True),
    ('P2PKH', 'Pay to Public Key Hash', True, False, 'Yes', 'P2PKH_addresses.db', False, False),
    ('P2SH', 'Pay to Script Hash', True, False, 'Yes', 'P2SH_addresses.db', False, False),
    ('P2WPKH', 'Pay to Witness Public Key Hash', True, False, 'Yes', 'P2WPKH_addresses.db', False, False),
    ('Ordinals', 'Inscriptions or metadata transactions', True, False, 'Yes', 'Ordinals_addresses.db', False, False)
]

def generate_type_report_database(address_conn, transaction_type, report_db_path):
    """
    Generates a report database for a specific transaction type by filtering records from addresses.db.

    Parameters:
    - address_conn (sqlite3.Connection): Connection to addresses.db.
    - transaction_type (str): The type_name of the transaction (e.g., 'Coinbase').
    - report_db_path (str): The filename for the report database (e.g., 'Coinbase_Report.db').
    """
    try:
        cursor = address_conn.cursor()
        # Fetch transactions of the specified type using the indexed 'source' field
        cursor.execute("""
            SELECT address, value, event_time, txid, tx_time, block_number, price_change,
                   address_name, source, event_name, address_description, spent, vout_index
            FROM addresses
            WHERE source = ?
            ORDER BY event_time DESC;
        """, (transaction_type,))
        records = cursor.fetchall()
        columns = [description[0] for description in cursor.description]

        # Connect to or create the report database
        report_conn = sqlite3.connect(report_db_path)
        report_cursor = report_conn.cursor()

        # Create the report table with the same schema
        table_name = f"{transaction_type.lower()}_addresses"
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT UNIQUE NOT NULL,
            value REAL,
            event_time TEXT,
            txid TEXT,
            tx_time TEXT,
            block_number INTEGER,
            price_change REAL,
            address_name TEXT,
            source TEXT,
            event_name TEXT,
            address_description TEXT,
            spent BOOLEAN,
            vout_index INTEGER,
            UNIQUE(address, txid, vout_index)
        );
        """
        report_cursor.execute(create_table_query)

        # Insert the filtered records into the report table
        insert_query = f"""
        INSERT OR IGNORE INTO {table_name}
        (address, value, event_time, txid, tx_time, block_number, price_change,
         address_name, source, event_name, address_description, spent, vout_index)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        # Exclude 'id' from the records
        formatted_records = [
            (
                record[0],  # address
                record[1],  # value
                record[2],  # event_time
                record[3],  # txid
                record[4],  # tx_time
                record[5],  # block_number
                record[6],  # price_change
                record[7],  # address_name
                record[8],  # source
                record[9],  # event_name
                record[10], # address_description
                record[11], # spent
                record[12]  # vout_index
            )
            for record in records
        ]
        report_cursor.executemany(insert_query, formatted_records)

        # Create an index on the 'address' field in the report database
        CREATE_INDEX_REPORT_QUERY = f"""
        CREATE INDEX IF NOT EXISTS idx_address_{transaction_type.lower()} ON {table_name} (address);
        """
        report_cursor.execute(CREATE_INDEX_REPORT_QUERY)

        report_conn.commit()
        report_conn.close()

        log(f"Report database '{report_db_path}' generated with {len(records)} records for type '{transaction_type}'.", level=3)

    except sqlite3.Error as e:
        log(f"Error generating report database '{report_db_path}': {e}", level=1)
    except Exception as e:
        log(f"Unexpected error generating report database '{report_db_path}': {e}", level=1)
        traceback.print_exc()



def generate_all_type_reports(address_conn, transaction_types):
    """
    Iterates through all transaction types and generates reports based on configuration.
    
    Parameters:
    - address_conn (sqlite3.Connection): Connection to addresses.db.
    - transaction_types (dict): Dictionary of transaction types loaded from CSV.
    """
    for type_name, config in transaction_types.items():
        report_filename = config.get('report_filename')
        database_report = config.get('database_report', False)
        csv_report = config.get('csv_report', False)
        
        if report_filename:
            # Generate Database Report if enabled
            if database_report:
                generate_type_report_database(
                    address_conn,
                    transaction_type=type_name,
                    report_db_path=report_filename
                )
            
            # Generate CSV Report if enabled
            if csv_report:
                # Determine the output CSV filename (e.g., 'Coinbase_Report.csv')
                output_csv = os.path.splitext(report_filename)[0] + '.csv'
                export_report_db_to_csv(
                    report_db_path=report_filename,
                    output_csv=output_csv
                )


# Initialize SQLite databases
def initialize_databases():
    """Initializes the SQLite database for addresses with a unified schema and necessary indexes."""
    try:
        # Define the unified schema with additional fields
        address_table_schema = """
            CREATE TABLE IF NOT EXISTS addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT UNIQUE NOT NULL,
                value REAL,
                event_time TEXT,
                txid TEXT,
                tx_time TEXT,
                block_number INTEGER,
                price_change REAL,
                address_name TEXT,
                source TEXT,
                event_name TEXT,
                address_description TEXT,
                spent BOOLEAN,
                vout_index INTEGER,
                UNIQUE(address, txid, vout_index)
            );
        """

        # Define the indexing queries
        CREATE_INDEX_ADDRESSES_QUERY = """
        CREATE INDEX IF NOT EXISTS idx_source ON addresses (source);
        CREATE INDEX IF NOT EXISTS idx_address_block ON addresses (address, block_number);
        CREATE INDEX IF NOT EXISTS idx_address_name ON addresses (address_name);
        """

        # Initialize addresses.db if it doesn't exist
        if not os.path.exists(CONFIG['address_file']):
            conn = sqlite3.connect(CONFIG['address_file'])
            cursor = conn.cursor()
            cursor.execute(address_table_schema)
            cursor.executescript(CREATE_INDEX_ADDRESSES_QUERY)  # Execute all index creations
            conn.commit()
            conn.close()
            log(f"Address database created at {CONFIG['address_file']}", level=1)

        log("Databases initialized successfully with unified schema and indexes.", level=1)

    except sqlite3.Error as e:
        log(f"Error initializing databases: {e}", level=1)
        sys.exit(1)


# Step 1: Function to load transaction types from Transaction_Types.csv
# Load transaction types from CSV
def load_transaction_types(file_path="Transaction_Types.csv"):
    """Loads transaction types from a CSV file into a dictionary."""
    transaction_types = {}
    try:
        with open(file_path, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                transaction_type = {
                    'description': row['description'],
                    'requires_address': row['requires_address'].strip().lower() == 'true',
                    'is_rejected': row['is_rejected'].strip().lower() == 'true',
                    'count_in_summary': row['count_in_summary'].strip().lower() == 'yes',
                    'report_filename': row['report_filename'].strip(),
                    'database_report': row['database_report'].strip().lower() == 'true',
                    'csv_report': row['csv_report'].strip().lower() == 'true'
                }
                transaction_types[row['type_name'].strip()] = transaction_type
        log(f"Loaded {len(transaction_types)} transaction types from {file_path}.", level=3)
    except FileNotFoundError:
        log(f"Error: {file_path} not found.", level=1)
        sys.exit(1)
    except Exception as e:
        log(f"Error loading transaction types from {file_path}: {e}", level=1)
        sys.exit(1)
    return transaction_types



def normalize(value):
    """
    Safely normalizes the value, handling different possible types.
    Ensures the value is converted to a Decimal, handling None and invalid types.
    """
    try:
        if value is None:
            return Decimal('0.0')  # Ensures None is explicitly treated as decimal zero
        return Decimal(str(value))  # Convert value to string before passing to Decimal for consistency
    except (InvalidOperation, ValueError, TypeError):
        log(f"Invalid value encountered during normalization: {value}", level=1)
        return Decimal('0.0')  # Return zero if an error occurs



# Generalized INSERT Query (applies to both addresses and coinbase_addresses)
def insert_address(cursor, address, value, event_time, txid, tx_time, block_number, price_change,
                   event_name, vout_index, source=None, address_name=None, address_description=None, spent=False):
    """
    Inserts an address record into the SQLite database.

    Parameters:
    - cursor: SQLite cursor object.
    - address: Bitcoin address.
    - value: Value associated with the address (from the transaction output).
    - event_time: Event time (Unix timestamp as TEXT).
    - txid: Transaction ID.
    - tx_time: Transaction time (Unix timestamp as TEXT).
    - block_number: Block number containing the transaction.
    - price_change: Price change associated with the event.
    - event_name: Name of the event.
    - vout_index: Index of the transaction output.
    - source: Source of the transaction type.
    - address_name: Optional name for the address.
    - address_description: Optional description for the address.
    - spent: Boolean indicating whether the output is spent.
    """
    # Ensure all values are of correct types
    address = str(address) if address is not None else ''
    value = float(value) if value is not None else 0.0
    event_time = str(event_time) if event_time is not None else ''
    txid = str(txid) if txid is not None else ''
    tx_time = str(tx_time) if tx_time is not None else ''
    block_number = int(block_number) if block_number is not None else 0
    price_change = float(price_change) if price_change is not None else 0.0
    event_name = str(event_name) if event_name is not None else ''
    vout_index = int(vout_index) if vout_index is not None else 0
    source = str(source) if source is not None else ''
    address_name = str(address_name) if address_name is not None else ''
    address_description = str(address_description) if address_description is not None else ''
    spent = bool(spent)

    try:
        cursor.execute('''
            INSERT INTO addresses (
                address, value, event_time, txid, tx_time, block_number, price_change,
                address_name, source, event_name, address_description, spent, vout_index
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (address, value, event_time, txid, tx_time, block_number, price_change,
              address_name, source, event_name, address_description, spent, vout_index))
    except sqlite3.IntegrityError as e:
        log(f"IntegrityError inserting address {address}: {e}", level=3)
    except sqlite3.Error as e:
        log(f"Error inserting address {address}: {e}", level=1)
        traceback.print_exc()




def generate_reports(conn, enable_csv, table_name):
    """
    Generates CSV reports at the end of processing, based on the SQL data.
    
    :param conn: SQLite connection object.
    :param enable_csv: Boolean indicating whether CSV reporting is enabled.
    :param table_name: Name of the table (either "addresses" or "coinbase_addresses").
    """
    try:
        if CONFIG.get('use_sql_for_addresses', False) is False:
            log(f"SQL writing is disabled. Skipping CSV generation for {table_name}.", 1)
            return
        
        if enable_csv and CONFIG.get('csv_report_enabled', False):
            log(f"Generating CSV report for {table_name}.", 1)
            export_sql_to_csv(conn, table_name, CONFIG[f"{table_name}_csv"])
        else:
            log(f"CSV generation is disabled. Only SQL processing was performed for {table_name}.", 1)

    except sqlite3.Error as e:
        log(f"Error generating report for {table_name}: {e}", 1)

 
def write_addresses_to_db(cursor, address_records):
    """
    Inserts address records into the addresses table.
    
    Parameters:
    - cursor (sqlite3.Cursor): SQLite cursor object.
    - address_records (list of dict): List of address records to insert.
    """
    for record in address_records:
        cursor.execute('''
            INSERT OR IGNORE INTO addresses 
            (address, value, percentage_of_total, txid, tx_time, event_time, price_change, event_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            record['address'], 
            record['value'], 
            record['percentage_of_total'], 
            record['txid'], 
            record['tx_time'], 
            record['event_time'], 
            record['price_change'], 
            record['event_name']
        ))
    log(f"Inserted {len(address_records)} records into addresses table", level=5)



def export_sql_to_csv(conn, table_name, csv_file):
    """
    Export data from the SQL database to a CSV file.

    :param conn: SQLite connection object.
    :param table_name: Name of the table to export data from (e.g., 'addresses' or 'coinbase_addresses').
    :param csv_file: Path to the CSV file to write the data to.
    """
    try:
        cursor = conn.cursor()
        
        # Fetch all data from the specified table
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        # Fetch column names from the cursor description
        columns = [description[0] for description in cursor.description]

        log(f"Exporting {len(rows)} rows from {table_name} to {csv_file}.", 1)

        # Write the data to the CSV file
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(columns)  # Write header row with column names
            writer.writerows(rows)    # Write the data rows

        log(f"Exported {len(rows)} rows from {table_name} to {csv_file}.", 5)

    except sqlite3.Error as e:
        log(f"Error exporting data from {table_name} to CSV: {e}", 1)
    except Exception as e:
        log(f"Unexpected error while exporting {table_name} to CSV: {e}", 1)

        
def process_block_for_coinbase(btc_rpc, block_hash, conn, coinbase_csv_file):
    """
    Processes the coinbase transaction(s) in a given block, inserts the details into the database
    based on configuration settings, and exports the records to a CSV file.

    Parameters:
    - btc_rpc: The connected Bitcoin RPC instance.
    - block_hash: The hash of the block to process.
    - conn: SQLite connection to the database.
    - coinbase_csv_file: The file where coinbase transaction records will be written.
    """
    # Enter debugger if enabled in the config
    if CONFIG.get('debugger', False):
        pdb.set_trace()  # Enter the debugger

    try:
        # Get the block data with transaction details
        block = btc_rpc.getblock(block_hash, 2)
        block_time_unix = block.get('time', 0)
        block_time_str = datetime.utcfromtimestamp(block_time_unix).strftime('%Y-%m-%dT%H:%M:%SZ')
        block_height = int(block.get('height', 0))

        log(f"Processing block {block_hash} for coinbase transactions", level=1)

        # Create a new cursor object from the database connection
        cursor = conn.cursor()

        # Check configuration settings
        log_addresses = CONFIG.get('log_coinbase_addresses', False)
        log_spent = CONFIG.get('log_spent', False)

        # Iterate over all transactions in the block
        for tx in block['tx']:
            # Check if this is a coinbase transaction by looking for 'coinbase' in the input
            if 'coinbase' in tx.get('vin', [{}])[0]:
                txid = tx['txid']
                tx_time_unix = tx.get('time', block_time_unix)
                tx_time_str = datetime.utcfromtimestamp(tx_time_unix).strftime('%Y-%m-%dT%H:%M:%SZ')

                # Process each output in the transaction
                for vout_index, vout in enumerate(tx['vout']):
                    script_pub_key = vout.get('scriptPubKey', {})
                    addresses = script_pub_key.get('addresses', [])
                    if not addresses and 'address' in script_pub_key:
                        addresses = [script_pub_key['address']]
                    if not addresses and 'address' in vout:
                        addresses = [vout['address']]
                    if not addresses:
                        address = None
                    else:
                        address = addresses[0]

                    value = Decimal(str(vout.get('value', '0.0')))

                    log(f"Coinbase transaction: {txid} to address {address} with value {value} BTC", level=1)

                    # Skip the transaction if no address is found
                    if address is None:
                        log(f"Skipping insert for transaction {txid} as the address is None", level=8)
                        continue

                    # Determine if the output is spent if log_spent is enabled
                    spent = False
                    if log_spent:
                        # Implement logic to determine if the output is spent
                        # For now, we'll set it to False or you can implement actual logic
                        # spent = is_output_spent(btc_rpc, txid, vout_index)
                        spent = False

                    # Insert the address and value into the database if logging addresses is enabled
                    if log_addresses:
                        insert_address(
                            cursor=cursor,
                            address=address,
                            value=value,
                            event_time=block_time_str,
                            txid=txid,
                            tx_time=tx_time_str,
                            block_number=block_height,
                            price_change=Decimal('0.0'),
                            event_name="coinbase",
                            vout_index=vout_index,
                            source="Coinbase",
                            address_name=None,
                            address_description=None,
                            spent=spent
                        )
                        log(f"Inserted coinbase address {address} from transaction {txid}", level=7)
                    else:
                        log(f"Address logging is disabled. Skipping address {address} from transaction {txid}", level=7)

        # Commit the database changes after processing the block if logging addresses
        if log_addresses:
            conn.commit()

        # Optionally export the processed coinbase transactions to a CSV file
        if log_addresses:
            export_to_csv(conn, coinbase_csv_file)
            log(f"Coinbase transactions written to {coinbase_csv_file}", level=8)

    except Exception as e:
        log(f"Error processing block {block_hash} for coinbase transactions: {e}", level=1)
        traceback.print_exc()



def find_block_by_time(btc_rpc, target_time):
    """Finds the block height closest to the target time using binary search."""
    try:
        lower = 0
        upper = btc_rpc.getblockcount()
        if upper == 0:
            log("Blockchain is empty. Returning block height 0.", level=1)
            return 0  # Only the genesis block exists
        while lower <= upper:
            mid = (lower + upper) // 2
            block_hash = btc_rpc.getblockhash(mid)
            block_header = btc_rpc.getblockheader(block_hash)
            block_time = block_header.get('time', 0)
            if block_time < target_time:
                lower = mid + 1
            elif block_time > target_time:
                upper = mid - 1
            else:
                return mid  # Exact match found
        # Ensure upper is within valid range
        if upper < 0:
            log("No blocks found before the target time. Returning block height 0.", level=1)
            return 0
        return upper  # Closest block before the target time
    except Exception as e:
        log(f"Error finding block by time: {e}", level=1)
        traceback.print_exc()
        return None


def run_batch_jobs(batch_jobs, transaction_types, btc_rpc):
    """
    Runs batch jobs sequentially. Each batch job has custom parameters such as min_transfer, max_transfer,
    and time_before_event, which are passed as arguments to the event processing function.
    """
    for job in batch_jobs:
        log(f"Running batch job: {job['name']}", level=1)
        try:
            # Extract job-specific parameters with enhanced 'None' handling
            min_transfer_str = job.get('min_transfer', '').strip().lower()
            max_transfer_str = job.get('max_transfer', '').strip().lower()
            time_before_event_str = job.get('time_before_event', '').strip().lower()

            min_transfer = float(job['min_transfer']) if min_transfer_str not in ['none', ''] else None
            max_transfer = float(job['max_transfer']) if max_transfer_str not in ['none', ''] else None
            time_before_event = int(job['time_before_event']) if time_before_event_str not in ['none', ''] else None
            filter_name = job['name']  # Use the job name as filter_name

            log(f"Job parameters: min_transfer={min_transfer}, max_transfer={max_transfer}, time_before_event={time_before_event}", level=1)

            # Call the event processing function with all the parameters
            process_events(
                transaction_types,
                btc_rpc,
                min_transfer=min_transfer,
                max_transfer=max_transfer,
                time_before_event=time_before_event,
                event_length=time_before_event,  # Assume event_length equals time_before_event
                filter_name=filter_name            # Pass filter_name from batch job
            )

        except Exception as e:
            log(f"Exception occurred during batch job {job['name']} - {e}. Job details: {job}", level=1)
            traceback.print_exc()
            continue




# Function to get input value for fee calculation
def get_input_value(btc_rpc, vin):
    """Retrieves the value of a transaction input by fetching the previous transaction's output."""
    if 'txid' not in vin or 'vout' not in vin:
        return Decimal('0.0')
    try:
        prev_tx = btc_rpc.getrawtransaction(vin['txid'], True)
        prev_vout = prev_tx['vout'][vin['vout']]
        return Decimal(str(prev_vout.get('value', '0.0')))
    except Exception as e:
        log(f"Error fetching input value for txid {vin.get('txid', 'N/A')}: {e}", 1)
        return Decimal('0.0')




def process_transactions(btc_rpc, transactions, event_name, event_time, event_change, transaction_types, address_conn, min_transfer, max_transfer):
    """
    Processes a list of Bitcoin transactions, calculates event statistics,
    and inserts address data into the SQLite database based on configuration.

    Parameters:
    - btc_rpc: Bitcoin RPC connection object.
    - transactions: List of Bitcoin transactions to process.
    - event_name: The name of the event being processed.
    - event_time: The Unix timestamp of the event.
    - event_change: The price change associated with the event.
    - transaction_types: Dictionary of transaction types loaded from CSV.
    - address_conn: SQLite database connection for addresses.
    - min_transfer (float or Decimal): Minimum BTC transfer amount to consider.
    - max_transfer (float or Decimal): Maximum BTC transfer amount to consider.

    Returns:
    - event_summary (dict): A dictionary containing various statistics for the processed transactions.
    """
    event_summary = {
        'event_name': event_name,
        'event_time': event_time,
        'event_change': event_change,
        'total_tx_value': Decimal('0.0'),
        'total_addresses': 0,
        'min_tx_value': None,
        'max_tx_value': None,
        'avg_tx_value': Decimal('0.0'),
        'total_fees': Decimal('0.0'),
        'avg_fee': Decimal('0.0'),
        'min_fee': None,
        'max_fee': None,
        'fee_tx_ratio': Decimal('0.0'),
        'rejected_transactions': 0,
        'total_processed_transactions': 0,
        'total_outputs': 0,  # New field added
        'transaction_type_counts': {key + '_count': 0 for key in transaction_types.keys()},
        'transaction_type_proportions': {key + '_proportion': Decimal('0.0') for key in transaction_types.keys()},
    }

    tx_values = []
    fees = []
    unique_addresses = set()
    start_time = time.time()
    cursor = address_conn.cursor()

    # Convert event_time to ISO 8601 format once
    event_time_str = datetime.utcfromtimestamp(event_time).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Log configuration settings
    log(f"Logging addresses is {'enabled' if CONFIG['log_addresses'] else 'disabled'}", level=3)
    log(f"'Spent' field calculation is {'enabled' if CONFIG['log_spent'] else 'disabled'}", level=3)

    # Log applied filters
    log(f"Applying filters - Min Transfer: {min_transfer} BTC, Max Transfer: {max_transfer} BTC", level=5)

    for tx in transactions:
        try:
            # Debug log for transaction processing start
            log(f"Processing transaction {tx['txid']}...", level=7)

            # Skip coinbase transactions
            first_vin = tx.get('vin', [{}])[0]
            if first_vin.get('coinbase'):
                log(f"Skipping coinbase transaction {tx['txid']}", level=7)
                # Increment Coinbase_count
                event_summary['transaction_type_counts']['Coinbase_count'] += 1
                # Optionally, count outputs of coinbase transactions
                event_summary['total_outputs'] += len(tx.get('vout', []))
                continue

            # Calculate total output value
            tx_value = sum([Decimal(str(vout.get('value', '0.0'))) for vout in tx.get('vout', [])])
            log(f"Transaction {tx['txid']} total output value: {tx_value} BTC", level=7)

            # Filter transactions based on min and max transfer
            if min_transfer is not None and tx_value < Decimal(str(min_transfer)):
                log(f"Transaction {tx['txid']} below min transfer ({tx_value} BTC < {min_transfer} BTC), skipping.", level=5)
                continue
            if max_transfer is not None and tx_value > Decimal(str(max_transfer)):
                log(f"Transaction {tx['txid']} above max transfer ({tx_value} BTC > {max_transfer} BTC), skipping.", level=5)
                continue

            tx_values.append(tx_value)
            event_summary['total_tx_value'] += tx_value

            # Calculate total input value by fetching previous outputs
            total_in = Decimal('0.0')
            for vin in tx.get('vin', []):
                input_value = get_input_value(btc_rpc, vin)
                total_in += input_value
                log(f"Transaction {tx['txid']} input value: {input_value} BTC", level=7)

            # Calculate fee
            fee = total_in - tx_value
            fees.append(fee)
            event_summary['total_fees'] += fee
            log(f"Transaction {tx['txid']} fee: {fee} BTC", level=7)

            # Convert tx_time to ISO 8601 format
            tx_time_unix = tx.get('time', event_time)
            tx_time_str = datetime.utcfromtimestamp(tx_time_unix).strftime('%Y-%m-%dT%H:%M:%SZ')

            # Update transaction type counts and collect addresses
            for vout_index, vout in enumerate(tx.get('vout', [])):
                script_pub_key = vout.get('scriptPubKey', {})
                script_type = script_pub_key.get('type', 'unknown')
                log(f"Transaction {tx['txid']} output script type: {script_type}", level=7)

                # Update transaction type counts
                if script_type in transaction_types:
                    event_summary['transaction_type_counts'][script_type + '_count'] += 1
                else:
                    # Handle unknown transaction types
                    event_summary['transaction_type_counts'][script_type + '_count'] = 1
                    transaction_types[script_type] = {
                        'description': 'Unknown',
                        'requires_address': True,
                        'is_rejected': False,
                        'count_in_summary': True
                    }

                # Increment total_outputs
                event_summary['total_outputs'] += 1

                # Collect addresses regardless of CONFIG['log_addresses']
                if transaction_types[script_type]['requires_address']:
                    addresses = script_pub_key.get('addresses', [])
                    if not addresses and 'address' in script_pub_key:
                        addresses = [script_pub_key['address']]
                    if not addresses and 'address' in vout:
                        addresses = [vout['address']]  # Some versions may have 'address' directly in vout
                    unique_addresses.update(addresses)

                    # Determine if the output is spent if log_spent is enabled
                    spent = False
                    if CONFIG['log_spent']:
                        # Implement logic to determine if the output is spent
                        # For now, we'll set it to False or you can implement actual logic
                        # spent = is_output_spent(btc_rpc, tx['txid'], vout_index)
                        spent = False

                    # Insert addresses into database only if logging addresses is enabled
                    if CONFIG['log_addresses']:
                        for address in addresses:
                            insert_address(
                                cursor=cursor,
                                address=address,
                                value=Decimal(str(vout.get('value', '0.0'))),
                                event_time=event_time_str,        # Use the ISO formatted string
                                txid=tx['txid'],
                                tx_time=tx_time_str,              # Use the ISO formatted string
                                block_number=tx.get('block_number', 0),
                                price_change=Decimal(str(event_change)),
                                event_name=event_name,
                                vout_index=vout_index,
                                source=script_type,
                                address_name=None,
                                address_description=None,
                                spent=spent
                            )
                            log(f"Inserted address {address} from transaction {tx['txid']}", level=7)

            # Increment total processed transactions
            event_summary['total_processed_transactions'] += 1

        except Exception as e:
            log(f"Error processing transaction {tx.get('txid', 'N/A')}: {e}", level=1)
            traceback.print_exc()
            event_summary['rejected_transactions'] += 1

    # Calculate statistics after processing all transactions
    if tx_values:
        event_summary['min_tx_value'] = min(tx_values)
        event_summary['max_tx_value'] = max(tx_values)
        event_summary['avg_tx_value'] = sum(tx_values) / Decimal(len(tx_values))
        log(f"Transaction value stats - Min: {event_summary['min_tx_value']} BTC, Max: {event_summary['max_tx_value']} BTC, Avg: {event_summary['avg_tx_value']} BTC", level=7)
    if fees:
        event_summary['min_fee'] = min(fees)
        event_summary['max_fee'] = max(fees)
        event_summary['avg_fee'] = sum(fees) / Decimal(len(fees))
        log(f"Fee stats - Min: {event_summary['min_fee']} BTC, Max: {event_summary['max_fee']} BTC, Avg: {event_summary['avg_fee']} BTC", level=7)
        if event_summary['total_tx_value'] > 0:
            event_summary['fee_tx_ratio'] = event_summary['total_fees'] / event_summary['total_tx_value']
            log(f"Fee to transaction value ratio: {event_summary['fee_tx_ratio']}", level=7)

    # **Always** set total_addresses regardless of CONFIG['log_addresses']
    event_summary['total_addresses'] = len(unique_addresses)
    event_summary['event_processing_duration'] = time.time() - start_time

    # Calculate transaction type proportions based on total_outputs
    for key in transaction_types.keys():
        count = event_summary['transaction_type_counts'].get(key + '_count', 0)
        proportion_key = key + '_proportion'
        if event_summary['total_outputs'] > 0:
            # Calculate proportion as a percentage
            event_summary['transaction_type_proportions'][proportion_key] = (Decimal(count) / Decimal(event_summary['total_outputs'])) * 100
        else:
            event_summary['transaction_type_proportions'][proportion_key] = Decimal('0.0')
        log(f"Transaction type {key} - Count: {count}, Proportion: {event_summary['transaction_type_proportions'][proportion_key]}%", level=7)

    # Commit address insertions if logging addresses
    if CONFIG['log_addresses']:
        address_conn.commit()

    return event_summary


# Function to get transactions within the time range
def get_transactions_in_time_range(btc_rpc, start_time, end_time, min_btc_transfer, max_btc_transfer):
    """
    Retrieves transactions from blocks within the specified time range.

    Parameters:
    - btc_rpc: Bitcoin RPC connection object.
    - start_time: Start time (Unix timestamp) of the time range.
    - end_time: End time (Unix timestamp) of the time range.
    - min_btc_transfer: Minimum BTC transfer value to filter transactions.
    - max_btc_transfer: Maximum BTC transfer value to filter transactions.

    Returns:
    - transactions: List of transactions within the time range, including block number, txid, tx_time.
    """
    transactions = []
    try:
        # Find the starting and ending block heights based on the time range
        start_block_height = find_block_by_time(btc_rpc, start_time)
        end_block_height = find_block_by_time(btc_rpc, end_time)

        log(f"Fetching blocks between heights {start_block_height} and {end_block_height}.", level=7)

        # Loop through the blocks and collect transactions
        for block_height in range(start_block_height, end_block_height + 1):
            block_hash = btc_rpc.getblockhash(block_height)
            block = btc_rpc.getblock(block_hash, 2)  # '2' fetches the full transaction details

            block_time = block.get('time', 0)
            block_number = block_height

            for tx in block['tx']:
                # Add block information to the transaction
                tx['block_number'] = block_number
                tx['block_time'] = block_time

                # Calculate the transaction value
                tx_total_value = sum([float(vout['value']) for vout in tx['vout'] if 'value' in vout])

                # Filter based on transaction value
                if min_btc_transfer is not None and tx_total_value < min_btc_transfer:
                    # log(f"DEBUG7: Transaction {tx['txid']} ignored due to transfer value below min: {tx_total_value} BTC", level=7)
                    continue
                if max_btc_transfer is not None and tx_total_value > max_btc_transfer:
                    # log(f"DEBUG7: Transaction {tx['txid']} ignored due to transfer value above max: {tx_total_value} BTC", level=7)
                    continue

                transactions.append(tx)

        log(f"Found {len(transactions)} transactions within the time range.", level=7)
        return transactions

    except Exception as e:
        log(f"Error retrieving transactions: {str(e)}", level=1)
        return []



def process_transaction_outputs(btc_rpc, transaction, transaction_types, address_conn, event_time, event_name, event_change, event_summary):
    """
    Processes the outputs of a transaction, collects addresses, and updates event summary.

    Parameters:
    - btc_rpc: Bitcoin RPC connection object.
    - transaction: The transaction to process.
    - transaction_types: Dictionary of transaction types loaded from CSV.
    - address_conn: SQLite database connection for addresses.
    - event_time: The Unix timestamp of the event.
    - event_name: The name of the event being processed.
    - event_change: The price change associated with the event.
    - event_summary: Dictionary to update with statistics.
    """
    try:
        cursor = address_conn.cursor()
        unique_addresses = event_summary.get('unique_addresses', set())

        for vout in transaction.get('vout', []):
            value = vout.get('value')
            if value is not None:
                normalized_value = normalize(value)

                # Log the value for debugging
                log(f"Processing output value: {normalized_value}", level=9)

                # Apply the min/max transfer filter
                if CONFIG['min_btc_transfer'] <= normalized_value <= CONFIG['max_btc_transfer']:
                    log(f"Transaction {transaction['txid']} passes the filter with value: {normalized_value}", level=7)

                    script_pub_key = vout.get('scriptPubKey', {})
                    script_type = script_pub_key.get('type', 'unknown')

                    # Update transaction type counts
                    if script_type in transaction_types:
                        event_summary['transaction_type_counts'][script_type + '_count'] += 1
                    else:
                        # Handle unknown transaction types
                        event_summary['transaction_type_counts'][script_type + '_count'] = 1
                        transaction_types[script_type] = {
                            'description': 'Unknown',
                            'requires_address': True,
                            'is_rejected': False,
                            'count_in_summary': True
                        }

                    # Collect addresses
                    addresses = script_pub_key.get('addresses', [])
                    if not addresses and 'address' in script_pub_key:
                        addresses = [script_pub_key['address']]

                    unique_addresses.update(addresses)

                    # Insert addresses into database
                    for address in addresses:
                        insert_address(cursor, address, normalized_value, event_time, transaction['txid'], transaction.get('time', event_time), event_change, event_name)
                else:
                    log(f"Transaction {transaction['txid']} does not pass the filter.", level=7)
            else:
                # Log and skip outputs without a 'value' field (e.g., OP_RETURN)
                log(f"Output in transaction {transaction['txid']} does not have a 'value' field. Skipping this output.", level=7)

        # Update event summary with unique addresses
        event_summary['unique_addresses'] = unique_addresses
        address_conn.commit()

    except Exception as e:
        log(f"Error processing transaction outputs for {transaction.get('txid', 'N/A')}: {e}", level=1)
        event_summary['rejected_transactions'] += 1






# Process and write events summary
# Main function to process events
def process_events(transaction_types, btc_rpc, min_transfer=None, max_transfer=None, time_before_event=None, event_length=None, filter_name=None):
    """
    Main function that processes all events from the change events CSV file.
    It retrieves and processes Bitcoin transactions within a time range for each event.

    Parameters:
    - transaction_types (dict): Dictionary of transaction types loaded from CSV.
    - btc_rpc: Bitcoin RPC connection object.
    - min_transfer (float or None): Minimum BTC transfer amount to consider.
    - max_transfer (float or None): Maximum BTC transfer amount to consider. If None, no max limit is applied.
    - time_before_event (int): Time before the event to start processing transactions.
    - event_length (int): Duration in seconds of the event (typically the same as time_before_event).
    - filter_name (str): The name of the filter applied, typically from batch job (optional).
    """

    # Initialize average transaction processing time from CONFIG
    average_time_per_tx = CONFIG.get('average_transaction_time', 0.1)  # Default to 0.1 seconds if not set

    try:
        # Read the events from the CSV file
        with open(CONFIG['change_events_file'], mode='r') as csvfile:
            reader = csv.DictReader(csvfile)
            events = list(reader)
            log(f"Processing {len(events)} events from {CONFIG['change_events_file']}...", level=1)
    except FileNotFoundError:
        log(f"Error: {CONFIG['change_events_file']} not found.", level=1)
        sys.exit(1)
    except Exception as e:
        log(f"Unexpected error reading {CONFIG['change_events_file']}: {e}", level=1)
        traceback.print_exc()
        sys.exit(1)

    try:
        # Connect to the SQLite database
        with sqlite3.connect(CONFIG['address_file']) as address_conn:
            # Iterate over each event in the list
            for idx, event in enumerate(events):
                try:
                    event_name = event['name']
                    event_time_str = event['date_time']
                    event_change = float(event.get('change_percent', '0.0'))

                    # Parse event_time_str into a Unix timestamp
                    try:
                        event_time = int(datetime.strptime(event_time_str, '%Y/%m/%d %H:%M:%S').timestamp())
                    except ValueError as ve:
                        log(f"Invalid date format for event '{event_name}': {event_time_str}. Error: {ve}", level=1)
                        continue

                    # Convert event_time to human-readable format
                    human_readable_time = datetime.utcfromtimestamp(event_time).strftime('%Y-%m-%d %H:%M:%S')
                    log(f"Processing event '{event_name}' at time {human_readable_time}.", level=1)

                    # Use provided time_before_event or default from CONFIG
                    current_time_before_event = time_before_event if time_before_event is not None else CONFIG['time_before_event']

                    start_time_range = event_time - current_time_before_event
                    end_time_range = event_time

                    # Use provided min_transfer and max_transfer or defaults from CONFIG
                    current_min_transfer = min_transfer if min_transfer is not None else CONFIG['min_btc_transfer']
                    current_max_transfer = max_transfer  # This line is correct

                    log(f"Processing events with min_transfer={current_min_transfer}, max_transfer={current_max_transfer}...", level=1)

                    # Retrieve transactions within the time range
                    transactions = get_transactions_in_time_range(
                        btc_rpc,
                        start_time_range,
                        end_time_range,
                        current_min_transfer,
                        current_max_transfer
                    )

                    log(f"Found {len(transactions)} transactions within the time range for event '{event_name}'.", level=7)

                    # Estimate Processing Time
                    if average_time_per_tx > 0 and len(transactions) > 0:
                        estimated_processing_time = len(transactions) * average_time_per_tx  # in seconds
                        estimated_finish_time = datetime.now() + timedelta(seconds=estimated_processing_time)
                        estimated_time_str = str(timedelta(seconds=int(estimated_processing_time)))
                        estimated_finish_time_str = estimated_finish_time.strftime('%Y-%m-%d %H:%M:%S')
                        log(f"Estimated processing time: {estimated_time_str}. Expected finish time: {estimated_finish_time_str}.", level=5)
                        print(f"Estimated processing time: {estimated_time_str}. Expected finish time: {estimated_finish_time_str}.")
                    elif len(transactions) > 0:
                        log("Estimating processing time based on the first event.", level=5)
                        print("Estimating processing time based on the first event.")
                    else:
                        log("No transactions to process for this event.", level=5)
                        print("No transactions to process for this event.")

                    # Start Timer
                    processing_start_time = time.time()

                    # Process the transactions and generate an event summary
                    event_summary = process_transactions(
                        btc_rpc,
                        transactions,
                        event_name,
                        event_time,
                        event_change,
                        transaction_types,
                        address_conn,
                        min_transfer=current_min_transfer,
                        max_transfer=current_max_transfer
                    )

                    # End Timer
                    processing_end_time = time.time()
                    actual_processing_time = processing_end_time - processing_start_time  # in seconds
                    actual_time_str = str(timedelta(seconds=int(actual_processing_time)))
                    log(f"Actual processing time: {actual_time_str}.", level=5)
                    print(f"Actual processing time: {actual_time_str}.")

                    # Compare Estimated and Actual Processing Time
                    if average_time_per_tx > 0 and len(transactions) > 0:
                        difference = actual_processing_time - estimated_processing_time
                        difference_str = str(timedelta(seconds=int(abs(difference))))
                        if difference > 0:
                            log(f"Processing took longer than estimated by {difference_str}.", level=5)
                            print(f"Processing took longer than estimated by {difference_str}.")
                        else:
                            log(f"Processing completed faster than estimated by {difference_str}.", level=5)
                            print(f"Processing completed faster than estimated by {difference_str}.")

                    # Update Average Transaction Processing Time
                    if len(transactions) > 0:
                        current_event_avg_time = actual_processing_time / len(transactions)
                        # Update the running average using weighted average (more weight to recent events)
                        alpha = 0.1  # Weight for the new average
                        average_time_per_tx = (1 - alpha) * average_time_per_tx + alpha * current_event_avg_time
                        log(f"Updated average transaction processing time to {average_time_per_tx:.6f} seconds.", level=5)
                        print(f"Updated average transaction processing time to {average_time_per_tx:.6f} seconds.")

                    # Add the event_length (time_before_event), filter_name, min_transfer, and max_transfer to the event summary
                    event_summary['event_length'] = current_time_before_event
                    event_summary['filter_name'] = filter_name
                    event_summary['min_transfer'] = current_min_transfer
                    event_summary['max_transfer'] = current_max_transfer

                    # Write the event summary to events.csv
                    write_event_summary(event_summary, current_min_transfer, current_max_transfer, filter_name)

                except KeyError as ke:
                    log(f"Missing expected field {ke} in event data: {event}. Skipping event.", level=1)
                    continue
                except ValueError as ve:
                    log(f"Value error processing event '{event.get('name', 'N/A')}': {ve}", level=1)
                    traceback.print_exc()
                    continue
                except Exception as e:
                    log(f"Error processing event {event.get('name', 'N/A')}: {e}", level=1)
                    traceback.print_exc()
                    continue

    except Exception as e:
        # Handle exceptions from the outer try block
        log(f"Fatal error during event processing: {e}", level=1)
        traceback.print_exc()
        sys.exit(1)

    # Deduplicate addresses in the database
    try:
        with sqlite3.connect(CONFIG['address_file']) as address_conn:
            deduplicate_addresses_db(address_conn, 'addresses')
    except Exception as e:
        log(f"Error during deduplication: {e}", level=1)
        traceback.print_exc()

    # Export SQL data to CSV for addresses
    try:
        with sqlite3.connect(CONFIG['address_file']) as address_conn:
            export_sql_to_csv(address_conn, 'addresses', CONFIG['addresses_csv'])
    except Exception as e:
        log(f"Error exporting SQL data to CSV: {e}", level=1)
        traceback.print_exc()

    # Final Log Statement
    log(f"Finished processing all {len(events)} events.", level=1)



# Consolidate address records by merging identical addresses
def consolidate_addresses(cursor, table_name='addresses'):
    """
    Consolidates address records by merging identical addresses and summing their values.
    This function reduces redundancy by leveraging the database's index.

    Parameters:
    - cursor (sqlite3.Cursor): SQLite cursor object.
    - table_name (str): Name of the table to consolidate ('addresses' by default).
    """
    try:
        # Query the database to aggregate by address, summing the values
        cursor.execute(f"""
            SELECT address, SUM(value) AS total_value, COUNT(*) as occurrences 
            FROM {table_name}
            GROUP BY address
            ORDER BY total_value DESC
        """)

        consolidated_data = cursor.fetchall()

        log(f"Consolidated {len(consolidated_data)} address records using SQL query.", level=1)

        return consolidated_data

    except sqlite3.Error as e:
        log(f"Error consolidating addresses in table {table_name}: {e}", level=1)
        return []


# Ensure all fields are present in CSV
def ensure_dynamic_fields_present(event_summary, fieldnames):
    """Ensures that all dynamic fields are present in the CSV fieldnames."""
    for key in event_summary:
        if isinstance(event_summary[key], dict):
            for sub_key in event_summary[key]:
                if sub_key not in fieldnames:
                    fieldnames.append(sub_key)
        else:
            if key not in fieldnames:
                fieldnames.append(key)
    return fieldnames

# Flatten event summary for CSV output
def flatten_event_summary(event_summary):
    """Flattens nested dictionaries in event_summary for CSV writing."""
    flat_summary = {
        'event_name': event_summary.get('event_name'),
        'event_time': event_summary.get('event_time'),
        'event_change': event_summary.get('event_change'),
        'total_tx_value': event_summary.get('total_tx_value'),
        'total_addresses': event_summary.get('total_addresses'),
        'min_tx_value': event_summary.get('min_tx_value'),
        'max_tx_value': event_summary.get('max_tx_value'),
        'avg_tx_value': event_summary.get('avg_tx_value'),
        'total_fees': event_summary.get('total_fees'),
        'avg_fee': event_summary.get('avg_fee'),
        'min_fee': event_summary.get('min_fee'),
        'max_fee': event_summary.get('max_fee'),
        'fee_tx_ratio': event_summary.get('fee_tx_ratio'),
        'rejected_transactions': event_summary.get('rejected_transactions'),
        'total_processed_transactions': event_summary.get('total_processed_transactions'),
        'event_processing_duration': event_summary.get('event_processing_duration'),
        'total_outputs': event_summary.get('total_outputs'),  # Ensure this is included
    }

    # Add transaction type counts
    for key, value in event_summary.get('transaction_type_counts', {}).items():
        flat_summary[key] = value

    # Add transaction type proportions
    for key, value in event_summary.get('transaction_type_proportions', {}).items():
        flat_summary[key] = float(value)  # Ensure Decimal is converted to float

    return flat_summary

def write_event_summary(event_summary, min_transfer=None, max_transfer=None, filter_name=None):
    """Writes the event summary to the events summary CSV file, including batch job-specific fields like min_transfer, max_transfer, and filter_name."""

    # Define the base fields, including the new fields: 'min_transfer', 'max_transfer', 'filter_name'
    fieldnames = [
        'event_name', 'event_time', 'event_length', 'event_change', 'min_transfer', 'max_transfer', 'filter_name',
        'total_tx_value', 'total_addresses', 'min_tx_value', 'max_tx_value', 'avg_tx_value', 'total_fees',
        'avg_fee', 'min_fee', 'max_fee', 'fee_tx_ratio', 'rejected_transactions', 
        'total_processed_transactions', 'event_processing_duration', 'total_outputs'
    ]

    # Include dynamic fields from transaction type counts and proportions
    for key in event_summary.get('transaction_type_counts', {}):
        if key not in fieldnames:
            fieldnames.append(key)
    for key in event_summary.get('transaction_type_proportions', {}):
        if key not in fieldnames:
            fieldnames.append(key)

    # Flatten the event summary
    flat_event_summary = flatten_event_summary(event_summary)

    # Add the new batch-related fields to the event summary
    flat_event_summary['min_transfer'] = min_transfer
    flat_event_summary['max_transfer'] = max_transfer
    flat_event_summary['filter_name'] = filter_name

    try:
        file_exists = os.path.isfile(CONFIG['events_summary_file'])
        with open(CONFIG['events_summary_file'], mode='a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists or csvfile.tell() == 0:
                writer.writeheader()
            # Convert Decimal values to float for CSV serialization
            for key in flat_event_summary:
                if isinstance(flat_event_summary[key], Decimal):
                    flat_event_summary[key] = float(flat_event_summary[key])
            # Write the event summary
            writer.writerow(flat_event_summary)
            log(f"Event summary written for event {event_summary['event_name']} to {CONFIG['events_summary_file']}.", level=1)
    except Exception as e:
        log(f"Failed to write event summary: {str(e)}", level=1)


# Function to write address records to CSV
def write_addresses_to_csv(address_records, filename=None):
    if filename is None:
        filename = CONFIG['addresses_csv']  # Use the configured addresses CSV file

    fieldnames = [
        'address', 'value', 'event_time', 'txid', 'tx_time', 'block_number',
        'price_change', 'event_name', 'vout_index'
    ]

    log(f"Attempting to write {len(address_records)} address records to {filename}.", level=1)

    try:
        file_exists = os.path.isfile(filename)
        with open(filename, mode='a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_exists or csvfile.tell() == 0:
                writer.writeheader()

            if address_records:
                log(f"Writing {len(address_records)} address records to {filename}.", level=1)
                for record in address_records:
                    # Convert Decimal values to float
                    for key in record:
                        if isinstance(record[key], Decimal):
                            record[key] = float(record[key])
                    writer.writerow(record)
                log(f"Written {len(address_records)} address records to {filename}.", level=1)

    except Exception as e:
        log(f"Error writing to {filename}: {str(e)}", level=1)



# write_address_records(): Create a generalized function to handle data insertion for both addresses.db
def write_address_records(cursor, address_records, table_name="addresses"):
    """
    Inserts address records into the specified SQLite table.

    Parameters:
    - cursor (sqlite3.Cursor): SQLite cursor object.
    - address_records (list of dict): List of address records to insert.
    - table_name (str): Name of the table to insert into ('addresses').
    """
    insert_query = f'''
        INSERT INTO {table_name} (
            address, value, event_time, txid, tx_time, block_number, price_change,
            address_name, source, event_name, address_description, spent, vout_index
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    for record in address_records:
        address = str(record.get('address', ''))
        value = float(record.get('value', 0.0))
        event_time = str(record.get('event_time', ''))
        txid = str(record.get('txid', ''))
        tx_time = str(record.get('tx_time', ''))
        block_number = int(record.get('block_number', 0))
        price_change = float(record.get('price_change', 0.0))
        address_name = str(record.get('address_name', ''))
        source = str(record.get('source', ''))
        event_name = str(record.get('event_name', ''))
        address_description = str(record.get('address_description', ''))
        spent = bool(record.get('spent', False))
        vout_index = int(record.get('vout_index', 0))

        try:
            cursor.execute(insert_query, (address, value, event_time, txid, tx_time, block_number,
                                          price_change, address_name, source, event_name,
                                          address_description, spent, vout_index))
        except sqlite3.IntegrityError as e:
            log(f"IntegrityError inserting address {address}: {e}", level=3)
        except sqlite3.Error as e:
            log(f"Error inserting address {address}: {e}", level=1)
            traceback.print_exc()
    log(f"Inserted {len(address_records)} records into {table_name} table", level=1)


def parse_args():
    parser = argparse.ArgumentParser(description='BTC Monitor System')
    parser.add_argument('--batch', help='Run batch jobs from batch CSV file', type=str, default=None)
    args = parser.parse_args()
    return args

def start_timer():
    return time.time()

def stop_timer(start_time):
    return time.time() - start_time

def read_batch_file(batch_file):
    """
    Reads the batch file and returns a list of batch jobs.
    Each batch job is expected to have: name, min_transfer, max_transfer, time_before_event.
    """
    batch_jobs = []
    try:
        with open(batch_file, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Append the entire row as a job dictionary
                batch_jobs.append(row)
        log(f"Loaded {len(batch_jobs)} batch jobs from {batch_file}.", level=1)
    except Exception as e:
        log(f"Error reading batch file {batch_file}: {e}", level=1)
        sys.exit(1)

    return batch_jobs


def fetch_recent_transactions(btc_rpc):
    """
    Fetch recent transactions from the Bitcoin node.
    
    This function retrieves transactions from the latest block and returns them as a list.
    
    :param btc_rpc: The connected Bitcoin RPC instance.
    :return: A list of transactions from the most recent block.
    """
    try:
        # Get the latest block hash
        latest_block_hash = btc_rpc.getbestblockhash()
        
        # Get the block details
        latest_block = btc_rpc.getblock(latest_block_hash, 2)  # '2' means get full transaction details
        
        transactions = latest_block.get('tx', [])
        log(f"Fetched {len(transactions)} transactions from the latest block {latest_block_hash}.", 1)
        
        return transactions
    
    except Exception as e:
        log(f"Error fetching recent transactions: {str(e)}", 1)
        return []

def deduplicate_addresses_db(conn, table_name):
    """
    Consolidates address records by aggregating data and removes duplicate entries from the specified SQLite table.

    Parameters:
    - conn (sqlite3.Connection): The SQLite database connection object.
    - table_name (str): The name of the table to deduplicate ('addresses' or 'coinbase_addresses').
    """
    try:
        cursor = conn.cursor()

        # Create a temporary table with aggregated data
        temp_table = f"{table_name}_temp"

        # Drop the temporary table if it exists
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")

        # Create the temporary table with aggregated data
        cursor.execute(f"""
            CREATE TABLE {temp_table} AS
            SELECT
                address,
                SUM(value) AS total_value,
                MIN(event_time) AS first_event_time,
                MAX(event_time) AS last_event_time,
                COUNT(*) AS transaction_count
            FROM {table_name}
            GROUP BY address
        """)

        # Delete all records from the original table
        cursor.execute(f"DELETE FROM {table_name}")

        # Insert the aggregated data back into the original table
        cursor.execute(f"""
            INSERT INTO {table_name} (address, value, event_time, txid, tx_time)
            SELECT
                address,
                total_value,
                first_event_time,
                NULL,  -- txid is set to NULL since multiple txids are combined
                NULL   -- tx_time is set to NULL
            FROM {temp_table}
        """)

        # Drop the temporary table
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")

        conn.commit()
        log(f"Consolidated and deduplicated records in {table_name}.", level=1)

    except sqlite3.Error as e:
        log(f"Error consolidating and deduplicating table {table_name}: {e}", level=1)


# main function
def main():
    print("BTC Monitor Script Starting...")
    try:
        # Step 1: Declare global variables
        global transaction_types, btc_rpc

        # Step 2: Start the program and initialize logging
        start_time = datetime.now()
        main_initialization()
        
        # Step 3: Create or load btc_monitor.conf file
        create_or_load_config()  # Add this line

        # Step 4: Print system settings for review
        print_settings()

        # Step 5: Parse command-line arguments (if any)
        args = parse_args()

        # Step 6: Connect to the Bitcoin RPC node
        log("Connecting to Bitcoin RPC...", level=1)
        btc_rpc = connect_rpc()
        if btc_rpc is None:
            log("Failed to connect to Bitcoin RPC. Exiting.", level=1)
            sys.exit(1)

        # Step 7: Enable debugging if the debugger flag is set in CONFIG
        if CONFIG.get('debugger', False):
            pdb.set_trace()  # Enter the debugger if enabled

        # Step 8: Load Transaction Types
        transaction_types = load_transaction_types(CONFIG['transaction_types_file'])
        log("Transaction types loaded successfully.", level=1)

        # Step 9: Get Latest Block Height
        latest_block_height = btc_rpc.getblockcount()
        log(f"Latest Block Height = {latest_block_height}", level=1)

        # Step 10: Display the startup screen with the current configuration
        btc_monitor_startup(
            version=CONFIG['version'],
            rpc_url=f"http://{CONFIG['rpc_user']}:{CONFIG['rpc_password']}@{CONFIG['rpc_host']}:{CONFIG['rpc_port']}",
            min_transfer=CONFIG['min_btc_transfer'],
            max_transfer=CONFIG['max_btc_transfer'],
            time_before_event=CONFIG['time_before_event'],
            debug_level=CONFIG['debug'],
            log_file=CONFIG['log_file'],
            batch_on="On" if args.batch else "Off"
        )

        input("Press Any Key to continue...")

        # Step 11: Check for batch mode
        if args.batch:
            log(f"Batch mode enabled. Running batch jobs from {args.batch}.", level=1)
            batch_jobs = read_batch_file(args.batch)  # Read batch job file
            run_batch_jobs(batch_jobs, transaction_types, btc_rpc)  # Pass transaction_types and btc_rpc
        else:
            # Step 12: Process events from changeevents.csv file
            log("Processing events from changeevents.csv file.", level=1)
            process_events(transaction_types, btc_rpc, min_transfer=CONFIG['min_btc_transfer'],
               max_transfer=CONFIG['max_btc_transfer'], time_before_event=CONFIG['time_before_event'])

    
        # Step 13: Deduplicate records in the database (if enabled in CONFIG)
        if CONFIG.get('deduplicate_addresses_db', True):
            log("Deduplicating addresses and coinbase addresses...", level=1)
            conn = sqlite3.connect(CONFIG['address_file'])
            deduplicate_addresses_db(conn, 'addresses')
            deduplicate_addresses_db(conn, 'coinbase_addresses')
            conn.close()

        # Step 14: Export deduplicated SQL data to CSV (if enabled)
        if CONFIG.get('csv_report_enabled', True):
            log("Exporting deduplicated SQL data to CSV...", level=1)
            conn = sqlite3.connect(CONFIG['address_file'])
            export_sql_to_csv(conn, 'addresses', CONFIG['addresses_csv'])
            export_sql_to_csv(conn, 'coinbase_addresses', CONFIG['coinbase_addresses_csv'])
            conn.close()

        # Step 15: Generate final reports for addresses and coinbase addresses
        log("Generating final reports...", level=1)
        conn = sqlite3.connect(CONFIG['address_file'])
        generate_reports(conn, enable_csv=True, table_name="addresses")
        generate_reports(conn, enable_csv=True, table_name="coinbase_addresses")
        conn.close()
        log("Final reports generated.", level=1)

        # Step 16: Log the total runtime of the script
        total_runtime = datetime.now() - start_time
        log(f"Total runtime (including startup/shutdown): {total_runtime}", level=1)

    except Exception as e:
        # Handle any errors that occur during execution
        log(f"An error occurred during execution: {e}", level=1)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
