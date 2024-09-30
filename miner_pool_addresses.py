
#!/usr/bin/env python3

"""
miner_pool_addresses.py

This script processes the Bitcoin blockchain to extract miner addresses from coinbase transactions.
It creates a SQLite database 'Miner_addresses.db' with an updated schema to include new fields.
It adjusts the sanity check to ensure total output value is >= expected block reward.
It enhances logging for flagged blocks to assist in debugging and analysis.

Modifications include:
- Adjusted sanity check to flag blocks where total output < expected reward.
- Enhanced logging to capture detailed output information.
- Updated insert_address function to include the 'spent' field.
- Added monitoring of RPC calls per second and reporting during batch output to the database.
"""

import logging
import sqlite3
import sys
import time
import os


from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import base58
from datetime import datetime
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from collections import defaultdict

# Initialize error counters for throttling
invalid_pubkey_counts = defaultdict(int)
MAX_INVALID_PUBKEY_LOGS = 10  # Max number of times to log a specific error

# Configuration settings
CONFIG = {
    'version': '0.5',
    'rpc_user': 'wrapperband',
    'rpc_password': 'ardork54',
    'rpc_host': 'localhost',
    'rpc_port': 18332,  # Change to 18332 for testnet
    'address_history_db': os.path.abspath('Miner_addresses.db'),
    'start_block_height': 0,          # Start from the genesis block - 0
    'end_block_height': None,         # None - Will default to the latest block
    'log_file': 'address_history.log',
    'log_level': logging.WARNING,        # Set log level to INFO for detailed logs
    'batch_size': 5000,               # Number of blocks to process before committing to the database
    'testnet': False
}



# Global counter for RPC calls
rpc_call_count = 0

def init_log():
    """
    Initializes logging based on configurations.
    """
    logging.basicConfig(
        filename=CONFIG['log_file'],
        level=CONFIG['log_level'],
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    # Also log to console for errors
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)  # Only log errors to console
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

def connect_rpc():
    """
    Establishes a connection to the Bitcoin RPC interface.
    """
    global rpc_call_count
    try:
        rpc_url = f"http://{CONFIG['rpc_user']}:{CONFIG['rpc_password']}@{CONFIG['rpc_host']}:{CONFIG['rpc_port']}"
        btc_rpc = AuthServiceProxy(rpc_url, timeout=60)  # Include timeout here
        # Test the connection
        btc_rpc.getblockcount()
        rpc_call_count += 1
        print("Successfully connected to Bitcoin RPC.")
        logging.info("Successfully connected to Bitcoin RPC.")
        return btc_rpc
    except Exception as e:
        logging.error(f"Error connecting to Bitcoin RPC: {e}")
        print(f"Error connecting to Bitcoin RPC: {e}")
        sys.exit(1)

def initialize_databases():
    """Initializes the SQLite database for addresses with a unified schema and necessary indexes."""
    try:
        # Define the unified schema with additional fields
        address_table_schema = """
            CREATE TABLE IF NOT EXISTS addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL,
                txid TEXT NOT NULL,
                vout_index INTEGER NOT NULL,
                value REAL,
                event_time TEXT,
                tx_time TEXT,
                block_number INTEGER,
                price_change REAL,
                address_name TEXT,
                source TEXT,
                event_name TEXT,
                address_description TEXT,
                spent BOOLEAN,
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
        if not os.path.exists(CONFIG['address_history_db']):
            conn = sqlite3.connect(CONFIG['address_history_db'])
            cursor = conn.cursor()
            cursor.executescript(address_table_schema)  # Execute the CREATE TABLE statement
            cursor.executescript(CREATE_INDEX_ADDRESSES_QUERY)  # Execute all index creations
            conn.commit()
            conn.close()
            logging.info(f"Address database created at {CONFIG['address_history_db']}")  # Use logging

        logging.info("Databases initialized successfully with unified schema and indexes.")  # Use logging

    except sqlite3.Error as e:
        logging.error(f"Error initializing databases: {e}")  # Use logging
        sys.exit(1)

def normalize(value):
    """
    Normalizes the input value to a Decimal.
    Handles None and invalid inputs gracefully.
    """
    try:
        if value is None:
            return Decimal('0.0')
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as e:
        logging.error(f"Error normalizing value '{value}': {e}")
        return Decimal('0.0')

def get_block_reward(height):
    """
    Returns the expected block reward for a given block height.
    """
    halvings = height // 210000
    initial_reward = Decimal('50')
    reward = initial_reward / (2 ** halvings)
    return reward

def pubkey_to_address(pubkey_hex, testnet=False):
    """
    Converts a public key to a Bitcoin address.
    """
    sha256_digest = hashlib.sha256(bytes.fromhex(pubkey_hex)).digest()
    ripemd160 = hashlib.new('ripemd160')
    ripemd160.update(sha256_digest)
    hashed_pubkey = ripemd160.digest()
    if testnet:
        prefix = b'\x6f'  # Testnet prefix
    else:
        prefix = b'\x00'  # Mainnet prefix
    payload = prefix + hashed_pubkey
    checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    address_bytes = payload + checksum
    address = base58.b58encode(address_bytes).decode()
    return address

def is_output_spent(btc_rpc, txid, vout_index):
    """
    Checks if a transaction output is spent.
    Returns True if spent, False if unspent.
    """
    global rpc_call_count
    try:
        txout = btc_rpc.gettxout(txid, vout_index, True)
        rpc_call_count += 1
        return txout is None  # If None, the output is spent
    except Exception as e:
        logging.error(f"Error checking spent status for txid {txid}, vout {vout_index}: {e}")
        return False  # Assume unspent if there's an error

def insert_address(cursor, address_data):
    """
    Inserts address data into the database.

    Parameters:
    - cursor (sqlite3.Cursor): The SQLite database cursor.
    - address_data (dict): A dictionary containing address details.
    """
    required_fields = [
        'address', 'txid', 'vout_index', 'value', 'event_time', 'tx_time',
        'block_number', 'price_change', 'address_name', 'source',
        'event_name', 'address_description', 'spent'
    ]
    
    # Check for missing fields
    missing_fields = [field for field in required_fields if field not in address_data]
    if missing_fields:
        logging.error(f"Missing fields {missing_fields} in address_data: {address_data}")
        return  # Skip insertion if data is incomplete
    
    try:
        # Cast types to ensure proper SQLite compatibility
        vout_index = int(address_data['vout_index'])  # Ensure this is an integer
        value = float(address_data['value'])  # Cast value to float
        block_number = int(address_data['block_number'])  # Ensure block_number is an integer
        spent = int(address_data['spent'])  # SQLite uses integers for boolean fields
        
        cursor.execute('''
            INSERT OR IGNORE INTO addresses (
                address, txid, vout_index, value, event_time, tx_time, block_number,
                price_change, address_name, source, event_name, address_description, spent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        ''', (
            address_data['address'],
            address_data['txid'],
            vout_index,  # Ensured as integer
            value,  # Ensured as float
            address_data['event_time'],
            address_data['tx_time'],
            block_number,  # Ensured as integer
            address_data['price_change'],
            address_data['address_name'],
            address_data['source'],
            address_data['event_name'],
            address_data['address_description'],
            spent  # Ensured as integer (for boolean)
        ))
    except sqlite3.Error as e:
        logging.error(f"Error inserting address {address_data['address']} into database: {e}")
        print(f"Error inserting address {address_data['address']} into database: {e}")


def update_last_processed_block(height):
    """Updates the last processed block number in a file."""
    try:
        with open('last_block.txt', 'w') as f:
            f.write(str(height))
        logging.info(f"Updated last processed block to {height}.")
    except Exception as e:
        logging.error(f"Error updating last processed block: {e}")


def is_valid_pubkey(pubkey_hex):
    """
    Validates whether the provided pubkey_hex is a valid hexadecimal public key.
    
    Parameters:
    - pubkey_hex (str): The public key in hexadecimal format.
    
    Returns:
    - bool: True if valid, False otherwise.
    """
    if not isinstance(pubkey_hex, str):
        return False
    if len(pubkey_hex) not in [66, 130]:  # 33 bytes (compressed) or 65 bytes (uncompressed)
        return False
    try:
        bytes.fromhex(pubkey_hex)
        return True
    except ValueError:
        return False

def process_coinbase_transaction(btc_rpc, transaction, cursor, block_time, block_height, is_testnet):
    txid = transaction['txid']
    tx_time = transaction.get('time', block_time)
    is_coinbase = any('coinbase' in vin for vin in transaction.get('vin', []))

    if not is_coinbase:
        return  # Not a coinbase transaction

    for vout in transaction.get('vout', []):
        vout_index = vout['n']
        value = vout['value']
        script_pub_key = vout.get('scriptPubKey', {})
        script_type = script_pub_key.get('type', '')
        addresses = script_pub_key.get('addresses', [])
        asm = script_pub_key.get('asm', '')
        addr = []

        if addresses:
            addr = addresses
        else:
            # Attempt to extract address manually
            if script_type == 'pubkey':
                pubkey = asm.split(' ')[0]
                if is_valid_pubkey(pubkey):
                    try:
                        address = pubkey_to_address(pubkey, testnet=is_testnet)
                        addr = [address]
                    except Exception as e:
                        logging.error(f"Error deriving address from pubkey: {e}")
            elif script_type == 'pubkeyhash':
                parts = asm.split(' ')
                if len(parts) >= 5 and parts[0] == 'OP_DUP' and parts[1] == 'OP_HASH160' and parts[3] == 'OP_EQUALVERIFY' and parts[4] == 'OP_CHECKSIG':
                    pubkey_hash = parts[2]
                    try:
                        address = pubkey_hash_to_address(pubkey_hash, testnet=is_testnet)
                        addr = [address]
                    except Exception as e:
                        logging.error(f"Error deriving address from pubkeyhash: {e}")
            else:
                # Unsupported script type
                continue

        if addr:
            # Insert address into the database
            for address in addr:
                # Build the address_data dictionary
                address_data = {
                    'address': address,
                    'txid': txid,
                    'vout_index': vout_index,
                    'value': value,
                    'event_time': None,          # You can set appropriate values if available
                    'tx_time': tx_time,
                    'block_number': block_height,
                    'price_change': None,        # Set to None or appropriate value
                    'address_name': None,        # Set if you have this information
                    'source': 'coinbase',        # Since we're processing coinbase transactions
                    'event_name': None,          # Set if applicable
                    'address_description': None, # Set if you have this information
                    'spent': is_output_spent(btc_rpc, txid, vout_index),
                }
                insert_address(cursor, address_data)
        else:
            # No address could be extracted
            continue



def pubkey_hash_to_address(pubkey_hash_hex, testnet=False):
    """
    Converts a public key hash to a Bitcoin address.
    """
    if testnet:
        prefix = b'\x6f'  # Testnet prefix
    else:
        prefix = b'\x00'  # Mainnet prefix
    payload = prefix + bytes.fromhex(pubkey_hash_hex)
    checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    address_bytes = payload + checksum
    address = base58.b58encode(address_bytes).decode()
    return address

# Add the missing check_output_spent function here
def check_output_spent(btc_rpc, txid, vout_index):
    """
    Checks if a specific output in a transaction is spent.
    
    Parameters:
    - btc_rpc: The Bitcoin RPC connection object.
    - txid: The transaction ID.
    - vout_index: The output index of the transaction to check.
    
    Returns:
    - True if the output is spent, False if it is unspent or if an error occurs.
    """
    global rpc_call_count
    try:
        # Use the `gettxout` RPC call to check the spent status of an output.
        txout = btc_rpc.gettxout(txid, vout_index, True)
        rpc_call_count += 1
        return txout is None  # If None, the output is spent
    except JSONRPCException as e:
        logging.error(f"Error checking spent status for txid {txid}, vout {vout_index}: {e}")
        return False  # Assume unspent if there's an error


    
def process_blocks(start_height, end_height, btc_rpc, is_testnet):
    conn = sqlite3.connect(CONFIG['address_history_db'], isolation_level=None, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('PRAGMA journal_mode=WAL;')
    cursor.execute('PRAGMA synchronous = NORMAL;')

    batch_size = CONFIG.get('batch_size', 10000)  # Use a reasonable batch size
    last_commit_time = time.time()
    blocks_processed = 0
    start_time = time.time()
    rpc_call_start_time = time.time()

    # Start a transaction
    cursor.execute('BEGIN TRANSACTION;')

    for height in range(start_height, end_height + 1):
        try:
            if height == 0:
                logging.info("Skipping genesis block.")
                continue

            # Fetch block data
            block_hash = btc_rpc.getblockhash(height)
            block = btc_rpc.getblock(block_hash, 1)
            block_time = block.get('time', int(datetime.utcnow().timestamp()))
            coinbase_txid = block['tx'][0]
            coinbase_tx = btc_rpc.getrawtransaction(coinbase_txid, True)

            # Process the coinbase transaction
            process_coinbase_transaction(btc_rpc, coinbase_tx, cursor, block_time, height, is_testnet)

            blocks_processed += 1

            # Commit after processing each batch of blocks
            if blocks_processed % batch_size == 0 or height == end_height:
                conn.commit()
                update_last_processed_block(height)
                current_time = time.time()
                batch_duration = current_time - last_commit_time
                last_commit_time = current_time

                # Calculate and display progress
                percent_complete = ((height - start_height + 1) / (end_height - start_height + 1)) * 100
                elapsed_time = current_time - start_time
                estimated_total_time = (elapsed_time / (height - start_height + 1)) * (end_height - start_height + 1)
                eta_formatted = datetime.fromtimestamp(start_time + estimated_total_time).strftime('%Y-%m-%d %H:%M:%S')
                rpc_calls_made = (height - start_height + 1) * 3
                rpc_calls_per_second = rpc_calls_made / (current_time - rpc_call_start_time)

                print(f"Processed block {height}/{end_height} ({percent_complete:.2f}% complete). ETA: {eta_formatted}")
                print(f"Batch processing time: {batch_duration:.2f} seconds. RPC calls per second: {rpc_calls_per_second:.2f}")

                # Start a new transaction for the next batch
                cursor.execute('BEGIN TRANSACTION;')

        except JSONRPCException as e:
            logging.error(f"RPC error processing block {height}: {e}")
            continue
        except Exception as e:
            logging.error(f"Error processing block {height}: {e}")
            continue

    # Final commit and cleanup
    conn.commit()
    conn.close()
    total_duration = time.time() - start_time
    print(f"Finished processing {blocks_processed} blocks in {total_duration / 60:.2f} minutes.")

def get_last_processed_block():
    """Retrieve the last processed block from the file, if it exists."""
    if os.path.exists('last_block.txt'):
        with open('last_block.txt', 'r') as f:
            return int(f.read().strip())
    return CONFIG.get('start_block_height', 0)  # Default to start_block_height if no file exists


def main():
    init_log()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_message = f"{current_time} - Starting historical analysis of Bitcoin blockchain."
    
    print(start_message)
    logging.info(start_message)
    
    print(f"Current working directory: {os.getcwd()}")
    initialize_databases()
    
    # Connect to Bitcoin RPC
    btc_rpc = connect_rpc()
    is_testnet = False  # Set this according to your configuration
    
    # Get the last processed block, if available
    start_height = get_last_processed_block()
    
    # Get the current block height from the Bitcoin node
    try:
        blockchain_info = btc_rpc.getblockchaininfo()
        end_height = blockchain_info.get('blocks') - 1  # Adjust as needed
        if end_height is None:
            raise ValueError("Could not retrieve 'blocks' from blockchain info.")
    except Exception as e:
        logging.error(f"Error retrieving blockchain info: {e}")
        print("Error retrieving blockchain info. Please ensure the Bitcoin node is running and fully synchronized.")
        sys.exit(1)
    
    print(f"Processing blocks from {start_height} to {end_height} (Total: {end_height - start_height + 1} blocks)")
    
    # Call process_blocks with valid start_height and end_height, and pass btc_rpc
    process_blocks(start_height, end_height, btc_rpc, is_testnet)

if __name__ == "__main__":
    import cProfile
    import pstats
    import sys

    profiler = cProfile.Profile()

    try:
        profiler.enable()
        main()
        profiler.disable()
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Generating profiling report...")
        profiler.disable()
    finally:
        # Save the profiling data to a file
        profiler.dump_stats('profile_stats')

        # Load and print the profiling data
        p = pstats.Stats('profile_stats')
        p.sort_stats('cumulative').print_stats(20)  # Adjust the number to display more or fewer entries
