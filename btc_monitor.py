import csv
import time
from datetime import datetime
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

# Configuration Variables
RPC_USER = 'RPC_USER'
RPC_PASSWORD = 'RPC_PASSWORD'
RPC_PORT = 18332
RPC_URL = f'http://{RPC_USER}:{RPC_PASSWORD}@localhost:{RPC_PORT}'
MIN_BTC_TRANSFER = 99  # Minimum BTC transfer to consider (e.g., 0.1 BTC)
TIME_BEFORE_EVENT = 10000  # Time to examine before the event in seconds
CHANGE_EVENTS_FILE = 'changeevents.csv'
ADDRESS_FILE = 'addresses.csv'
EVENTS_SUMMARY_FILE = 'events.csv'  # CSV for summary of events
DEBUG_LEVEL = 3  # Set debug level (1 = basic info, 2 = detailed info, 3 = all logs)

# Custom logging function
def log(message, level=1):
    if level <= DEBUG_LEVEL:
        print(message)

# RPC Connection
def connect_rpc():
    try:
        btc_rpc = AuthServiceProxy(RPC_URL, timeout=120)
        btc_rpc.getblockchaininfo()  # Test the connection
        log("Successfully connected to Bitcoin RPC.", 1)
        return btc_rpc
    except JSONRPCException as e:
        log(f"Error connecting to Bitcoin RPC: {e}", 1)
        exit(1)

# Function to find the first block within the time range
def find_block_by_time(btc_rpc, target_time):
    lower_bound = 0
    upper_bound = btc_rpc.getblockcount()

    while lower_bound <= upper_bound:
        mid = (lower_bound + upper_bound) // 2
        block_hash = btc_rpc.getblockhash(mid)
        block = btc_rpc.getblock(block_hash)
        block_time = block['time']

        if block_time < target_time:
            lower_bound = mid + 1
        elif block_time > target_time:
            upper_bound = mid - 1
        else:
            return mid  # Exact match

    return upper_bound if upper_bound >= 0 else 0  # Closest match

# Function to get transactions within the time range
def get_transactions_in_time_range(btc_rpc, start_time, end_time, min_btc_transfer):
    transactions = []
    start_block = find_block_by_time(btc_rpc, start_time)
    end_block = find_block_by_time(btc_rpc, end_time)
    max_tx_value = 0

    log(f"Identified block closest to event time: {end_block}", 1)

    # Go backwards from the end block to the start block
    block_height = end_block

    while block_height >= start_block:
        try:
            block_hash = btc_rpc.getblockhash(block_height)
            block = btc_rpc.getblock(block_hash, 2)
            block_time = block['time']

            block_datetime = datetime.utcfromtimestamp(block_time).strftime('%Y-%m-%d %H:%M:%S')
            log(f"Processing block {block_height} at {block_datetime} with {len(block['tx'])} transactions.", 2)

            for tx in block['tx']:
                if 'vout' not in tx:
                    log(f"Transaction {tx['txid']} does not contain 'vout'. Skipping this transaction.", 3)
                    continue

                tx_value = sum(output['value'] for output in tx['vout'])
                if tx_value >= min_btc_transfer:
                    transactions.append({
                        'txid': tx['txid'],
                        'value': tx_value,
                        'time': block_time,
                        'inputs': tx['vin'],
                        'outputs': tx['vout']
                    })
                    log(f"Transaction {tx['txid']} processed with value {tx_value} BTC.", 2)
                    max_tx_value = max(max_tx_value, tx_value)

            # Move to the previous block
            block_height -= 1

        except JSONRPCException as e:
            log(f"Error retrieving block {block_height}: {e}", 1)
            break

    log(f"Stopping block: {block_height}", 1)
    log(f"Max transaction value found: {max_tx_value} BTC.", 1)
    log(f"Total transactions found in time range: {len(transactions)}", 1)
    return transactions

# Function to extract and record addresses from transactions
def extract_and_record_sent_addresses(transactions, event_change, event_time, min_btc_transfer):
    address_records = []
    vin_error_count = 0

    for tx in transactions:
        log(f"Processing transaction {tx['txid']} with structure: {tx}", 2)

        tx_time = datetime.utcfromtimestamp(tx['time']).strftime('%Y-%m-%d %H:%M:%S')
        tx_value = sum(output['value'] for output in tx['outputs'])  # Calculate total transaction value from outputs

        for output in tx['outputs']:
            script_pub_key = output.get('scriptPubKey', {})
            address = script_pub_key.get('address')  # Try to directly access the address field
            value = output.get('value', 0)

            if address:
                log(f"Found address: {address} with value: {value} BTC in transaction {tx['txid']}", 2)
                if value >= min_btc_transfer:
                    percentage_of_total = (value / tx_value) * 100 if tx_value > 0 else 0

                    record = {
                        'address': address,
                        'value': value,
                        'percentage_of_total': percentage_of_total,
                        'txid': tx['txid'],
                        'tx_time': tx_time,
                        'event_time': datetime.utcfromtimestamp(event_time).strftime('%Y-%m-%d %H:%M:%S'),
                        'price_change': event_change
                    }
                    address_records.append(record)
                    log(f"Recorded sent address: {record}", 2)
            else:
                log(f"Transaction {tx['txid']} output {output} does not contain a recognized address. Skipping this output.", 2)

    log(f"Total address records: {len(address_records)}", 1)
    return address_records, vin_error_count

# Function to write address records to CSV
def write_addresses_to_csv(address_records, filename="addresses.csv"):
    fieldnames = ['address', 'value', 'percentage_of_total', 'txid', 'tx_time', 'event_time', 'price_change']

    with open(filename, mode='a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if csvfile.tell() == 0:
            writer.writeheader()

        if address_records:
            for record in address_records:
                writer.writerow(record)
        else:
            log("No address records to write.", 1)

    log(f"Total addresses recorded for this event: {len(address_records)}", 1)

# Function to write event summary to CSV
def write_event_summary(event_time_str, total_tx_value, total_addresses, event_change, min_tx_value, max_tx_value, avg_tx_value):
    fieldnames = ['event_time', 'total_tx_value', 'total_addresses', 'price_change', 'min_tx_value', 'max_tx_value', 'avg_tx_value']

    # Check if the file exists and has the correct headers
    file_exists = False
    try:
        with open(EVENTS_SUMMARY_FILE, mode='r') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader, None)
            if headers == fieldnames:
                file_exists = True
    except FileNotFoundError:
        pass

    # Write to the CSV file, adding the header if it doesn't exist
    with open(EVENTS_SUMMARY_FILE, mode='a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        writer.writerow({
            'event_time': event_time_str,
            'total_tx_value': total_tx_value,
            'total_addresses': total_addresses,
            'price_change': event_change,
            'min_tx_value': min_tx_value,
            'max_tx_value': max_tx_value,
            'avg_tx_value': avg_tx_value
        })

    log(f"Event summary written to {EVENTS_SUMMARY_FILE}: Time: {event_time_str}, "
        f"Total Tx Value: {total_tx_value} BTC, Total Addresses: {total_addresses}, "
        f"Price Change: {event_change}%, Min Tx Value: {min_tx_value}, Max Tx Value: {max_tx_value}, Avg Tx Value: {avg_tx_value}", 1)

def process_events():
    btc_rpc = connect_rpc()

    with open(CHANGE_EVENTS_FILE, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            event_change = float(row['change_percent'])
            event_time_str = row['date_time']
            event_time = int(time.mktime(time.strptime(event_time_str, '%Y/%m/%d %H:%M:%S')))
            start_time = event_time - TIME_BEFORE_EVENT

            log(f"\nProcessing event: {event_time_str} with price change of {event_change}%", 1)
            log(f"Searching blocks from {datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')} to {event_time_str}", 2)

            # This retrieves transactions based on the time range
            transactions = get_transactions_in_time_range(btc_rpc, start_time, event_time, MIN_BTC_TRANSFER)

            if transactions:
                total_tx_value = sum(tx['value'] for tx in transactions)
                total_addresses = len(set(
                    output.get('scriptPubKey', {}).get('address') 
                    for tx in transactions 
                    for output in tx['outputs'] 
                    if output.get('scriptPubKey', {}).get('address')
                ))

                # Calculate min, max, and average transaction values
                min_tx_value = min(tx['value'] for tx in transactions)
                max_tx_value = max(tx['value'] for tx in transactions)
                avg_tx_value = total_tx_value / len(transactions) if transactions else 0

                # Write the event summary to events.csv
                write_event_summary(event_time_str, total_tx_value, total_addresses, event_change, min_tx_value, max_tx_value, avg_tx_value)

                # Extract and record addresses from transactions, then write to addresses.csv
                address_records, vin_error_count = extract_and_record_sent_addresses(transactions, event_change, event_time, MIN_BTC_TRANSFER)
                write_addresses_to_csv(address_records, filename=ADDRESS_FILE)

            else:
                log("No transactions found in the specified time range for this event.", 1)

# Main Execution
if __name__ == "__main__":
    process_events()
