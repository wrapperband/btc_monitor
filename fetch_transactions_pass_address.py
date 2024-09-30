import sys
from bitcoinrpc.authproxy import AuthServiceProxy

# Configuration (use your RPC connection details here)
RPC_USER = "wrapperband"
RPC_PASSWORD = "ardork54"
RPC_HOST = "localhost"
RPC_PORT = "18332"

# Function to connect to Bitcoin RPC
def connect_rpc():
    """Connect to Bitcoin RPC."""
    try:
        rpc_url = f"http://{RPC_USER}:{RPC_PASSWORD}@{RPC_HOST}:{RPC_PORT}"
        return AuthServiceProxy(rpc_url)
    except Exception as e:
        print(f"Error connecting to Bitcoin RPC: {e}")
        sys.exit(1)

# Function to fetch and log transaction details
def fetch_transaction_details(btc_rpc, txid):
    """Fetch and log detailed information of a specific transaction."""
    try:
        print(f"Fetching details for transaction {txid}...")
        tx = btc_rpc.getrawtransaction(txid, True)  # Fetch transaction details in decoded format
        print(f"Transaction {txid} details:")
        print(tx)
        
        # Log if the transaction is missing inputs or outputs
        if 'vin' not in tx or 'vout' not in tx:
            print(f"Transaction {txid} is missing inputs or outputs.")
        else:
            print(f"Transaction {txid} has {len(tx['vin'])} inputs and {len(tx['vout'])} outputs.")
    except Exception as e:
        print(f"Error fetching transaction {txid}: {e}")

# Main execution
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python fetch_transaction.py <txid>")
        sys.exit(1)

    txid = sys.argv[1]  # Transaction ID passed as an argument
    btc_rpc = connect_rpc()  # Connect to Bitcoin RPC

    fetch_transaction_details(btc_rpc, txid)  # Fetch and log transaction details
