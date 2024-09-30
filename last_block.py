from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

# RPC credentials
RPC_USER = 'wrapperband'
RPC_PASSWORD = 'ardork54'
RPC_PORT = 18332  # Default port for Bitcoin Core RPC

# Create a connection to the Bitcoin Core RPC server
rpc_url = f'http://{RPC_USER}:{RPC_PASSWORD}@localhost:{RPC_PORT}'

def fetch_latest_block():
    try:
        # Create the RPC connection
        rpc_connection = AuthServiceProxy(rpc_url)
        
        # Get the latest block hash
        latest_block_hash = rpc_connection.getbestblockhash()
        print(f"Latest Block Hash: {latest_block_hash}")
        
        # Get the latest block details
        block = rpc_connection.getblock(latest_block_hash, True)  # True for detailed info
        print(f"Block Height: {block['height']}")
        print(f"Block Time: {block['time']}")
        print(f"Number of Transactions: {len(block['tx'])}")

        # Print the entire block to understand its structure
        print("Block Details:", block)
        
        # Iterate through each transaction ID in the block
        for txid in block['tx']:
            # Fetch detailed transaction info
            try:
                tx = rpc_connection.getrawtransaction(txid, True)  # True for detailed info
                print("\nTransaction ID:", tx.get('txid', 'N/A'))
                print("  Version:", tx.get('version', 'N/A'))
                print("  Lock Time:", tx.get('locktime', 'N/A'))
                print("  Inputs:")
                for vin in tx.get('vin', []):
                    print(f"    - TxID: {vin.get('txid', 'N/A')} | Vout: {vin.get('vout', 'N/A')} | ScriptSig: {vin.get('scriptSig', {}).get('asm', 'N/A')}")
                print("  Outputs:")
                for vout in tx.get('vout', []):
                    print(f"    - Value: {vout.get('value', 'N/A')} BTC | ScriptPubKey: {vout.get('scriptPubKey', {}).get('asm', 'N/A')} | Address: {vout.get('scriptPubKey', {}).get('addresses', 'N/A')}")
            except JSONRPCException as e:
                print(f"An error occurred while fetching transaction details: {e}")

    except JSONRPCException as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    fetch_latest_block()
