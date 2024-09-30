import logging
from bitcoinrpc.authproxy import AuthServiceProxy

# In-memory cache for addresses
address_cache = {}

def load_addresses_to_cache(address_file):
    """
    Loads addresses from the address file into an in-memory cache.
    """
    global address_cache
    address_cache.clear()  # Clear any existing data in the cache
    try:
        with open(address_file, 'r') as f:
            for line in f:
                address = line.strip()
                if address:
                    address_cache[address] = True
        logging.info(f"Loaded {len(address_cache)} addresses into cache from {address_file}")
    except FileNotFoundError:
        logging.error(f"Address file {address_file} not found.")
        raise
    except Exception as e:
        logging.error(f"Error loading addresses into cache: {e}")
        raise

def monitor_addresses(config, agent_data_directory):
    """
    Monitors the blockchain for transactions involving addresses listed in the address file.
    """
    try:
        address_file = config['General']['address_file']
        rpc_user = config['General']['rpc_user']
        rpc_password = config['General']['rpc_password']
        rpc_host = config['General']['rpc_host']
        rpc_port = config['General']['rpc_port']

        rpc_url = f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}"
        logging.debug(f"Connecting to Bitcoin RPC at {rpc_url}")
        btc_rpc = AuthServiceProxy(rpc_url, timeout=120)

        logging.info(f"Monitoring addresses from {address_file}")
        
        # Load addresses into cache before monitoring
        load_addresses_to_cache(address_file)

        # Process each block (example: from latest block back to a certain point)
        latest_block = btc_rpc.getblockcount()
        block_to_start = max(0, latest_block - 100)  # example: checking the last 100 blocks

        for block_number in range(latest_block, block_to_start - 1, -1):
            block_hash = btc_rpc.getblockhash(block_number)
            block = btc_rpc.getblock(block_hash, 2)  # Fetch block with transaction details
            logging.debug(f"Checking block {block_number} for monitored addresses...")

            matched_outputs = []
            for tx in block['tx']:
                for output in tx['vout']:
                    address = output['scriptPubKey'].get('address', None)
                    if address and address in address_cache:
                        matched_outputs.append(f"{address}: {output['value']} BTC")

            if matched_outputs:
                logging.info(f"Block {block_number} contains {len(matched_outputs)} matching transactions.")

        logging.info("Address monitoring completed successfully.")
        return True
    except FileNotFoundError as e:
        logging.error(f"Address file not found: {e}")
        return False
    except Exception as e:
        logging.error(f"Error during address monitoring: {e}")
        return False
