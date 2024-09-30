import logging
import os
from monitor_addresses import address_cache

"""
    Cleans up resources used by monitor_addresses and potentially other modules.
    Should be called by the main program when needed.
"""
def monitor_addresses_cleanup(config, address_cache):
    """
    Cleans up resources used by monitor_addresses and potentially other modules.
    Should be called by the main program when needed.
    """
    logging.info("Running cleanup for monitor_addresses...")

    # Clear in-memory cache after task completion
    address_cache.clear()
    logging.info("Address cache cleared during cleanup.")

    data_directory = config['General'].get('data_directory', './agent-002-data/')
    temp_cache_file = os.path.join(data_directory, 'address_cache.tmp')

    try:
        if os.path.exists(temp_cache_file):
            os.remove(temp_cache_file)
            logging.info(f"Temporary cache file {temp_cache_file} removed.")
        else:
            logging.info(f"No temporary cache file found at {temp_cache_file}.")
    except Exception as e:
        logging.error(f"Failed to remove temporary cache file {temp_cache_file}: {e}")

    logging.info("monitor_addresses cleanup completed successfully.")
