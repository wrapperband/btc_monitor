# Program Name: disk_usage_tool_btc_index_size.py

## Purpose:
To calculate, monitor, and report the size of the Bitcoin block index during a re-indexing operation.
The tool monitors directories, logs relevant disk usage statistics, and calculates the progress of indexing based on disk usage.
It uses GB for calculations after retrieving directory sizes in MB.

## Setup Instructions:

### Bitcoin Node Configuration (bitcoin.conf):
Ensure your Bitcoin node is configured properly by updating your bitcoin.conf file with the following:

```
server=1
rpcuser=your_username
rpcpassword=your_password
rpcallowip=127.0.0.1
rpcport=18332
```

### Tool Configuration:
Configure the script directly by adjusting the following parameters in the script header:

- `rpc_user`: RPC username for Bitcoin node.
- `rpc_password`: RPC password for Bitcoin node.
- `rpc_host`: Host address of the Bitcoin node.
- `rpc_port`: Port number for the Bitcoin node's RPC.
- `debug`: Debug level (1 for INFO, 3 for DEBUG).
- `speak`: Boolean to enable or disable speech output.
- `display_speech_instructions`: Boolean to show speech setup instructions at startup.
- `monitor_interval`: Interval between size checks in seconds.
- `blocks_dir`: Directory path for Bitcoin blocks.
- `indexes_dir`: Directory path for Bitcoin indexes.
- `default_index_size_per_block`: Default estimate of index size per block in GB.
- `speech_volume`: Volume setting for speech (0.0 to 1.0).
- `speech_rate`: Speech rate in words per minute.
- `speech_voice`: Selected voice for speech (can be left as "Default").
- `calculation_decimals`: Number of decimals used for calculations.
- `display_decimals`: Number of decimals used for displaying results.
- `speak_decimals`: Number of decimals used for speech.

## Function List:

1. `initialize_speech_engine`: Initializes the TTS (text-to-speech) engine for speech output.
2. `speak`: Uses the TTS engine to speak a given text.
3. `display_speech_instructions`: Displays instructions for setting up speech properties.
4. `setup_logging`: Sets up logging with the appropriate debug level.
5. `initialize_rpc`: Initializes the RPC connection to the Bitcoin node.
6. `get_blockchain_info`: Retrieves blockchain information from the Bitcoin node using RPC.
7. `calculate_estimated_index_size`: Estimates the total size of the Bitcoin index based on the current block count.
8. `get_directory_size_mb`: Retrieves the size of a directory in MB.
9. `get_free_disk_space_mb`: Retrieves the amount of free disk space in MB.
10. `calculate_indexing_progress`: Calculates the percentage of indexing completed based on disk usage.
11. `display_startup_screen`: Displays a startup screen with information about the tool's purpose, usage, and current configuration.
12. `main`: The main function that runs the tool.

## Additional Notes:

- The tool calculates the block index size in GB by retrieving directory sizes in MB and then converting them to GB.
- The script uses the default estimate of the index size per block, which can be adjusted in the configuration.
- Speech output is configurable and can be disabled if not needed.
- The tool monitors disk usage and reports the progress of Bitcoin block indexing in real-time.

## Logging:

- Logs are stored in `disk_usage_tool.log` and include information about RPC connections, directory sizes, and indexing progress.
- The debug level controls the verbosity of the logs. Higher debug levels (e.g., 3) provide more detailed output.

## Example Usage:

1. Ensure your Bitcoin node is running and configured with the correct RPC settings.
2. Adjust the script configuration if necessary.
3. Run the script:

```bash
python3 disk_usage_tool_btc_index_size.py
```

4. The tool will begin monitoring the block and index directories, reporting the size and indexing progress periodically.
