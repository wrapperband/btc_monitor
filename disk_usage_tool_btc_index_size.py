import os
import logging
import time
import subprocess
import pyttsx3
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

# Configuration settings
CONFIG = {
    'rpc_user': 'wrapperband',
    'rpc_password': 'ardork54',
    'rpc_host': 'localhost',
    'rpc_port': 18332,
    'debug': 3,  # Set to 1 for INFO level, 3 for DEBUG level
    'speak': True,  # Set to True to enable speech output
    'display_speech_instructions': False,  # Set to True to display speech setup instructions
    'monitor_interval': 60,  # Interval between size checks in seconds
    'blocks_dir': '/media/BigDisk2/.bitcoin/blocks',
    'indexes_dir': '/media/BigDisk2/.bitcoin/indexes',
    'default_index_size_per_block_gb': 0.000058,  # Updated index size estimate in GB
    'decimal_places': 2,  # Number of decimal places for display
    'version': '1.04',  # Versioning for tracking
    'speech_voice': "en-gb-scotland",  # Default to "en-gb-scotland" voice if not specified
    'calculation_decimals': 4,  # Used for calculations
    'display_decimals': 2,  # Used for displaying
    'speak_decimals': 1,  # Used for speak numbers
    'log_to_file': True,  # Enable or disable logging to a file
    'log_to_screen': True  # Enable or disable logging to the screen
}

# Initialize TTS engine with configurable properties
def initialize_speech_engine():
    engine = pyttsx3.init()  # Initialize the speech engine
    engine.setProperty('volume', CONFIG.get('speech_volume', 1.0))
    engine.setProperty('rate', CONFIG.get('speech_rate', 150))
    voices = engine.getProperty('voices')
    selected_voice = CONFIG.get('speech_voice', 'Default')

    if selected_voice == 'Default':
        selected_voice = voices[0].id  # Default to the first available voice
        logging.info(f"Default voice selected: {voices[0].name}")
    else:
        logging.info(f"Selected voice: {selected_voice}")
    
    engine.setProperty('voice', selected_voice)
    return engine

engine = initialize_speech_engine()

def speak(text):
    if CONFIG['speak']:
        engine.say(text)
        engine.runAndWait()

def display_speech_instructions():
    if CONFIG['display_speech_instructions']:
        voices = engine.getProperty('voices')
        logging.info("Speech setup instructions:")
        logging.info("1. Volume: Set volume level using `engine.setProperty('volume', value)`")
        logging.info(f"3. Voice: The current voice is set to {CONFIG['speech_voice']}. Available voices are listed below:")
        for voice in voices:
            logging.info(f" - ID: {voice.id}, Name: {voice.name}, Lang: {voice.languages}")

# Setup logging based on the debug level
def setup_logging(debug_level):
    log_levels = {1: logging.INFO, 3: logging.DEBUG}
    logging_format = '%(asctime)s - %(levelname)s - %(message)s'

    # If logging to a file is enabled
    if CONFIG['log_to_file']:
        logging.basicConfig(
            filename='disk_usage_tool.log',
            filemode='a',
            level=log_levels.get(debug_level, logging.INFO),
            format=logging_format
        )

    # Add console handler if logging to the screen is enabled
    if CONFIG['log_to_screen']:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_levels.get(debug_level, logging.INFO))
        console_handler.setFormatter(logging.Formatter(logging_format))
        logging.getLogger().addHandler(console_handler)

# Initialize RPC connection to Bitcoin node
def initialize_rpc():
    rpc_url = f"http://{CONFIG['rpc_user']}:{CONFIG['rpc_password']}@{CONFIG['rpc_host']}:{CONFIG['rpc_port']}"
    try:
        logging.debug(f"Connecting to RPC at {rpc_url}")
        rpc_connection = AuthServiceProxy(rpc_url)
        return rpc_connection
    except JSONRPCException as e:
        logging.error(f"Error connecting to RPC: {e}")
        speak(f"Error connecting to RPC: {e}")
        raise

# Check if Bitcoin server is up
def check_bitcoin_server_status():
    """
    Polls the Bitcoin server every 10 seconds to check if it's up.
    Returns True if the server is up, otherwise False.
    """
    retry_interval = 10  # Time in seconds between retries
    max_retries = 60  # Maximum number of retries (600 seconds total)

    # Print and announce waiting message
    print("Waiting for Bitcoin server to become available......")
    speak("Waiting for Bitcoin server, to become available.")

    for attempt in range(max_retries):
        try:
            rpc_connection = initialize_rpc()
            blockchain_info = rpc_connection.getblockchaininfo()  # Test the connection
            logging.info("Bitcoin server is up.")
            return True  # Bitcoin server is up
        except (JSONRPCException, ConnectionError) as e:
            logging.warning(f"Bitcoin server is down or unreachable: {e}. Retrying in {retry_interval} seconds...")
            speak(f"Bitcoin server is down, retrying in {retry_interval} seconds.")
            time.sleep(retry_interval)

    logging.error("Bitcoin server failed to start after multiple attempts.")
    speak("Bitcoin server failed to start after multiple attempts.")
    return False

def display_startup_screen():
    """
    Displays a startup screen with information about the tool's purpose, usage, and output.
    Also displays current configuration settings such as the monitoring interval and directories.
    """
    print("="*34)
    print(f"disk_usage_tool_btc_index_size.py v{CONFIG['version']}")
    print("="*34)
    print("Description:")
    print("  Calculates and monitors the size of the Bitcoin block index during re-indexing.")
    print("Usage:")
    print("  Ensure the Bitcoin node is running and accessible. Customize the configuration")
    print("  settings in the header of this script. Run the script using the command:")
    print("  python3 disk_usage_tool_btc_index_size.py")
    print("Output:")
    print("  Prints estimated block index size and monitors size changes during re-indexing.")
    print("Speech Settings:")
    print(f"  Speech enabled: {CONFIG['speak']}")
    print(f"  Speech voice: {CONFIG['speech_voice']}")
    print("Configuration:")
    print(f"  Monitoring interval: {CONFIG['monitor_interval']} seconds")
    print(f"  Blocks directory: {CONFIG['blocks_dir']}")
    print(f"  Indexes directory: {CONFIG['indexes_dir']}")
    print("="*34)

    # Poll Bitcoin server status until it's up
    if check_bitcoin_server_status():
        print("Bitcoin server is up and running.")
        logging.info("Bitcoin server is up and running.")
        speak("Bitcoin server is up and running.")
    else:
        print("Bitcoin server is down or unreachable after retries.")
        logging.error("Bitcoin server is down or unreachable after retries.")
        speak("Bitcoin server is down or unreachable.")
    
    print("Starting index size monitoring tool...")
    speak("Hard Disk usage tool, for Bitcoin index size is starting.")
    speak("Monitoring the size of Bitcoin block index during re-indexing.")

def get_directory_size(directory):
    """
    Retrieves the size of the specified directory using the `du` command.
    Returns the size in GB.
    """
    if not os.path.isdir(directory):
        logging.error(f"Directory does not exist: {directory}")
        return 0

    try:
        result = subprocess.run(['du', '-sh', directory], capture_output=True, text=True, check=True)
        logging.debug(f"du command output: {result.stdout}")
        size_str = result.stdout.split()[0]
        size_value, size_unit = float(size_str[:-1]), size_str[-1]
        if size_unit == 'G':
            return size_value
        elif size_unit == 'M':
            return size_value / 1024
        elif size_unit == 'K':
            return size_value / (1024 ** 2)
        return size_value
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running command: {e}")
        logging.error(f"Command output: {e.output}")
        return 0  # Return 0 on error instead of raising an exception to avoid crashing

def get_free_disk_space(path):
    """
    Retrieves the available free disk space for the specified path using the `df` command.
    Returns the free space in GB.
    """
    try:
        result = subprocess.run(['df', '-h', path], capture_output=True, text=True, check=True)
        free_space_str = result.stdout.splitlines()[1].split()[3]
        size_value, size_unit = float(free_space_str[:-1]), free_space_str[-1]
        if size_unit == 'G':
            return size_value
        elif size_unit == 'T':
            return size_value * 1024
        elif size_unit == 'M':
            return size_value / 1024
        return size_value
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running command: {e}")
        logging.error(f"Command output: {e.output}")
        return 0  # Return 0 on error instead of raising an exception to avoid crashing

def monitor_disk_usage():
    """
    Monitors the disk usage for the Bitcoin blocks and indexes directories, calculating the indexing progress.
    Logs the current directory sizes, free disk space, the proportion of disk used, and the indexing progress as a percentage.
    """
    logging.info("Starting to monitor disk usage.")
    
    while True:
        blocks_size_gb = get_directory_size(CONFIG['blocks_dir'])
        indexes_size_gb = get_directory_size(CONFIG['indexes_dir'])

        if blocks_size_gb == 0 or indexes_size_gb == 0:
            logging.error("Failed to retrieve directory sizes.")
            speak("Failed to retrieve directory sizes.")
            continue

        # Estimate the total index size based on block height
        estimated_total_index_size = CONFIG['default_index_size_per_block_gb'] * 860900  # Adjust for current block height
        progress = (indexes_size_gb / estimated_total_index_size) * 100
        progress = min(progress, 100)  # Cap progress at 100%

        # Get the free space on disk and calculate the proportion of disk used by indexes
        free_space_gb = get_free_disk_space(CONFIG['blocks_dir'])
        total_disk_size_gb = blocks_size_gb + indexes_size_gb + free_space_gb
        disk_used_percent = (indexes_size_gb / total_disk_size_gb) * 100

        # Log and print the sizes, progress, and disk usage information
        logging.info(f"Blocks directory size: {blocks_size_gb:.2f} GB")
        print(f"Blocks directory size: {blocks_size_gb:.2f} GB")

        logging.info(f"Indexes directory size: {indexes_size_gb:.2f} GB")
        print(f"Indexes directory size: {indexes_size_gb:.2f} GB")

        logging.info(f"Estimated indexing progress: {progress:.2f}%")
        print(f"Estimated indexing progress: {progress:.2f}%")

        logging.info(f"Free disk space: {free_space_gb:.2f} GB")
        print(f"Free disk space: {free_space_gb:.2f} GB")

        logging.info(f"Total disk size: {total_disk_size_gb:.2f} GB")
        print(f"Total disk size: {total_disk_size_gb:.2f} GB")

        logging.info(f"Proportion of disk used by indexes: {disk_used_percent:.2f}%")
        print(f"Proportion of disk used by indexes: {disk_used_percent:.2f}%")

        # Speak the same information
        speak(f"Blocks directory size is {blocks_size_gb:.{CONFIG['speak_decimals']}f} gigabytes.")
        speak(f"Indexes directory size is {indexes_size_gb:.{CONFIG['speak_decimals']}f} gigabytes.")
        speak(f"Estimated indexing progress is {progress:.{CONFIG['speak_decimals']}f} percent.")
        speak(f"Free disk space is {free_space_gb:.{CONFIG['speak_decimals']}f} gigabytes.")
        speak(f"Proportion of disk used by indexes is {disk_used_percent:.{CONFIG['speak_decimals']}f} percent.")

        if free_space_gb < 5:  # Low disk space warning threshold (5 GB)
            logging.warning("Low disk space!")
            speak("Warning, disk space is running low!")

        # Wait for the next interval before checking again
        time.sleep(CONFIG['monitor_interval'])

# Main function to run the tool
def main():
    """
    The main function orchestrates the tool's functionality:
    - Displays startup information.
    - Initializes the RPC connection to the Bitcoin node.
    - Continuously monitors the block and index directories, calculating progress.
    - Logs the progress and optionally speaks the current indexing status.
    """
    setup_logging(CONFIG['debug'])
    display_startup_screen()

    # Start monitoring the disk usage
    monitor_disk_usage()

if __name__ == "__main__":
    main()
