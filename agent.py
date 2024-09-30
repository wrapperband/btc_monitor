import os
import time
import logging
import configparser
import sys
import traceback
import psutil
from datetime import datetime, timedelta
from monitor_addresses import monitor_addresses,  address_cache
from check_blocks import check_blocks
from cleanup import monitor_addresses_cleanup
import signal

def load_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    logging.info(f"Configuration loaded from {config_file}:")
    for section in config.sections():
        logging.info(f"[{section}]")
        for key, value in config.items(section):
            logging.info(f"{key} = {value}")
    return config

def initialize_logging(config):
    log_file = config['General']['log_file']
    log_dir = os.path.dirname(log_file)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    console_log_level_str = config['Reporting'].get('console', 'INFO').split('#')[0].strip().upper()
    file_log_level_str = config['Reporting'].get('log', 'INFO').split('#')[0].strip().upper()

    console_log_level = getattr(logging, console_log_level_str, logging.NOTSET)
    file_log_level = getattr(logging, file_log_level_str, logging.NOTSET)

    logger = logging.getLogger()
    
    # Clear any existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(logging.INFO)  # Ensure the root logger level is set to INFO

    if file_log_level != logging.NOTSET:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(file_log_level)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)

    if console_log_level != logging.NOTSET:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_log_level)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)

    logger.info("Logging initialized.")



def display_startup_screen(agent_name, config):
    print("\n==============================")
    print(f"Starting Agent: {agent_name}")
    print("==============================")
    
    if config['General'].get('log_level', 'INFO').upper() == 'DEBUG':
        print("\nAgent Configuration:")
        for section in config.sections():
            print(f"[{section}]")
            for key, value in config.items(section):
                print(f"{key} = {value}")
        print("==============================\n")
    else:
        print("Agent is starting in non-debug mode.")
        print("==============================\n")
    
    print("Press 'Ctrl+C' to exit the agent and trigger a graceful shutdown.")
    logging.info(f"Agent {agent_name} started.")

def check_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    logging.debug(f"Memory usage: RSS = {mem_info.rss / (1024 * 1024):.2f} MB")

def run_tasks(config, agent_data_directory):
    tasks = config['Tasks']
    try:
        while True:
            for task_entry in tasks:
                try:
                    task_name, interval_str, *additional_params = tasks[task_entry].split(',')
                    task_name = task_name.strip()
                    interval_str = interval_str.strip()

                    interval = int(interval_str.replace('interval=', '').replace('min', '').strip()) * 60
                    logging.info(f"Starting task: {task_name}")

                    result = run_task(task_name, config, additional_params, agent_data_directory)
                    logging.info(f"Task {task_name} completed with result: {result}")

                    check_memory_usage()

                    wake_up_time = datetime.now() + timedelta(seconds=interval)
                    logging.info(f"Sleeping for {interval} seconds until next task is due at {wake_up_time.strftime('%Y-%m-%d %H:%M:%S')}.")

                    time.sleep(interval)

                except Exception as e:
                    logging.error(f"Error processing task entry '{task_entry}': {e}")
                    logging.error(traceback.format_exc())
    except Exception as e:
        logging.error(f"Unexpected error during task loop: {e}")
    finally:
        # Run cleanup only after the loop ends or when an exception occurs that stops the loop
        monitor_addresses_cleanup(config, address_cache)

def run_task(task_name, config, additional_params=None, agent_data_directory=None):
    start_time = time.time()
    logging.info(f"Starting task {task_name} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        task_func_map = {
            "check_blocks": check_blocks,
            "monitor_addresses": monitor_addresses,
        }
        
        if task_name in task_func_map:
            result = task_func_map[task_name](config)
        else:
            raise ValueError(f"Unknown task: {task_name}")
    
    except Exception as e:
        logging.error(f"Exception occurred while running task {task_name}: {e}")
        logging.error(traceback.format_exc())
        result = False

    elapsed_time = time.time() - start_time
    logging.info(f"Completed task {task_name}. Elapsed time: {elapsed_time:.2f} seconds.")
    
    return result

def check_and_setup_agent(agent_name):
    main_directory = os.getcwd()
    agent_conf = os.path.join(main_directory, f'{agent_name}.conf')
    agent_data_directory = os.path.join(main_directory, f'{agent_name}-data')
    
    if not os.path.exists(agent_data_directory):
        os.makedirs(agent_data_directory)
    
    if not os.path.exists(agent_conf):
        raise FileNotFoundError(f"Agent configuration file {agent_conf} not found.")

    create_default_data_files(agent_data_directory)
    return agent_conf, agent_data_directory

def create_default_data_files(agent_data_directory):
    block_file = os.path.join(agent_data_directory, 'blocknumber.csv')
    address_file = os.path.join(agent_data_directory, 'address.csv')
    
    if not os.path.exists(block_file):
        with open(block_file, 'w') as f:
            f.write("")
        logging.info(f"Block file created at {block_file}.")
    
    if not os.path.exists(address_file):
        with open(address_file, 'w') as f:
            f.write("address,transaction_value\n")
        logging.info(f"Address file created at {address_file}.")

def shutdown_agent(signal_received, frame, config):
    logging.info("Shutting down the agent due to signal interrupt...")
    monitor_addresses_cleanup(config, address_cache)  # Cleanup called during shutdown
    logging.info("Agent shutdown completed.")
    sys.exit(0)

def main():
    agent_name = 'agent-001'
    if len(sys.argv) > 1:
        agent_name = sys.argv[1]

    config_file, agent_data_directory = check_and_setup_agent(agent_name)
    config = load_config(config_file)

    initialize_logging(config)

    display_startup_screen(agent_name, config)

    logging.info(f"Agent {agent_name} is starting up...")

    # Correctly pass the config to shutdown_agent
    signal.signal(signal.SIGINT, lambda sig, frame: shutdown_agent(sig, frame, config))

    try:
        run_tasks(config, agent_data_directory)
    except Exception as e:
        logging.error(f"Exception in the main loop: {e}")
        logging.error(traceback.format_exc())
        shutdown_agent(signal.SIGINT, None, config)

if __name__ == "__main__":
    main()
