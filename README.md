```
    ██████╗ ████████╗ ██████╗        ███╗   ███╗ ██████╗ ███╗   ██╗██╗████████╗ ██████╗ ██████╗   
    ██╔══██╗╚══██╔══╝██╔═══██╗       ████╗ ████║██╔═══██╗████╗  ██║██║╚══██╔══╝██╔═══██╗██╔══██╗  
    ██████╔╝   ██║   ██║             ██╔████╔██║██║   ██║██╔██╗ ██║██║   ██║   ██║   ██║██████╔╝  
    ██╔══██╗   ██║   ██║   ██║       ██║╚██╔╝██║██║   ██║██║╚██╗██║██║   ██║   ██║   ██║██╔══██╗  
    ██████╔╝   ██║   ╚██████╔╝       ██║ ╚═╝ ██║╚██████╔╝██║ ╚████║██║   ██║   ╚██████╔╝██║  ╚██╗
    ╚═════╝    ╚═╝    ╚═════╝        ╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝   ╚═╝    ╚═════╝ ╚═╝   ╚═╝ 
```

# Bitcoin Monitoring and Analysis Suite

The Bitcoin Monitoring and Analysis Suite is a comprehensive system designed to automate the tracking, analysis, and reporting of significant Bitcoin blockchain activities. This suite enables users to monitor large transactions, analyze market events, and generate actionable insights, making it an essential tool for staying ahead of market dynamics.

## Key Components

### `btc_monitor.py`
- **Purpose**: Extracts and records Bitcoin addresses involved in significant transactions around predefined market events.
- **Functionality**: Processes blockchain data within specified time ranges, identifying transactions that exceed a given BTC threshold. Populates `addresses.csv` with relevant addresses and transaction details. BTC Monitor analyses the types and frequency of transactions within the filter periods, either set in the config or btc_monitor.conf, or using the "batch file" system.

BTC_monitor.py can be passed a "overnight_batch.csv" file, which contains a set of filters to analyse.

$ python3 btc_monitor.py --batch overnight_batch.csv

BTC_monitor creates a events.csv file which is analysis of transactions for the time period before each event in the changeevents.csv file, for each of the filter conditions such as transaction value range, in the customisable configuration file "overnight_batch.csv".

### Agent System (`agent.py`)
- **Purpose**: Automates continuous monitoring of blockchain data, providing round-the-clock tracking of new blocks and transactions.
- **Functionality**: Operates based on a flexible configuration system (`agent-XXX.conf`), executing tasks such as block monitoring and address tracking at regular intervals. Logs activities and can trigger alerts based on predefined conditions.

### `address_percent.py`
- **Purpose**: Filters Bitcoin addresses based on their percentage of total transaction value and compiles statistics for key events.
- **Functionality**: Analyzes data in `addresses.csv`, filtering addresses within a specified percentage range, and generates a summarized report (`address_events.csv`) that provides insights into transaction behaviors during specific events.

### `block_sync_status.py`
- **Purpose**: Monitors the synchronization status of your Bitcoin node.
- **Functionality**: Continuously checks the current block against the latest block header to determine the number of blocks remaining to sync. This script is useful for ensuring your node is fully synchronized before running other monitoring tasks.

## Flexible Agent Configuration

The Agent system (`agent.py`) is designed to be highly flexible, allowing users to define various tasks that run at specific intervals. Tasks can include checking for new blocks, monitoring specific addresses, or performing custom analysis.

### How the Agent System Works

1. **Task Definition**: Tasks are defined in the configuration file (e.g., `agent-001.conf`) under the `[Tasks]` section. Each task is assigned a name and an interval at which it should run.

2. **Conditional Execution**: Tasks can be conditionally executed based on the output of previous tasks. For example, you can set the agent to monitor addresses only if a new block has been detected.

3. **Task Execution**: The agent reads the configuration, determines which tasks to run and at what intervals, and then executes the tasks sequentially. It logs the results and waits until the next interval.

### Available Tasks

The agent comes with built-in tasks, which can be easily extended or customized:

- **`check_blocks`**:
  - **Purpose**: Checks for new blocks on the Bitcoin blockchain.
  - **Example**: `task1 = check_blocks, interval=5min`
  - **Returns**: `True` if new blocks are found, `False` otherwise.

- **`monitor_addresses`**:
  - **Purpose**: Monitors Bitcoin addresses for significant transactions. This task is typically run after detecting a new block.
  - **Example**: `task2 = monitor_addresses, interval=15min, if=task1`
  - **Depends on**: The `check_blocks` task.

### Example Configuration (`agent-001.conf`):

```ini
[General]
agent_id = agent-001
log_file = agent-001.log
log_level = DEBUG
block_file = ./data/blocknumber.csv
address_file = ./data/addresses.csv
data_directory = ./data/

[Tasks]
task1 = check_blocks, interval=5min
task2 = monitor_addresses, interval=15min, if=task1

[Reporting]
method = console
email = False
alert_on_large_transaction = True
large_transaction_threshold = 1000
smtp_server = smtp.example.com
smtp_port = 587
smtp_user = email_user@example.com
smtp_password = email_password
email_recipient = admin@example.com
```

### Running the Agent

To run the agent:

```bash
python agent.py agent-001
```

This will start the agent with the configuration specified in `agent-001.conf`. The agent will continuously monitor the blockchain, checking for new blocks every 5 minutes (as defined in `task1`). If a new block is found, it will run the `monitor_addresses` task 15 minutes later (as defined in `task2`).

## Environment Setup

### Prerequisites
- **Python**: Python 3.7 or higher.
- **Bitcoin Node**: A running Bitcoin node with RPC access.
- **Virtual Environment**: Set up a Python virtual environment for managing dependencies.

### Setting Up the Python Environment
1. **Create a Virtual Environment**:
   ```bash
   python3 -m venv btc_monitor_env
   ```
2. **Activate the Environment**:
   ```bash
   source btc_monitor_env/bin/activate
   ```
3. **Install Dependencies**:
   Ensure your `requirements.txt` is up to date and install the necessary Python packages:
   ```bash
   pip install -r requirements.txt
   ```

### Directory Structure
Ensure your project directory is set up as follows:
```
/bitcoin-monitoring-suite
├── agent.py
├── agent-001.conf
├── btc_monitor.py
├── address_percent.py
├── block_sync_status.py
├── changeevents.csv
├── requirements.txt
├── /data
│   ├── addresses.csv
│   ├── address.csv
│   ├── address_events.csv
│   └── blocknumber.csv
└── /logs
    └── agent-001.log
```

### Running the Bitcoin Node
Before running the monitoring scripts, ensure that your Bitcoin node is running and synchronized. The `bitcoind` daemon should be started with the appropriate configuration:

```bash
/media/programs/bitcoin-22.0/bin/bitcoind -conf=/home/user/.config/Bitcoin/bitcoin.conf -printtoconsole -daemon
```

### Running the Suite

1. **Set Up `changeevents.csv`**:
   - Define your significant events with price changes and corresponding timestamps.
   - Example:
     ```csv
     change_percent,date_time
     -2.5,2024/08/29 19:00:00
     -3.7,2024/08/27 20:23:00
     -3.3,2024/08/20 15:00:00
     -4.0,2024/07/31 20:14:00
     ```

2. **Run `block_sync_status.py`**:
   - Continuously monitor the synchronization status of your Bitcoin node to ensure it is fully synced before running other tasks:
     ```bash
     python block_sync_status.py
     ```

3. **Run `btc_monitor.py`**:
   - Extract and record Bitcoin addresses involved in transactions related to your events.
   - Example:
     ```bash
     python btc_monitor.py  --batch overnight_batch.csv
     ```

4. **Configure and Run the Agent**:
   - Set up `agent-001.conf` to define tasks and intervals.
   - Run the agent to start continuous monitoring:
     ```bash
     python agent.py agent-001
     ```

5. **Analyze Data with `address_percent.py`**:
   - Filter addresses and compile event statistics:
     ```bash
     python address_percent.py
     ```

## Contributions

Contributions are welcome! Please submit a pull request or open an issue to discuss any changes or improvements.

## License

This project is licensed under the GNU. See the `LICENSE` file for details.

## Contact

Submit an issue on GitHub.

---

Happy Monitoring!
