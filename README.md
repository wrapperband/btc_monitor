# Bitcoin Monitoring and Analysis Suite

The Bitcoin Monitoring and Analysis Suite is a comprehensive system designed to automate the tracking, analysis, and reporting of significant Bitcoin blockchain activities. This suite enables users to monitor large transactions, analyze market events, and generate actionable insights, making it an essential tool for staying ahead of market dynamics.

## Key Components

### `btc_monitor.py`
- **Purpose**: Extracts and records Bitcoin addresses involved in significant transactions around predefined market events.
- **Functionality**: Processes blockchain data within specified time ranges, identifying transactions that exceed a given BTC threshold. Populates `addresses.csv` with relevant addresses and transaction details.

### Agent System (`agent.py`)
- **Purpose**: Automates continuous monitoring of blockchain data, providing round-the-clock tracking of new blocks and transactions.
- **Functionality**: Operates based on a configuration file (`agent-001.conf`), executing tasks such as block monitoring and address tracking at regular intervals. Logs activities and can trigger alerts based on predefined conditions.

### `address_percent.py`
- **Purpose**: Filters Bitcoin addresses based on their percentage of total transaction value and compiles statistics for key events.
- **Functionality**: Analyzes data in `addresses.csv`, filtering addresses within a specified percentage range, and generates a summarized report (`address_events.csv`) that provides insights into transaction behaviors during specific events.

## How It Works

1. **Event-Driven Monitoring**:
   - Define significant market events in `changeevents.csv`, such as large price changes. These events guide the system’s focus on relevant time frames and transactions.

2. **Data Extraction and Initialization**:
   - Run `btc_monitor.py` to process blockchain data around the defined events, extracting and recording addresses and transaction details into `addresses.csv`.

3. **Automated Continuous Monitoring**:
   - The agent system continuously tracks new blockchain data, using the initialized `addresses.csv` to keep the system updated with the latest relevant transactions.

4. **Data Filtering and Analysis**:
   - Use `address_percent.py` to refine the collected data, filtering addresses based on their transaction significance and compiling event-based statistics to understand market behavior.

5. **Reporting and Alerts**:
   - The system generates detailed logs, reports, and alerts based on the data and analysis, providing users with timely insights and enabling proactive decision-making.

## Getting Started

### Prerequisites
- Python 3.7 or higher
- A running Bitcoin node with RPC access
- Install necessary Python dependencies:
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

2. **Run `btc_monitor.py`**:
   - Extract and record Bitcoin addresses involved in transactions related to your events.
   - Example:
     ```bash
     python btc_monitor.py
     ```

3. **Configure and Run the Agent**:
   - Set up `agent-001.conf` to define tasks and intervals.
   - Run the agent to start continuous monitoring:
     ```bash
     python agent.py
     ```

4. **Analyze Data with `address_percent.py`**:
   - Filter addresses and compile event statistics:
     ```bash
     python address_percent.py
     ```

## Contributions

Contributions are welcome! Please submit a pull request or open an issue to discuss any changes or improvements.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contact

For any inquiries, please contact [Your Email] or submit an issue on GitHub.

---

Happy Monitoring!
