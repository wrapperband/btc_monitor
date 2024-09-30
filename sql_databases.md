# Database Structure and CSV Export Documentation

## Overview
The BTC Monitor System uses SQL databases to store and manage transaction data for both general transactions and coinbase transactions. This document outlines the structure of the databases and how CSV reports are generated from them.

## SQL Database Structure

### 1. `addresses.db`
This database contains all non-coinbase transaction data. Each record corresponds to a Bitcoin address involved in a transaction, along with relevant details about the transaction.

- **Table**: `addresses`
- **Fields**:
  - `address`: The Bitcoin address involved in the transaction.
  - `value`: The total amount of BTC sent/received at this address.
  - `percentage_of_total`: The percentage of the transaction value that this address holds.
  - `txid`: The unique transaction ID.
  - `tx_time`: The timestamp when the transaction occurred.
  - `event_time`: The event time associated with the transaction (optional).
  - `price_change`: The percentage price change associated with the transaction (optional).
  - `count`: The number of times this address appears in the database (for multi-transaction addresses).
  - `btc_in`: The total BTC received at this address (optional).
  - `btc_out`: The total BTC sent from this address (optional).
  - `name`: A label to identify the address (e.g., whale, miner) (optional).

### 2. `coinbase_addresses.db`
This database holds all coinbase (mining reward) transactions. Each record represents an address that received mining rewards.

- **Table**: `coinbase_addresses`
- **Fields**:
  - `address`: The miner's Bitcoin address.
  - `value`: The mining reward received at this address.
  - `txid`: The unique coinbase transaction ID.
  - `tx_time`: The timestamp when the block was mined.
  - `event_time`: Optional field, can be the same as `tx_time` or left blank.
  - `price_change`: Optional field, could be used to record market conditions at the time.
  - `count`: Number of blocks mined by this address.
  - `btc_in`: Typically zero for coinbase transactions.
  - `btc_out`: Typically zero for coinbase transactions.
  - `name`: A label for the miner (e.g., mining pool).

## CSV Export Format

After processing, the system exports the following CSV files based on the data stored in the SQL databases:

### `addresses.csv`
This CSV file contains transaction data extracted from the `addresses.db` database. The fields are:

| Field                | Description                                               |
|----------------------|-----------------------------------------------------------|
| `address`            | Bitcoin address involved in the transaction               |
| `value`              | Total amount of BTC sent/received at this address          |
| `percentage_of_total`| Percentage of total transaction value (if applicable)      |
| `txid`               | Transaction ID                                            |
| `tx_time`            | Time of the transaction                                   |
| `event_time`         | Time of the associated event (optional)                   |
| `price_change`       | Percentage price change at the time of the transaction     |
| `count`              | Number of occurrences of this address                     |
| `btc_in`             | Total BTC received at this address                        |
| `btc_out`            | Total BTC sent from this address                          |
| `name`               | Optional label for the address (e.g., whale, miner)       |

### `coinbase_addresses.csv`
This CSV file contains mining reward data from the `coinbase_addresses.db` database. The format is the same as `addresses.csv` but is specific to coinbase transactions:

| Field                | Description                                               |
|----------------------|-----------------------------------------------------------|
| `address`            | Miner’s Bitcoin address                                   |
| `value`              | Mining reward received                                    |
| `txid`               | Coinbase transaction ID                                   |
| `tx_time`            | Time when the block was mined                             |
| `event_time`         | Optional, can be the same as `tx_time`                    |
| `price_change`       | Optional, percentage price change at the time of mining   |
| `count`              | Number of blocks mined by this address                    |
| `btc_in`             | Typically zero                                            |
| `btc_out`            | Typically zero                                            |
| `name`               | Optional label for the miner                              |

## Deduplication and Final CSV Generation

To ensure that only unique addresses are exported, the system performs deduplication before exporting to CSV. Both the `addresses.db` and `coinbase_addresses.db` databases are scanned for duplicate entries, based on the combination of `address`, `txid`, and `tx_time`.

### Deduplication Process
- Remove duplicate entries in `addresses.db` based on key fields like `address`, `value`, `txid`, and `tx_time`.
- Remove duplicate entries in `coinbase_addresses.db` using the same method.
- After deduplication, generate the final `addresses.csv` and `coinbase_addresses.csv` reports.

## Future Improvements

The SQL-based approach allows for future enhancements such as:
- **Real-time monitoring**: Enhanced address tracking and filtering.
- **Address labeling**: As addresses are identified (e.g., miners, whales), they can be labeled in the `name` field.
- **Performance improvements**: The use of indexed searches in SQL speeds up address lookups and transaction analysis.

## Conclusion

This structure offers a flexible and scalable way to manage and analyze Bitcoin transaction data, with the ability to easily generate reports and monitor the blockchain in real time.
