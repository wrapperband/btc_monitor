#!/usr/bin/env python3

import sqlite3
import os
import sys
import time
from datetime import datetime
from bitcoinrpc.authproxy import AuthServiceProxy

# Configuration
CONFIG = {
    'version': '1.03',
    'database_file': 'daily_block_summary.db',
    'rpc_user': 'wrapperband',
    'rpc_password': 'ardork54',
    'rpc_host': 'localhost',
    'rpc_port': 18332,
    'debug': True,
    'log_file': 'block_summary.log'
}

# Initialize logging
def log(message):
    if CONFIG['debug']:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(CONFIG['log_file'], 'a') as f:
            f.write(f"{timestamp} - {message}\n")
        print(f"{timestamp} - {message}")

# Connect to Bitcoin RPC
def connect_rpc():
    try:
        rpc_url = f"http://{CONFIG['rpc_user']}:{CONFIG['rpc_password']}@{CONFIG['rpc_host']}:{CONFIG['rpc_port']}"
        return AuthServiceProxy(rpc_url, timeout=120)
    except Exception as e:
        log(f"Error connecting to Bitcoin RPC: {e}")
        sys.exit(1)

# Initialize database
def initialize_database(conn, cursor):
    # Create the daily_summary table with the required columns
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_summary (
        date TEXT PRIMARY KEY,
        total_blocks INTEGER,
        avg_blocks_per_hour REAL,
        min_blocks_per_hour INTEGER,
        max_blocks_per_hour INTEGER
    )
    """)
    conn.commit()
    log("Database initialized.")


# Process blocks
def process_blocks(conn, cursor, rpc_conn):
    # Retrieve the last processed date
    cursor.execute("SELECT date FROM daily_summary ORDER BY date DESC LIMIT 1")
    result = cursor.fetchone()
    if result:
        last_processed_date = result[0]
        last_processed_timestamp = int(datetime.strptime(last_processed_date, '%Y-%m-%d').timestamp())
    else:
        # If no data, start from the first block
        last_processed_timestamp = 1231006505  # Bitcoin genesis block timestamp
        last_processed_date = datetime.utcfromtimestamp(last_processed_timestamp).strftime('%Y-%m-%d')
        log("No previous data found. Starting from the genesis block.")

    # Get the current block height
    current_block_height = rpc_conn.getblockcount()
    log(f"Current block height: {current_block_height}")

    # Initialize variables
    block_heights = []
    block_times = []

    # Fetch blocks from the last processed timestamp to the current block
    for height in range(current_block_height + 1):
        block_hash = rpc_conn.getblockhash(height)
        block_header = rpc_conn.getblockheader(block_hash)
        block_time = block_header['time']
        block_heights.append(height)
        block_times.append(block_time)

        # Log progress every 1000 blocks
        if height % 1000 == 0:
            log(f"Fetched block {height}/{current_block_height}")

    # Organize blocks by day
    blocks_by_day = {}
    for height, timestamp in zip(block_heights, block_times):
        date_str = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')
        if date_str not in blocks_by_day:
            blocks_by_day[date_str] = []
        blocks_by_day[date_str].append(timestamp)

    # Process each day's blocks
    for date_str in sorted(blocks_by_day.keys()):
        if date_str <= last_processed_date:
            continue  # Skip already processed dates

        timestamps = blocks_by_day[date_str]
        total_blocks = len(timestamps)

        # Organize blocks by hour
        blocks_by_hour = {}
        for ts in timestamps:
            hour_str = datetime.utcfromtimestamp(ts).strftime('%H')
            if hour_str not in blocks_by_hour:
                blocks_by_hour[hour_str] = 0
            blocks_by_hour[hour_str] += 1

        hourly_blocks = list(blocks_by_hour.values())
        avg_blocks_per_hour = sum(hourly_blocks) / len(hourly_blocks)
        min_blocks_per_hour = min(hourly_blocks)
        max_blocks_per_hour = max(hourly_blocks)

        # Insert summary into the database
        cursor.execute("""
        INSERT OR REPLACE INTO daily_summary
        (date, total_blocks, avg_blocks_per_hour, min_blocks_per_hour, max_blocks_per_hour)
        VALUES (?, ?, ?, ?, ?)
        """, (date_str, total_blocks, avg_blocks_per_hour, min_blocks_per_hour, max_blocks_per_hour))
        conn.commit()
        log(f"Processed date {date_str}: Total blocks={total_blocks}, Avg/hour={avg_blocks_per_hour:.2f}, Min/hour={min_blocks_per_hour}, Max/hour={max_blocks_per_hour}")

    log("Block processing completed.")

if __name__ == "__main__":
    print("===== Blocks Per Day and Per Hour Analysis =====")
    print(f"Version: {CONFIG['version']}")
    print("This script calculates the blocks per day and hourly averages, min, max for each day.")
    print(f"Database: {CONFIG['database_file']}")
    input("Press any key to start...\n")

    # Connect to RPC
    rpc_conn = connect_rpc()

    # Connect to database
    conn = sqlite3.connect(CONFIG['database_file'])
    cursor = conn.cursor()
    initialize_database(conn, cursor)

    # Process blocks
    process_blocks(conn, cursor, rpc_conn)

    # Close connections
    conn.close()
    log("Script execution completed.")
