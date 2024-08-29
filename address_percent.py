"""
address_percent.py

This script filters Bitcoin addresses from an input CSV file (addresses.csv) based on a specified 
percentage range of total transaction value. It then compiles statistics for each event based 
on the date, including average, maximum, minimum percentage, and the total number of addresses 
that passed the filter for each event. The filtered addresses are written to address.csv, and 
event statistics are written to address_events.csv.

Usage:
    1. Adjust the MIN_PERCENTAGE and MAX_PERCENTAGE variables in the script to set the desired 
       percentage range.
    2. Ensure that 'addresses.csv' is present in the same directory as this script.
    3. Run the script from the terminal using:
       python3 address_percent.py
    4. The filtered results will be saved in 'address.csv', and event statistics will be saved 
       in 'address_events.csv'.

Configuration:
    - MIN_PERCENTAGE: Minimum percentage of total transaction value to include in the filter.
    - MAX_PERCENTAGE: Maximum percentage of total transaction value to include in the filter.
    - INPUT_FILE: The input CSV file to read from (default: 'addresses.csv').
    - OUTPUT_FILE: The output CSV file to write filtered records to (default: 'address.csv').
    - EVENTS_FILE: The output CSV file to write event statistics to (default: 'address_events.csv').

Notes:
    - The input CSV file ('addresses.csv') must contain 'percentage_of_total' and 'event_time' fields.
    - The script writes all filtered records to 'address.csv' and event statistics to 'address_events.csv'.
    - Make sure to run this script in a Python 3 environment.

"""

import csv
from collections import defaultdict

# Configuration for filtering
MIN_PERCENTAGE = 20  # Minimum percentage of total to consider
MAX_PERCENTAGE = 80  # Maximum percentage of total to consider

INPUT_FILE = 'addresses.csv'       # The input file to read from
OUTPUT_FILE = 'address.csv'        # The output file to write filtered records to
EVENTS_FILE = 'address_events.csv' # The output file to write event statistics to

# Function to apply the percentage filter, compile event statistics, and write results to new CSV files
def filter_addresses_and_compile_stats(input_file, output_file, events_file, min_percentage, max_percentage):
    events = defaultdict(list)  # To store statistics for each event date based on filtered addresses

    with open(input_file, mode='r') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames

        with open(output_file, mode='w', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                percentage_of_total = float(row['percentage_of_total'])
                event_date = row['event_time'].split()[0]  # Assuming event_time is in the format 'YYYY-MM-DD HH:MM:SS'

                if min_percentage <= percentage_of_total <= max_percentage:
                    writer.writerow(row)  # Write the filtered row to the output file
                    print(f"Included: {row['address']} with {percentage_of_total}%")

                    # Store the percentage for the corresponding event date (only for filtered records)
                    events[event_date].append(percentage_of_total)

    # Compile and write event statistics to the events file (based on filtered addresses)
    with open(events_file, mode='w', newline='') as eventfile:
        event_writer = csv.writer(eventfile)
        event_writer.writerow(['event_date', 'average_percentage', 'max_percentage', 'min_percentage', 'num_addresses'])

        for event_date, percentages in events.items():
            average_percentage = sum(percentages) / len(percentages)
            max_percentage = max(percentages)
            min_percentage = min(percentages)
            num_addresses = len(percentages)

            event_writer.writerow([event_date, average_percentage, max_percentage, min_percentage, num_addresses])
            print(f"Stats for {event_date}: Avg={average_percentage}, Max={max_percentage}, Min={min_percentage}, Count={num_addresses}")

    print(f"Filtering and statistics compilation complete. Filtered records written to {output_file}, event stats written to {events_file}.")

# Main execution
if __name__ == "__main__":
    print(f"Running filter: MIN_PERCENTAGE={MIN_PERCENTAGE}%, MAX_PERCENTAGE={MAX_PERCENTAGE}%")
    filter_addresses_and_compile_stats(INPUT_FILE, OUTPUT_FILE, EVENTS_FILE, MIN_PERCENTAGE, MAX_PERCENTAGE)
