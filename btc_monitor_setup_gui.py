import tkinter as tk
from tkinter import scrolledtext
import threading
import time
import subprocess

# Function to start the BTC Monitor
def start_btc_monitor():
    # Collect configuration values from the UI inputs
    min_btc_transfer = min_btc_entry.get()
    max_btc_transfer = max_btc_entry.get()
    time_before_event = time_before_event_entry.get()
    
    # Update the config variables (you would set them globally or pass them to the script)
    config['MIN_BTC_TRANSFER'] = float(min_btc_transfer)
    config['MAX_BTC_TRANSFER'] = None if max_btc_transfer.lower() == "none" else float(max_btc_transfer)
    config['TIME_BEFORE_EVENT'] = int(time_before_event)
    
    # Run the monitoring process in a separate thread to prevent UI blocking
    monitor_thread = threading.Thread(target=run_btc_monitor)
    monitor_thread.start()

def run_btc_monitor():
    try:
        # Start the actual BTC monitoring script as a subprocess
        process = subprocess.Popen(
            ["python3", "btc_monitor.py"], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Capture the output from the subprocess and display it in the text widget
        for line in iter(process.stdout.readline, ""):
            output_area.insert(tk.END, line)
            output_area.see(tk.END)  # Scroll to the latest line
            root.update()
        process.stdout.close()
        process.wait()
        
    except Exception as e:
        output_area.insert(tk.END, f"Error: {str(e)}\n")

# Create the main application window
root = tk.Tk()
root.title("BTC Monitor UI")
root.geometry("600x400")

# Create a frame for the configuration inputs
config_frame = tk.Frame(root)
config_frame.pack(pady=10)

# Input fields for configuration
tk.Label(config_frame, text="MIN_BTC_TRANSFER:").grid(row=0, column=0, padx=5, pady=5)
min_btc_entry = tk.Entry(config_frame)
min_btc_entry.grid(row=0, column=1, padx=5, pady=5)
min_btc_entry.insert(0, "50")  # Default value

tk.Label(config_frame, text="MAX_BTC_TRANSFER (None for no max):").grid(row=1, column=0, padx=5, pady=5)
max_btc_entry = tk.Entry(config_frame)
max_btc_entry.grid(row=1, column=1, padx=5, pady=5)
max_btc_entry.insert(0, "None")  # Default value

tk.Label(config_frame, text="TIME_BEFORE_EVENT (seconds):").grid(row=2, column=0, padx=5, pady=5)
time_before_event_entry = tk.Entry(config_frame)
time_before_event_entry.grid(row=2, column=1, padx=5, pady=5)
time_before_event_entry.insert(0, "43200")  # Default value

# Start Button
start_button = tk.Button(root, text="Start BTC Monitor", command=start_btc_monitor)
start_button.pack(pady=10)

# Text area to display the output logs
output_area = scrolledtext.ScrolledText(root, width=70, height=15, wrap=tk.WORD)
output_area.pack(pady=10)

# Start the Tkinter main loop
root.mainloop()
