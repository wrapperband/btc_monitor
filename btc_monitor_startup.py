import shutil

def btc_monitor_startup(version, rpc_url, min_transfer, max_transfer, time_before_event, debug_level, log_file, batch_on):
    """
    Displays the startup screen for BTC Monitor with a retro ASCII design.

    Args:
        version: Version number of the BTC Monitor.
        rpc_url: The RPC URL for the Bitcoin node.
        min_transfer: Minimum BTC transfer threshold.
        max_transfer: Maximum BTC transfer threshold.
        time_before_event: Time window before the event (in seconds).
        debug_level: Current debug level for logging.
        log_file: Path to the log file.
    """

    # ASCII Art Title for "BTC Monitor"
    ascii_logo = """
    ██████╗ ████████╗ ██████╗        ███╗   ███╗ ██████╗ ███╗   ██╗██╗████████╗ ██████╗ ██████╗   
    ██╔══██╗╚══██╔══╝██╔═══██╗       ████╗ ████║██╔═══██╗████╗  ██║██║╚══██╔══╝██╔═══██╗██╔══██╗  
    ██████╔╝   ██║   ██║             ██╔████╔██║██║   ██║██╔██╗ ██║██║   ██║   ██║   ██║██████╔╝  
    ██╔══██╗   ██║   ██║   ██║       ██║╚██╔╝██║██║   ██║██║╚██╗██║██║   ██║   ██║   ██║██╔══██╗  
    ██████╔╝   ██║   ╚██████╔╝       ██║ ╚═╝ ██║╚██████╔╝██║ ╚████║██║   ██║   ╚██████╔╝██║  ╚██╗
    ╚═════╝    ╚═╝    ╚═════╝        ╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝   ╚═╝    ╚═════╝ ╚═╝   ╚═╝ 
    """

    # Define text that follows the ASCII art
    info_text = f"""
| RPC URL           : {rpc_url}
| Min BTC Transfer  : {min_transfer} BTC
| Max BTC Transfer  : {max_transfer if max_transfer is not None else 'None'}
| Time Before Event : {time_before_event} seconds
| Debug Level       : {debug_level}
| Log File          : {log_file}
| Batch Mode        : {batch_on}
| Quit BTC_Monitor  : Press Ctrl-C at any time.
    """

    # Get terminal size
    terminal_width = shutil.get_terminal_size().columns

    # Function to add the necessary indentation
    def add_indentation(line, indent_size=4):
        return ' ' * indent_size + line

    # Add indentation to all the info_text lines to align them properly
    indented_info_lines = [add_indentation(line) for line in info_text.splitlines()]

    # Center the ASCII logo
    centered_logo_lines = [line.center(terminal_width) for line in ascii_logo.splitlines()]

    # Add centered title and bottom border to the info section
    top_bottom_border = ' ' * 4 + '-' * (terminal_width - 8)
    title_line = ' ' * 4 + f"WELCOME TO BTC MONITOR SYSTEM v{version}".center(terminal_width - 8)

    # Print everything
    print("\n".join(centered_logo_lines))
    print(top_bottom_border)
    print(title_line)
    print(top_bottom_border)
    print("\n".join(indented_info_lines))
    print(top_bottom_border)

