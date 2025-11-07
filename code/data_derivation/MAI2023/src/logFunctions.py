import psutil
import os
import logging

def setup_logging():
    """Set up the logging configuration."""
    logging.basicConfig(filename='memory_usage.log', level=logging.INFO, 
                        format='%(asctime)s %(levelname)s:%(message)s', 
                        filemode='w')

def log_memory_usage():
    """
    Logs the memory usage of the current process and its children to a file.
    """
    process = psutil.Process(os.getpid())
    # Get memory usage data
    mem = process.memory_full_info()
    # Calculate memory usage in megabytes
    memory_usage = mem.uss / (1024 ** 2)
    logging.info(f"Current memory usage: {memory_usage:.2f} MB")

    # Also consider logging memory usage of child processes if needed
    for child in process.children(recursive=True):
        child_mem = child.memory_full_info()
        child_memory_usage = child_mem.uss / (1024 ** 2)
        logging.info(f"Child process ID {child.pid} memory usage: {child_memory_usage:.2f} MB")