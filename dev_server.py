"""
Development server for SAP HANA MCP with auto-restart functionality.

This script runs the MCP server and automatically restarts it when any Python files
in the project directory are modified.
"""

import os
import sys
import time
import logging
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Path to the main server script
SERVER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'server.py')

class ServerProcess:
    """Manages the server process with restart capability."""
    
    def __init__(self, script_path, args=None):
        self.script_path = script_path
        self.args = args or []
        self.process = None
        self.start()
    
    def start(self):
        """Start the server process."""
        if self.process and self.process.poll() is None:
            logger.info("Terminating existing server process...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning("Server process did not terminate gracefully, forcing kill")
                self.process.kill()
        
        cmd = [sys.executable, self.script_path] + self.args
        logger.info(f"Starting server: {' '.join(cmd)}")
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        # Start a thread to read and log the server output
        import threading
        def log_output():
            for line in self.process.stdout:
                print(line.rstrip())
        
        threading.Thread(target=log_output, daemon=True).start()
    
    def restart(self):
        """Restart the server process."""
        logger.info("Restarting server...")
        self.start()
    
    def stop(self):
        """Stop the server process."""
        if self.process and self.process.poll() is None:
            logger.info("Stopping server...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning("Server process did not terminate gracefully, forcing kill")
                self.process.kill()


class ChangeHandler(FileSystemEventHandler):
    """Handles file system events and restarts the server when Python files change."""
    
    def __init__(self, server_process):
        self.server_process = server_process
        self.last_restart = time.time()
        # Minimum time between restarts to prevent rapid restarts on multiple file changes
        self.restart_cooldown = 2  # seconds
    
    def on_modified(self, event):
        if event.is_directory:
            return
        
        if event.src_path.endswith('.py'):
            current_time = time.time()
            if current_time - self.last_restart > self.restart_cooldown:
                logger.info(f"Detected change in {event.src_path}")
                self.server_process.restart()
                self.last_restart = current_time


def main():
    """Main function to run the development server with auto-restart."""
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Run SAP HANA MCP server with auto-restart')
    parser.add_argument('--debug', action='store_true', help='Run server in debug mode')
    parser.add_argument('--port', type=int, default=3000, help='Port to run server on')
    args = parser.parse_args()
    
    # Build server arguments
    server_args = []
    if args.debug:
        server_args.append('--debug')
    if args.port:
        server_args.extend(['--port', str(args.port)])
    
    # Start the server
    server_process = ServerProcess(SERVER_SCRIPT, server_args)
    
    # Set up file system watcher
    event_handler = ChangeHandler(server_process)
    observer = Observer()
    
    # Watch the current directory and all subdirectories
    path = os.path.dirname(os.path.abspath(__file__))
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    
    logger.info(f"Watching for changes in {path}")
    logger.info("Press Ctrl+C to stop")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping server and watcher...")
        observer.stop()
        server_process.stop()
    
    observer.join()


if __name__ == "__main__":
    main()
