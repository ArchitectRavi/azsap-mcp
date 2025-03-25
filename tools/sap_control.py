#!/usr/bin/env python3
"""
SAP System Control Tool (Start/Stop)
"""
import logging
import json
from datetime import datetime
from pathlib import Path
import sys
import time

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from core.command_executor import CommandExecutor
from auth.authentication import Authentication
from tools.sap_status import SAPStatusTool

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SAPControlTool:
    """Tool for starting and stopping SAP systems"""
    
    def __init__(self):
        """Initialize the SAP control tool"""
        self.executor = CommandExecutor()
        self.auth = Authentication()
        self.status_tool = SAPStatusTool()
        
    def manage_sap_system(self, sid, instance_number, host, action, auth_context=None, wait=True, timeout=300):
        """
        Start, stop or restart SAP system
        
        Parameters:
            sid (str): SAP System ID
            instance_number (str): Instance number
            host (str): Host where system is running
            action (str): Action to perform: 'start', 'stop', or 'restart'
            auth_context (dict): Authentication context
            wait (bool): Whether to wait for action completion
            timeout (int): Maximum time to wait in seconds
            
        Returns:
            dict: Operation result
        """
        # Validate action
        if action.lower() not in ['start', 'stop', 'restart']:
            return {
                "status": "error",
                "message": "Invalid action. Use 'start', 'stop', or 'restart'"
            }
        
        action = action.lower()
        
        # Verify permissions based on action
        permission = f"SAP_{action.upper()}"
        if auth_context and not self.auth.has_permission(auth_context, permission):
            return {
                "status": "error",
                "message": f"Insufficient permissions to {action} SAP system"
            }
        
        # Log the action
        logger.info(f"{action.capitalize()}ing SAP system {sid} on {host}")
        
        # Prepare SAP environment
        sid_upper = sid.upper()
        sid_lower = sid.lower()
        
        # Determine sapcontrol function based on action
        if action == 'restart':
            sapcontrol_function = "RestartSystem"
        else:
            # First letter uppercase for sapcontrol functions
            sapcontrol_function = f"{action.capitalize()}System"
        
        # Build sapcontrol command
        sapcontrol_cmd = f"sapcontrol -nr {instance_number} -function {sapcontrol_function}"
        
        # Wrap with <sid>adm user
        command = f"su - {sid_lower}adm -c '{sapcontrol_cmd}'"
        
        # Execute the command
        return_code, stdout, stderr = self.executor.execute_command(host, command, auth_context)
        
        # Check for errors
        if return_code != 0:
            logger.error(f"SAP {action} failed: {stderr}")
            return {
                "status": "error",
                "message": f"Failed to {action} SAP system: {stderr}"
            }
        
        # Wait for system to reach desired state if requested
        result = {
            "status": "success",
            "message": f"SAP system {action} initiated successfully",
            "system_id": sid_upper,
            "instance": instance_number,
            "host": host,
            "timestamp": datetime.now().isoformat()
        }
        
        if wait:
            expected_status = "GREEN" if action == "start" else "GRAY"
            wait_result = self._wait_for_status(sid, instance_number, host, expected_status, auth_context, timeout)
            result.update(wait_result)
        
        return result
    
    def _wait_for_status(self, sid, instance_number, host, expected_status, auth_context, timeout):
        """Wait for SAP system to reach expected status"""
        logger.info(f"Waiting for SAP system {sid} to reach {expected_status} status")
        
        start_time = time.time()
        interval = 10  # Check every 10 seconds
        
        while (time.time() - start_time) < timeout:
            # Get current status
            status_result = self.status_tool.check_sap_status(sid, instance_number, host, auth_context)
            
            if status_result.get("status") == "error":
                logger.warning(f"Failed to check status while waiting: {status_result.get('message')}")
                time.sleep(interval)
                continue
            
            # Check if all instances have reached the expected status
            instances = status_result.get("instances", [])
            all_expected = True
            
            for instance in instances:
                if instance.get("dispstatus") != expected_status:
                    all_expected = False
                    break
            
            if all_expected:
                return {
                    "wait_status": "success",
                    "wait_message": f"SAP system reached {expected_status} status",
                    "instances": instances
                }
            
            # Sleep before next check
            time.sleep(interval)
        
        # Timeout occurred
        return {
            "wait_status": "timeout",
            "wait_message": f"Timeout waiting for SAP system to reach {expected_status} status",
            "current_status": self.status_tool.check_sap_status(sid, instance_number, host, auth_context)
        }


# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Control SAP system (start/stop)")
    parser.add_argument("--sid", required=True, help="SAP System ID")
    parser.add_argument("--instance", required=True, help="Instance number")
    parser.add_argument("--host", default="localhost", help="Target host")
    parser.add_argument("--action", required=True, choices=["start", "stop", "restart"], help="Action to perform")
    parser.add_argument("--nowait", action="store_true", help="Don't wait for action completion")
    parser.add_argument("--timeout", type=int, default=300, help="Maximum wait time in seconds")
    parser.add_argument("--username", help="Username for authentication")
    parser.add_argument("--password", help="Password for authentication")
    args = parser.parse_args()
    
    # Create tool instance
    tool = SAPControlTool()
    
    # Authenticate if credentials provided
    auth_context = None
    if args.username and args.password:
        auth = Authentication()
        success, auth_context = auth.authenticate_user(args.username, args.password)
        if not success:
            print("Authentication failed")
            sys.exit(1)
    
    # Perform SAP system operation
    result = tool.manage_sap_system(
        args.sid, 
        args.instance, 
        args.host, 
        args.action, 
        auth_context,
        not args.nowait,
        args.timeout
    )
    
    # Print result as JSON
    print(json.dumps(result, indent=2))
