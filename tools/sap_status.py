#!/usr/bin/env python3
"""
SAP System Status Check Tool
"""
import logging
import re
import json
from datetime import datetime
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from core.command_executor import CommandExecutor
from auth.authentication import Authentication

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SAPStatusTool:
    """Tool for checking SAP system status"""
    
    def __init__(self):
        """Initialize the SAP status tool"""
        self.executor = CommandExecutor()
        self.auth = Authentication()
        
    def check_sap_status(self, sid, instance_number, host, auth_context=None):
        """
        Check SAP system status using sapcontrol
        
        Parameters:
            sid (str): SAP System ID
            instance_number (str): Instance number
            host (str): Host where system is running
            auth_context (dict): Authentication context
            
        Returns:
            dict: System status information
        """
        # Verify permissions if auth_context provided
        if auth_context and not self.auth.has_permission(auth_context, "SAP_VIEW"):
            return {
                "status": "error",
                "message": "Insufficient permissions to view SAP status"
            }
        
        # Log the action
        logger.info(f"Checking SAP status for {sid} on {host}")
        
        # Prepare SAP environment
        sid_upper = sid.upper()
        sid_lower = sid.lower()
        
        # Execute sapcontrol command
        sapcontrol_cmd = f"sapcontrol -nr {instance_number} -function GetSystemInstanceList"
        
        # If using <sid>adm user, wrap command
        command = f"su - {sid_lower}adm -c '{sapcontrol_cmd}'"
        
        # Execute the command
        return_code, stdout, stderr = self.executor.execute_command(host, command, auth_context)
        
        # Check for errors
        if return_code != 0:
            logger.error(f"SAP status check failed: {stderr}")
            return {
                "status": "error",
                "message": f"Failed to get SAP status: {stderr}"
            }
        
        # Parse the output
        instances = []
        instance_pattern = re.compile(r'^(\d+):.*')
        
        for line in stdout.split('\n'):
            if instance_pattern.match(line):
                parts = line.split(',')
                if len(parts) >= 5:
                    instance = {
                        "hostname": parts[0].split(':')[1].strip(),
                        "instance": parts[1].strip(),
                        "features": parts[2].strip(),
                        "dispstatus": parts[3].strip(),
                        "pid": parts[4].strip()
                    }
                    instances.append(instance)
        
        # Return the structured data
        return {
            "status": "success",
            "system_id": sid_upper,
            "instances": instances,
            "timestamp": datetime.now().isoformat()
        }
