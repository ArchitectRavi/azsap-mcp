#!/usr/bin/env python3
"""
HDB Userstore Management Tool - Set, list and manage HANA database user credentials
"""
import logging
import json
import re
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

class HDBUserstoreTool:
    """Tool for managing HDB user store entries"""
    
    def __init__(self):
        """Initialize the HDB userstore tool"""
        self.executor = CommandExecutor()
        self.auth = Authentication()
    
    def manage_hdbuserstore(self, host, action, sid=None, key=None, username=None, password=None, 
                           database=None, auth_context=None):
        """
        Manage HDB user store
        
        Parameters:
            host (str): Target host
            action (str): Action to perform (set, list, delete)
            sid (str): SAP System ID (to identify the <sid>adm user)
            key (str): User store key name
            username (str): Database username
            password (str): Database password
            database (str): Database hostname and port (HOST:PORT)
            auth_context (dict): Authentication context
            
        Returns:
            dict: Operation result
        """
        # Validate action
        if action.lower() not in ['set', 'list', 'delete']:
            return {
                "status": "error",
                "message": f"Invalid action '{action}'. Use 'set', 'list', or 'delete'"
            }
            
        # Map actions to required permissions
        action_permissions = {
            'set': 'HANA_ADMIN',
            'list': 'HANA_VIEW',
            'delete': 'HANA_ADMIN'
        }
        
        # Verify permissions if auth_context provided
        required_permission = action_permissions.get(action.lower())
        if auth_context and not self.auth.has_permission(auth_context, required_permission):
            return {
                "status": "error",
                "message": f"Insufficient permissions for hdbuserstore {action}"
            }
        
        # Validate parameters based on action
        if action.lower() == 'set':
            if not all([sid, key, username, database]):
                return {
                    "status": "error",
                    "message": "Missing required parameters for SET action. Need sid, key, username, and database"
                }
        elif action.lower() == 'delete':
            if not all([sid, key]):
                return {
                    "status": "error",
                    "message": "Missing required parameters for DELETE action. Need sid and key"
                }
        elif action.lower() == 'list':
            # For list, we need at least SID (can optionally have key for a specific entry)
            if not sid:
                return {
                    "status": "error",
                    "message": "Missing required parameter for LIST action. Need sid"
                }
        
        # Prepare environment and build command
        sid_lower = sid.lower() if sid else None
        
        if action.lower() == 'set':
            # Note: In a production environment, never include passwords in command line
            # This is a significant security risk - use a more secure method
            hdbuserstore_cmd = f"hdbuserstore SET {key} {database} {username} {password}"
        elif action.lower() == 'list':
            hdbuserstore_cmd = f"hdbuserstore LIST"
            if key:
                hdbuserstore_cmd += f" {key}"
        elif action.lower() == 'delete':
            hdbuserstore_cmd = f"hdbuserstore DELETE {key}"
        
        # Execute as <sid>adm user
        command = f"su - {sid_lower}adm -c '{hdbuserstore_cmd}'"
        
        # Log the action (without password)
        safe_command = command.replace(password or "", "********") if password else command
        logger.info(f"Executing hdbuserstore command on {host}: {safe_command}")
        
        # Execute the command
        return_code, stdout, stderr = self.executor.execute_command(host, command, auth_context)
        
        # Check for errors
        if return_code != 0:
            logger.error(f"hdbuserstore {action} failed: {stderr}")
            return {
                "status": "error",
                "message": f"Failed to {action} hdbuserstore: {stderr}"
            }
        
        # Process output based on action
        if action.lower() == 'list':
            parsed_entries = self._parse_hdbuserstore_list(stdout)
            result = {
                "status": "success",
                "action": action.lower(),
                "entries": parsed_entries,
                "timestamp": datetime.now().isoformat()
            }
        else:
            result = {
                "status": "success",
                "action": action.lower(),
                "message": f"hdbuserstore {action} completed successfully",
                "timestamp": datetime.now().isoformat()
            }
            
            # Add additional info for SET
            if action.lower() == 'set':
                result["key"] = key
                result["database"] = database
                result["username"] = username
        
        return result
    
    def _parse_hdbuserstore_list(self, output):
        """Parse hdbuserstore list output"""
        entries = []
        current_entry = None
        
        for line in output.split('\n'):
            line = line.strip()
            
            # Check for key entry
            if line.startswith("KEY "):
                if current_entry:
                    entries.append(current_entry)
                
                key_name = line.split(" ", 1)[1].strip()
                current_entry = {
                    "key": key_name,
                    "ENV": [],
                    "USER": None,
                    "DATABASE": None
                }
            
            # Check for environment, user, or database info
            elif current_entry and ":" in line:
                key, value = line.split(":", 1)
                key = key.strip()
                value = value.strip()
                
                if key == "ENV":
                    current_entry["ENV"].append(value)
                elif key == "USER":
                    current_entry["USER"] = value
                elif key in ["DATABASE", "DATABASENAME"]:
                    current_entry["DATABASE"] = value
        
        # Add the last entry if exists
        if current_entry:
            entries.append(current_entry)
        
        return entries

# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage HDB user store")
    parser.add_argument("--host", default="localhost", help="Target host")
    parser.add_argument("--action", required=True, choices=["set", "list", "delete"], help="Action to perform")
    parser.add_argument("--sid", required=True, help="SAP System ID")
    parser.add_argument("--key", help="User store key name")
    parser.add_argument("--username", help="Database username")
    parser.add_argument("--password", help="Database password")
    parser.add_argument("--database", help="Database connection string (HOST:PORT)")
    parser.add_argument("--auth-username", help="Username for authentication")
    parser.add_argument("--auth-password", help="Password for authentication")
    args = parser.parse_args()
    
    # Create tool instance
    tool = HDBUserstoreTool()
    
    # Authenticate if credentials provided
    auth_context = None
    if args.auth_username and args.auth_password:
        auth = Authentication()
        success, auth_context = auth.authenticate_user(args.auth_username, args.auth_password)
        if not success:
            print("Authentication failed")
            sys.exit(1)
    
    # Manage HDB userstore
    result = tool.manage_hdbuserstore(
        args.host,
        args.action,
        args.sid,
        args.key,
        args.username,
        args.password,
        args.database,
        auth_context
    )
    
    # Print result as JSON
    print(json.dumps(result, indent=2))
