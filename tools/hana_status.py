#!/usr/bin/env python3
"""
HANA Database Status Check Tool

This module provides tools for checking the status of SAP HANA databases,
including service status, version information, and system overview.
"""
import logging
import re
import json
from datetime import datetime
import asyncio
from typing import Dict, Any, List, Optional

from tools.command_executor import execute_command_for_system, execute_command, get_system_info, execute_command_as_sap_user

# Configure logging
logger = logging.getLogger(__name__)

async def check_hana_status(
    sid: str,
    instance_number: str = None,
    host: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Check the status of a HANA database
    
    Args:
        sid (str): SAP System ID
        instance_number (str, optional): HANA instance number. Defaults to None.
        host (str, optional): Hostname or IP address. Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: Status information
    """
    try:
        # Get system information
        system_info = get_system_info(sid)
        if not system_info:
            return {
                "status": "error",
                "message": f"System {sid} not found in configuration"
            }
            
        # Use values from system_info if not provided
        if not instance_number and "instance_number" in system_info:
            instance_number = system_info["instance_number"]
            logger.info(f"Using instance number from config: {instance_number}")
            
        if not host and "hostname" in system_info:
            host = system_info["hostname"]
            logger.info(f"Using hostname from config: {host}")
        
        # Log the check operation
        logger.info(f"Checking HANA status for {sid}")
        
        # Construct the command to check HANA status
        # Try with HDB info first
        command = "HDB info"
        
        # Execute the command as the database admin user
        logger.info(f"Executing command on {host or 'localhost'} as {system_info.get('sap_users', {}).get('dbadm', {}).get('username')}: {command}")
        result = await execute_command_as_sap_user(
            sid=sid,
            component="db",
            command=command,
            sap_user_type="dbadm"
        )
        
        if result["return_code"] == 0:
            # Parse the output to get service status
            services = parse_hdb_info_output(result["stdout"])
            
            # Log the parsed services
            logger.info(f"Parsed services: {services}")
            
            # Determine overall status
            overall_status = "running"
            essential_services = ["hdbnameserver", "hdbindexserver"]
            missing_essential = [svc for svc in essential_services if not any(s["name"] == svc for s in services)]
            
            if missing_essential:
                overall_status = "partial"
                logger.warning(f"Missing essential services: {missing_essential}")
            
            if not services:
                overall_status = "stopped"
                logger.warning("No HANA services found running")
            
            # Return the status information
            return {
                "status": "success",
                "hana_status": services,
                "overall_status": overall_status,
                "instance_number": instance_number,
                "sid": sid,
                "host": host,
                "raw_output": result["stdout"]
            }
        else:
            # If the command failed, try different commands to check HANA status
            commands = [
                # Try with full paths
                f"/usr/sap/{sid.upper()}/HDB{instance_number}/HDB info",
                f"/hana/shared/{sid.lower()}/hdbclient/HDB info",
                # Try with sourcing the environment first
                "source ~/.bashrc && HDB info",
                "source ~/.profile && HDB info"
            ]
            
            # Try each command until one succeeds
            success = False
            result_output = ""
            
            for command in commands:
                logger.info(f"Executing command on {host or 'localhost'} as {system_info.get('sap_users', {}).get('dbadm', {}).get('username')}: {command}")
                
                try:
                    result = await execute_command_as_sap_user(
                        sid=sid,
                        component="db",
                        command=command,
                        sap_user_type="dbadm",
                        timeout=60
                    )
                    
                    if result["return_code"] == 0:
                        success = True
                        result_output = result["stdout"]
                        logger.info(f"Command succeeded: {command}")
                        break
                    else:
                        logger.warning(f"Command failed with return code {result['return_code']}: {command}")
                        logger.warning(f"Error output: {result['stderr']}")
                except Exception as e:
                    logger.error(f"Error executing command: {str(e)}")
            
            if not success:
                # If all commands failed, try to list the directories to help diagnose
                try:
                    list_result = await execute_command_as_sap_user(
                        sid=sid,
                        component="db",
                        command="ls -la /usr/sap/",
                        sap_user_type="dbadm",
                        timeout=30
                    )
                    
                    logger.info(f"SAP directories: {list_result.get('stdout', '')}")
                    
                    # Also try to check if the user can run HDB command at all
                    check_result = await execute_command_as_sap_user(
                        sid=sid,
                        component="db",
                        command="which HDB || echo HDB not found",
                        sap_user_type="dbadm",
                        timeout=30
                    )
                    
                    logger.info(f"HDB command check: {check_result.get('stdout', '')}")
                except Exception as e:
                    logger.error(f"Error checking directories: {str(e)}")
                
                return {
                    "status": "error",
                    "message": f"Failed to get HANA status: All commands failed. Check if HDB command is available."
                }
            
            if success:
                # Parse the output to get service status
                services = parse_hdb_info_output(result_output)
                
                # Log the parsed services
                logger.info(f"Parsed services: {services}")
                
                # Determine overall status
                overall_status = "running"
                essential_services = ["hdbnameserver", "hdbindexserver"]
                missing_essential = [svc for svc in essential_services if not any(s["name"] == svc for s in services)]
                
                if missing_essential:
                    overall_status = "partial"
                    logger.warning(f"Missing essential services: {missing_essential}")
                
                if not services:
                    overall_status = "stopped"
                    logger.warning("No HANA services found running")
                
                # Return the status information
                return {
                    "status": "success",
                    "hana_status": services,
                    "overall_status": overall_status,
                    "instance_number": instance_number,
                    "sid": sid,
                    "host": host,
                    "raw_output": result_output
                }
        
        # If we reach this point, something went wrong
        return {
            "status": "error",
            "message": "Failed to get HANA status: Unknown error"
        }
        
    except Exception as e:
        logger.error(f"Error in check_hana_status: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        return {
            "status": "error",
            "message": f"Failed to get HANA status: {str(e)}"
        }

async def get_hana_version(
    sid: str,
    instance_number: str = None,
    host: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Get the version of a HANA database
    
    Args:
        sid (str): SAP System ID
        instance_number (str, optional): HANA instance number. Defaults to None.
        host (str, optional): Hostname or IP address. Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: Version information
    """
    try:
        # Get system information
        system_info = get_system_info(sid)
        if not system_info:
            return {
                "status": "error",
                "message": f"System {sid} not found in configuration"
            }
            
        # Use values from system_info if not provided
        if not instance_number and "instance_number" in system_info:
            instance_number = system_info["instance_number"]
            logger.info(f"Using instance number from config: {instance_number}")
            
        if not host and "hostname" in system_info:
            host = system_info["hostname"]
            logger.info(f"Using hostname from config: {host}")
        
        # Log the check operation
        logger.info(f"Getting HANA version for {sid}")
        
        # Try multiple methods to get the HANA version
        version_info = None
        error_messages = []
        
        # Construct the full path to HDB command
        hdb_path = f"/usr/sap/{sid.upper()}/HDB{instance_number}"
        
        # Method 1: Try using HDB version command
        try:
            version_cmd = f"{hdb_path}/HDB version"
            version_result = await execute_command_as_sap_user(
                sid=sid,
                component="db",
                command=version_cmd,
                sap_user_type="dbadm",
                timeout=30
            )
            
            if version_result["return_code"] == 0:
                # Parse the output to get version information
                version_info = parse_hdb_version_output(version_result["stdout"])
                if version_info:
                    return {
                        "status": "success",
                        "version": version_info.get("version", "Unknown"),
                        "version_info": version_info
                    }
            else:
                error_messages.append(f"HDB version command failed: {version_result.get('stderr', '')}")
        except Exception as e:
            error_messages.append(f"Error executing HDB version: {str(e)}")
        
        # Method 2: Try using SQL query
        try:
            # Create SQL command to get version
            sql_command = "SELECT * FROM M_DATABASE"
            
            # Create a temporary file for the SQL command
            temp_file = f"/tmp/hana_version_{sid.lower()}.sql"
            
            # Write SQL command to temporary file using the SAP user
            cmd_result = await execute_command_as_sap_user(
                sid=sid,
                component="db",
                command=f"echo '{sql_command}' > {temp_file}",
                sap_user_type="dbadm",
                timeout=30
            )
            
            if cmd_result["return_code"] == 0:
                # Execute SQL command using hdbsql
                hdbsql_cmd = f"hdbsql -i {instance_number} -d SYSTEMDB -U SYSTEM -A -j -I {temp_file}"
                
                result = await execute_command_as_sap_user(
                    sid=sid,
                    component="db",
                    command=hdbsql_cmd,
                    sap_user_type="dbadm",
                    timeout=30
                )
                
                # Clean up temporary file
                try:
                    cleanup_result = await execute_command_as_sap_user(
                        sid=sid,
                        component="db",
                        command=f"rm {temp_file}",
                        sap_user_type="dbadm",
                        timeout=30
                    )
                except Exception as e:
                    logger.warning(f"Failed to clean up temporary file: {str(e)}")
                
                if result["return_code"] == 0:
                    # Parse the SQL output
                    try:
                        sql_output = result["stdout"]
                        import json
                        db_info = json.loads(sql_output)
                        
                        if isinstance(db_info, list) and len(db_info) > 0:
                            db_record = db_info[0]
                            version_info = {
                                "version": db_record.get("VERSION", "Unknown"),
                                "database_name": db_record.get("DATABASE_NAME", "Unknown"),
                                "host": db_record.get("HOST", "Unknown"),
                                "start_time": db_record.get("START_TIME", "Unknown"),
                                "usage": db_record.get("USAGE", "Unknown")
                            }
                            
                            return {
                                "status": "success",
                                "version": version_info["version"],
                                "version_info": version_info
                            }
                    except Exception as e:
                        error_messages.append(f"Failed to parse SQL output: {str(e)}")
                else:
                    error_messages.append(f"SQL query failed: {result.get('stderr', '')}")
            else:
                error_messages.append(f"Failed to create SQL file: {cmd_result.get('stderr', '')}")
        except Exception as e:
            error_messages.append(f"Error executing SQL query: {str(e)}")
        
        # Method 3: Try using HDB info command to extract version from service information
        try:
            info_cmd = f"{hdb_path}/HDB info"
            info_result = await execute_command_as_sap_user(
                sid=sid,
                component="db",
                command=info_cmd,
                sap_user_type="dbadm",
                timeout=30
            )
            
            if info_result["return_code"] == 0:
                # Extract version information from HDB info output if possible
                info_output = info_result["stdout"]
                if "version" in info_output.lower():
                    import re
                    version_match = re.search(r'version\s*:\s*(\d+\.\d+\.\d+)', info_output, re.IGNORECASE)
                    if version_match:
                        version = version_match.group(1)
                        version_info = {"version": version}
                        return {
                            "status": "success",
                            "version": version,
                            "version_info": version_info
                        }
            else:
                error_messages.append(f"HDB info command failed: {info_result.get('stderr', '')}")
        except Exception as e:
            error_messages.append(f"Error executing HDB info: {str(e)}")
        
        # If we get here, all methods failed
        return {
            "status": "error",
            "message": "Failed to get HANA version",
            "details": error_messages
        }
    except Exception as e:
        logger.error(f"Error in get_hana_version: {str(e)}")
        return {
            "status": "error",
            "message": f"Error getting HANA version: {str(e)}"
        }

def parse_hdb_version_output(output: str) -> Dict[str, str]:
    """
    Parse HDB version output to extract version information
    
    Args:
        output (str): Output from HDB version command
        
    Returns:
        Dict[str, str]: Version information
    """
    version_info = {"version": "Unknown"}
    
    # Try to extract version using regex
    version_match = re.search(r'version:\s+([0-9.]+)', output, re.IGNORECASE)
    if version_match:
        version_info["version"] = version_match.group(1)
    
    # Extract other information if available
    patch_match = re.search(r'patch\s+number:\s+([0-9.]+)', output, re.IGNORECASE)
    if patch_match:
        version_info["patch"] = patch_match.group(1)
    
    revision_match = re.search(r'revision:\s+([0-9.]+)', output, re.IGNORECASE)
    if revision_match:
        version_info["revision"] = revision_match.group(1)
    
    return version_info

def parse_hdb_info_output(output: str) -> List[Dict[str, Any]]:
    """
    Parse HDB info output to extract service status
    
    Args:
        output (str): Output from HDB info command
        
    Returns:
        list: List of service dictionaries with name, status, etc.
    """
    services = []
    
    # Skip empty output
    if not output or output.strip() == "":
        return services
    
    # Check if the output is a process listing (ps command output)
    if "USER" in output and "PID" in output and "COMMAND" in output:
        # Process the ps output format
        lines = output.splitlines()
        
        # Skip the header line
        for line in lines[1:]:
            parts = line.split()
            if len(parts) < 7:  # Ensure we have enough parts
                continue
                
            # Extract the command part (which might contain spaces)
            command_part = " ".join(parts[6:])
            
            # Look for HANA services
            hana_services = [
                "hdbnameserver", "hdbindexserver", "hdbcompileserver", 
                "hdbpreprocessor", "hdbwebdispatcher", "hdbxsengine",
                "hdbdpserver", "hdbdocstore", "hdbscriptserver", "hdbdiserver"
            ]
            
            for service_name in hana_services:
                if service_name in command_part:
                    # Extract the port if available
                    port = None
                    port_match = re.search(r'-port (\d+)', command_part)
                    if port_match:
                        port = port_match.group(1)
                    
                    services.append({
                        "name": service_name,
                        "status": "running",  # If it's in the process list, it's running
                        "pid": parts[1],
                        "port": port,
                        "details": {
                            "user": parts[0],
                            "cpu": parts[3],
                            "memory": parts[5]  # RSS value
                        }
                    })
                    break
    else:
        # Process the standard HDB info output format
        current_service = None
        
        for line in output.splitlines():
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
                
            # Check for service headers (they start with hdb...)
            if line.startswith('hdb'):
                # If we were processing a service, add it to the list
                if current_service:
                    services.append(current_service)
                    
                # Start a new service
                parts = line.split()
                if len(parts) >= 2:
                    service_name = parts[0]
                    status = parts[1]
                    
                    current_service = {
                        "name": service_name,
                        "status": status,
                        "details": {}
                    }
            
            # Process detail lines (they have a key: value format)
            elif current_service and ':' in line:
                key, value = line.split(':', 1)
                current_service["details"][key.strip()] = value.strip()
        
        # Add the last service if we have one
        if current_service:
            services.append(current_service)
    
    return services

async def _get_hana_version(
    sid: str,
    instance_number: str = None,
    host: str = None
) -> str:
    """
    Get HANA database version
    
    Args:
        sid (str): SAP System ID
        instance_number (str, optional): Instance number (if not using SID-based config)
        host (str, optional): Host where HANA is running (if not using SID-based config)
        
    Returns:
        str: HANA version string
    """
    sid_lower = sid.lower()
    command = f"sudo -u {sid_lower}adm HDB version"
    
    # Execute the command using SID-based approach
    if host:
        # Legacy approach with direct host
        result = await execute_command(host, command)
    else:
        # New SID-based approach
        result = await execute_command_for_system(sid, "db", command)
    
    if result["status"] == "error" or result["return_code"] != 0:
        logger.warning(f"Failed to get HANA version: {result.get('stderr', '')}")
        return "Unknown"
    
    # Parse version info
    version_match = re.search(r'version:\s+(\d+\.\d+\.\d+)', result["stdout"])
    if version_match:
        return version_match.group(1)
    
    return "Unknown"

async def _get_hana_sql_status(
    sid: str,
    instance_number: str = None,
    host: str = None
) -> Dict[str, Any]:
    """
    Get HANA status information using SQL queries
    
    Args:
        sid (str): SAP System ID
        instance_number (str, optional): Instance number (if not using SID-based config)
        host (str, optional): Host where HANA is running (if not using SID-based config)
        
    Returns:
        dict: HANA SQL status information
    """
    try:
        # Get system information
        system_info = get_system_info(sid)
        if not system_info:
            logger.error(f"System {sid} not found in configuration")
            return {}
            
        # Use values from system_info if not provided
        if not instance_number and "instance_number" in system_info:
            instance_number = system_info["instance_number"]
            
        if not host and "hostname" in system_info:
            host = system_info["hostname"]
        
        # Create SQL commands
        sql_commands = [
            "SELECT * FROM M_HOST_INFORMATION;",
            "SELECT * FROM M_SYSTEM_OVERVIEW;",
            "SELECT * FROM M_SERVICE_STATISTICS;",
            "SELECT * FROM M_CONNECTIONS;",
            "SELECT * FROM M_CS_TABLES WHERE USED_MEMORY_SIZE > 1000000 ORDER BY USED_MEMORY_SIZE DESC LIMIT 10;",
            "SELECT * FROM M_DISK_USAGE;",
            "SELECT * FROM M_SERVICE_MEMORY;"
        ]
        
        # Create a temporary file for the SQL commands
        temp_file = f"/tmp/hana_status_{sid.lower()}.sql"
        
        # Write SQL commands to temporary file
        cmd_result = await execute_command(
            host=host,
            command=f"echo '{chr(10).join(sql_commands)}' > {temp_file}"
        )
        
        if cmd_result["return_code"] != 0:
            logger.error(f"Failed to create temporary SQL file: {cmd_result['stderr']}")
            return {}
        
        # Execute SQL commands using hdbsql
        hdbsql_cmd = f"hdbsql -i {instance_number} -d SYSTEMDB -U SYSTEM -A -j -I {temp_file}"
        
        result = await execute_command_as_sap_user(
            sid=sid,
            component="db",
            command=hdbsql_cmd,
            sap_user_type="dbadm",
            timeout=60
        )
        
        # Clean up temporary file
        cleanup_result = await execute_command(
            host=host,
            command=f"rm {temp_file}"
        )
        
        if result["return_code"] != 0:
            logger.error(f"Failed to execute SQL commands: {result['stderr']}")
            return {}
        
        # Parse SQL output (JSON format)
        try:
            # Split the output into separate JSON objects
            sql_output = result["stdout"]
            parts = sql_output.strip().split('\n\n')
            
            # Parse each part
            sql_results = {}
            
            for i, part in enumerate(parts):
                if not part.strip():
                    continue
                    
                try:
                    data = json.loads(part)
                    if i == 0:
                        sql_results["host_info"] = data
                    elif i == 1:
                        sql_results["system_overview"] = data
                    elif i == 2:
                        sql_results["service_statistics"] = data
                    elif i == 3:
                        sql_results["connections"] = data
                    elif i == 4:
                        sql_results["memory_tables"] = data
                    elif i == 5:
                        sql_results["disk_usage"] = data
                    elif i == 6:
                        sql_results["service_memory"] = data
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse part {i} as JSON")
            
            return sql_results
        except Exception as e:
            logger.error(f"Error in _get_hana_sql_status: {str(e)}")
            return {}
    except Exception as e:
        logger.error(f"Error in _get_hana_sql_status: {str(e)}")
        return {}

async def get_hana_service_status(
    sid: str,
    instance_number: str = None,
    host: str = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Get only the service status for a HANA instance
    
    Args:
        sid (str): SAP System ID
        instance_number (str, optional): Instance number (if not using SID-based config)
        host (str, optional): Host where HANA is running (if not using SID-based config)
        auth_context (dict, optional): Authentication context for SSH connection
    
    Returns:
        dict: HANA service status information
    """
    # Check permissions if auth_context is provided
    if auth_context and not auth_context.get("permissions", {}).get("HANA_VIEW", False):
        if "ADMIN" not in auth_context.get("roles", []):
            return {
                "status": "error",
                "message": "Permission denied: HANA_VIEW permission required"
            }
    
    try:
        # Log the action
        logger.info(f"Getting HANA service status for {sid}")
        
        # Prepare HANA environment
        sid_lower = sid.lower()
        
        # Get HANA process status
        command = f"sudo -u {sid_lower}adm HDB info"
        
        # Execute the command using SID-based approach
        if host:
            # Legacy approach with direct host
            result = await execute_command(host, command)
        else:
            # New SID-based approach
            result = await execute_command_for_system(sid, "db", command)
        
        # Check for errors
        if result["status"] == "error" or result["return_code"] != 0:
            logger.error(f"HANA service status check failed: {result.get('stderr', '')}")
            return {
                "status": "error",
                "message": f"Failed to get HANA service status: {result.get('stderr', '')}"
            }
        
        # Parse the output
        services = parse_hdb_info_output(result["stdout"])
        
        # Return simplified status
        return {
            "status": "success",
            "system_id": sid.upper(),
            "services": services,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error in get_hana_service_status: {str(e)}")
        return {
            "status": "error",
            "message": f"Error checking HANA service status: {str(e)}"
        }

# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Check HANA Database Status")
    parser.add_argument("--sid", required=True, help="SAP System ID")
    parser.add_argument("--instance", default=None, help="Instance number")
    parser.add_argument("--host", default=None, help="Host where HANA is running")
    
    args = parser.parse_args()
    
    # Run the status check
    result = asyncio.run(check_hana_status(args.sid, args.instance, args.host))
    
    # Print the result
    print(json.dumps(result, indent=2))
