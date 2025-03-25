#!/usr/bin/env python3
"""
Disk Space Check Tool for SAP/HANA Systems

This module provides tools for checking disk space on SAP/HANA systems,
including filesystem usage and HANA volume information.
"""
import logging
import json
import re
from datetime import datetime
from typing import Dict, Any, List, Optional
import asyncio

from tools.command_executor import execute_command, execute_command_for_system

# Configure logging
logger = logging.getLogger(__name__)

async def check_disk_space(sid: str = None, host: str = None, filesystem: Optional[str] = None, 
                          auth_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Check disk space on OS and filesystems
    
    Args:
        sid (str, optional): SAP System ID (when using SID-based configuration)
        host (str, optional): Target host (when not using SID-based configuration)
        filesystem (str, optional): Optional specific filesystem to check
        auth_context (dict, optional): Authentication context
            
    Returns:
        dict: Disk space information
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and not auth_context.get("permissions", {}).get("OS_VIEW", False):
            if "ADMIN" not in auth_context.get("roles", []):
                return {
                    "status": "error",
                    "message": "Permission denied: OS_VIEW permission required"
                }
        
        # Log the action
        if filesystem:
            if sid:
                logger.info(f"Checking disk space for filesystem {filesystem} on system {sid}")
            else:
                logger.info(f"Checking disk space for filesystem {filesystem} on host {host}")
        else:
            if sid:
                logger.info(f"Checking disk space on system {sid}")
            else:
                logger.info(f"Checking disk space on host {host}")
        
        # Execute df command
        if filesystem:
            command = f"df -h {filesystem}"
        else:
            command = "df -h"
        
        # Execute the command using SID-based approach or direct host
        if sid:
            # Determine available components for this system
            from tools.command_executor import get_system_config
            
            system_config = get_system_config(sid)
            components = system_config.get("components", {})
            
            # Check which components exist
            has_db = "db" in components
            has_app = "app" in components
            
            if not has_db and not has_app:
                error_msg = f"No valid components found for system {sid}"
                logger.error(error_msg)
                return {
                    "status": "error",
                    "message": f"Failed to check disk space: {error_msg}"
                }
            
            # Initialize results
            all_filesystems = []
            
            # Try db component if it exists
            if has_db:
                try:
                    db_result = await execute_command_for_system(sid, "db", command)
                    if db_result["status"] == "success" and db_result["return_code"] == 0:
                        db_filesystems = _parse_df_output(db_result["stdout"])
                        # Add component info to each filesystem
                        for fs in db_filesystems:
                            fs["component"] = "db"
                        all_filesystems.extend(db_filesystems)
                    else:
                        logger.warning(f"Failed to check disk space on db component: {db_result.get('stderr', '')}")
                except Exception as e:
                    logger.warning(f"Error checking disk space on db component: {str(e)}")
            
            # Try app component if it exists
            if has_app:
                try:
                    app_result = await execute_command_for_system(sid, "app", command)
                    if app_result["status"] == "success" and app_result["return_code"] == 0:
                        app_filesystems = _parse_df_output(app_result["stdout"])
                        # Add component info to each filesystem
                        for fs in app_filesystems:
                            fs["component"] = "app"
                        all_filesystems.extend(app_filesystems)
                    else:
                        logger.warning(f"Failed to check disk space on app component: {app_result.get('stderr', '')}")
                except Exception as e:
                    logger.warning(f"Error checking disk space on app component: {str(e)}")
            
            # If we couldn't get any filesystem info, return error
            if not all_filesystems:
                error_msg = "Failed to get filesystem information from any component"
                logger.error(error_msg)
                return {
                    "status": "error",
                    "message": f"Failed to check disk space: {error_msg}"
                }
            
            # Use the collected filesystems
            filesystems = all_filesystems
        else:
            # Use direct host approach
            result = await execute_command(host, command)
            
            # Check for errors
            if result["status"] == "error" or result["return_code"] != 0:
                logger.error(f"Disk space check failed: {result.get('stderr', '')}")
                return {
                    "status": "error",
                    "message": f"Failed to check disk space: {result.get('stderr', '')}"
                }
            
            # Parse the output
            filesystems = _parse_df_output(result["stdout"])
        
        # Get SAP/HANA specific volumes if available
        sap_volumes = []
        try:
            if sid:
                sap_volumes = await _get_sap_hana_volumes(sid=sid)
            else:
                sap_volumes = await _get_sap_hana_volumes(host=host)
        except Exception as e:
            logger.warning(f"Could not get SAP/HANA volumes: {str(e)}")
        
        # Return the structured data
        return {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "filesystems": filesystems,
            "sap_volumes": sap_volumes
        }
    except Exception as e:
        logger.error(f"Error in check_disk_space: {str(e)}")
        return {
            "status": "error",
            "message": f"Error checking disk space: {str(e)}"
        }

def _parse_df_output(output: str) -> List[Dict[str, Any]]:
    """
    Parse df command output
    
    Args:
        output (str): Output from df command
        
    Returns:
        list: List of filesystem data dictionaries
    """
    filesystems = []
    lines = output.strip().split('\n')
    
    # Skip the header line
    if len(lines) > 1:
        for line in lines[1:]:
            parts = line.split()
            
            # Handle different df output formats
            if len(parts) >= 6:
                # Standard format
                filesystem = parts[0]
                size = parts[1]
                used = parts[2]
                available = parts[3]
                use_percent = parts[4]
                mount_point = parts[5]
            elif len(parts) >= 5:
                # Some systems combine filesystem and size
                filesystem = parts[0]
                size = "N/A"
                used = parts[1]
                available = parts[2]
                use_percent = parts[3]
                mount_point = parts[4]
            else:
                # Skip invalid lines
                continue
                
            # Extract numeric values for sorting and alerts
            use_percent_value = int(use_percent.rstrip('%')) if use_percent.rstrip('%').isdigit() else 0
            
            # Determine status based on usage
            status = "normal"
            if use_percent_value >= 90:
                status = "critical"
            elif use_percent_value >= 80:
                status = "warning"
                
            filesystems.append({
                "filesystem": filesystem,
                "size": size,
                "used": used,
                "available": available,
                "use_percent": use_percent,
                "mount_point": mount_point,
                "use_percent_value": use_percent_value,
                "status": status
            })
    
    # Sort by usage percentage (descending)
    filesystems.sort(key=lambda x: x["use_percent_value"], reverse=True)
    
    return filesystems

async def _get_sap_hana_volumes(sid: str = None, host: str = None) -> List[Dict[str, Any]]:
    """
    Get SAP/HANA specific volume information
    
    Args:
        sid (str, optional): SAP System ID (when using SID-based configuration)
        host (str, optional): Target host (when not using SID-based configuration)
        
    Returns:
        list: List of SAP/HANA volume information
    """
    volumes = []
    
    try:
        # Common SAP/HANA directories to check
        sap_dirs = [
            "/usr/sap",
            "/sapmnt",
            "/hana/data",
            "/hana/log",
            "/hana/shared"
        ]
        
        for sap_dir in sap_dirs:
            # Execute df command for each directory
            command = f"df -h {sap_dir} 2>/dev/null || echo 'Not found'"
            
            # Execute the command using SID-based approach or direct host
            if sid:
                # Try db server first, then app server if available
                try:
                    result = await execute_command_for_system(sid, "db", command)
                except ValueError as e:
                    logger.info(f"DB component not found for system {sid}, trying app component: {str(e)}")
                    try:
                        result = await execute_command_for_system(sid, "app", command)
                    except ValueError as e2:
                        logger.warning(f"Could not execute command on any component for system {sid}: {str(e2)}")
                        continue
            else:
                # Use direct host approach
                result = await execute_command(host, command)
            
            # Check for errors
            if result["status"] == "error" or result["return_code"] != 0 or "Not found" in result["stdout"]:
                continue
            
            # Parse the output
            filesystem_info = _parse_df_output(result["stdout"])
            
            # Add only relevant filesystems
            for fs in filesystem_info:
                if sap_dir in fs["mount_point"] or fs["mount_point"] in sap_dir:
                    fs["sap_directory"] = sap_dir
                    volumes.append(fs)
        
        return volumes
    except Exception as e:
        logger.error(f"Error in _get_sap_hana_volumes: {str(e)}")
        return []

async def check_hana_volumes(sid: str = None, host: str = None, 
                            auth_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Check HANA data volumes and their disk usage
    
    Args:
        sid (str, optional): SAP System ID (when using SID-based configuration)
        host (str, optional): Target host (when not using SID-based configuration)
        auth_context (dict, optional): Authentication context
        
    Returns:
        dict: HANA volume information
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and not auth_context.get("permissions", {}).get("HANA_VIEW", False):
            if "ADMIN" not in auth_context.get("roles", []):
                return {
                    "status": "error",
                    "message": "Permission denied: HANA_VIEW permission required"
                }
        
        # Log the action
        if sid:
            logger.info(f"Checking HANA volumes for system {sid}")
        else:
            logger.info(f"Checking HANA volumes on host {host}")
        
        # Get HANA volume sizes using SQL (if possible)
        hana_volumes = []
        try:
            if sid:
                hana_volumes = await check_hana_data_volume_sizes(sid=sid)
            else:
                hana_volumes = await check_hana_data_volume_sizes(host=host)
        except Exception as e:
            logger.warning(f"Failed to get HANA volume sizes: {str(e)}")
        
        # Get general filesystem information
        filesystems = []
        try:
            # Get disk space information for the system
            if sid:
                # Determine available components for this system
                from tools.command_executor import get_system_config
                
                system_config = get_system_config(sid)
                components = system_config.get("components", {})
                
                # Check which components exist
                has_db = "db" in components
                has_app = "app" in components
                
                # Try to get disk space from the available components
                if has_db:
                    try:
                        disk_space_result = await check_disk_space(sid=sid)
                        if disk_space_result.get("status") == "success":
                            filesystems = disk_space_result.get("filesystems", [])
                    except Exception as e:
                        logger.warning(f"Failed to get disk space: {str(e)}")
            else:
                # Use direct host approach
                try:
                    disk_space_result = await check_disk_space(host=host)
                    if disk_space_result.get("status") == "success":
                        filesystems = disk_space_result.get("filesystems", [])
                except Exception as e:
                    logger.warning(f"Failed to get disk space from host {host}: {str(e)}")
        except Exception as e:
            logger.warning(f"Error getting filesystem information: {str(e)}")
        
        # Get SAP/HANA specific volumes
        sap_volumes = []
        try:
            if sid:
                sap_volumes = await _get_sap_hana_volumes(sid=sid)
            else:
                sap_volumes = await _get_sap_hana_volumes(host=host)
        except Exception as e:
            logger.warning(f"Failed to get SAP/HANA volumes: {str(e)}")
        
        # Prepare response
        response = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "system_id": sid if sid else "unknown",
            "hana_volumes": {
                "status": "success" if hana_volumes else "error",
                "message": "HANA volume sizes retrieved successfully" if hana_volumes else "Failed to get HANA volume sizes",
                "volumes": hana_volumes
            },
            "filesystems": filesystems,
            "sap_volumes": sap_volumes
        }
        
        return response
    except Exception as e:
        logger.error(f"Error in check_hana_volumes: {str(e)}")
        return {
            "status": "error",
            "message": f"Failed to check HANA volumes: {str(e)}"
        }

async def check_hana_data_volume_sizes(sid: str, instance_number: str = None, host: str = None) -> Dict[str, Any]:
    """
    Check HANA data volume sizes using HANA SQL
    
    Args:
        sid (str): SAP System ID
        instance_number (str, optional): Instance number (when not using SID-based configuration)
        host (str, optional): Host where HANA is running (when not using SID-based configuration)
        
    Returns:
        dict: HANA volume information
    """
    try:
        # Log the action
        logger.info(f"Checking HANA data volume sizes for {sid}")
        
        # Prepare HANA environment
        sid_lower = sid.lower()
        
        # Get instance number from system config if not provided
        if not instance_number and not host:
            from tools.command_executor import get_system_info
            try:
                system_info = get_system_info(sid, "db")
                instance_number = system_info.get("instance_number", "00")
            except Exception as e:
                logger.warning(f"Could not get instance number from config: {e}")
                instance_number = "00"  # Default to 00
        
        # Create temporary SQL file
        sql_commands = """
        SELECT * FROM M_VOLUME_FILES;
        SELECT * FROM M_DISKS;
        SELECT * FROM M_DATA_VOLUMES;
        """
        
        sql_file = f"/tmp/hana_volumes_{sid_lower}.sql"
        create_file_cmd = f"echo '{sql_commands}' > {sql_file}"
        
        # Create SQL file
        if host:
            result = await execute_command(host, create_file_cmd)
        else:
            result = await execute_command_for_system(sid, "db", create_file_cmd)
            
        if result["status"] == "error" or result["return_code"] != 0:
            return {"status": "error", "message": "Failed to create SQL file", "volumes": []}
        
        # Execute SQL command using HDBSQL
        hdbsql_cmd = f"sudo -u {sid_lower}adm hdbsql -i {instance_number} -d SYSTEMDB -U SYSTEM -A -j -I {sql_file}"
        
        if host:
            result = await execute_command(host, hdbsql_cmd)
        else:
            result = await execute_command_for_system(sid, "db", hdbsql_cmd)
        
        # Clean up SQL file
        cleanup_cmd = f"rm {sql_file}"
        if host:
            await execute_command(host, cleanup_cmd)
        else:
            await execute_command_for_system(sid, "db", cleanup_cmd)
        
        # Check for errors
        if result["status"] == "error" or result["return_code"] != 0:
            logger.warning(f"Failed to get HANA volume sizes: {result.get('stderr', '')}")
            return {"status": "error", "message": "Failed to get HANA volume sizes", "volumes": []}
        
        # Parse the output
        volumes = _parse_hana_sql_output(result["stdout"])
        
        # Return the structured data
        return {
            "status": "success",
            "system_id": sid.upper(),
            "volumes": volumes
        }
    except Exception as e:
        logger.error(f"Error in check_hana_data_volume_sizes: {str(e)}")
        return {"status": "error", "message": f"Error checking HANA data volume sizes: {str(e)}", "volumes": []}

def _parse_hana_sql_output(output: str) -> List[Dict[str, Any]]:
    """
    Parse HANA SQL output for volume information
    
    Args:
        output (str): HANA SQL output
        
    Returns:
        list: Parsed volume data
    """
    volumes = []
    
    try:
        # HDBSQL JSON output might contain multiple JSON objects
        # We need to clean it up and parse each one
        json_parts = output.strip().split('\n')
        parsed_results = []
        
        for part in json_parts:
            if part.strip():
                try:
                    parsed_results.append(json.loads(part))
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse JSON: {part}")
        
        # Process volume files
        volume_files = parsed_results[0] if len(parsed_results) > 0 else []
        disks = parsed_results[1] if len(parsed_results) > 1 else []
        data_volumes = parsed_results[2] if len(parsed_results) > 2 else []
        
        # Combine the information
        for volume in data_volumes:
            volume_info = {
                "volume_id": volume.get("VOLUME_ID", ""),
                "type": volume.get("TYPE", ""),
                "path": volume.get("PATH", ""),
                "size": volume.get("SIZE", 0),
                "used": volume.get("USED_SIZE", 0),
                "free": volume.get("FREE_SIZE", 0)
            }
            
            # Calculate usage percentage
            if volume_info["size"] > 0:
                volume_info["use_percent"] = f"{(volume_info['used'] / volume_info['size']) * 100:.1f}%"
                volume_info["use_percent_value"] = (volume_info["used"] / volume_info["size"]) * 100
            else:
                volume_info["use_percent"] = "N/A"
                volume_info["use_percent_value"] = 0
            
            # Determine status based on usage
            status = "normal"
            if volume_info["use_percent_value"] >= 90:
                status = "critical"
            elif volume_info["use_percent_value"] >= 80:
                status = "warning"
                
            volume_info["status"] = status
            
            volumes.append(volume_info)
        
        # Sort by usage percentage (descending)
        volumes.sort(key=lambda x: x["use_percent_value"], reverse=True)
    except Exception as e:
        logger.warning(f"Error parsing HANA SQL output: {str(e)}")
    
    return volumes

# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Check disk space on SAP/HANA systems")
    parser.add_argument("--sid", help="SAP System ID")
    parser.add_argument("--host", help="Target host")
    parser.add_argument("--filesystem", help="Specific filesystem to check")
    parser.add_argument("--hana-volumes", action="store_true", help="Check HANA volumes")
    
    args = parser.parse_args()
    
    if args.hana_volumes:
        result = asyncio.run(check_hana_volumes(sid=args.sid, host=args.host))
    else:
        result = asyncio.run(check_disk_space(sid=args.sid, host=args.host, filesystem=args.filesystem))
    
    print(json.dumps(result, indent=2))
