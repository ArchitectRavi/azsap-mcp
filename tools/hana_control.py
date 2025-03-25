#!/usr/bin/env python3
"""
HANA Database Control Tool (Start/Stop)

This module provides tools for controlling SAP HANA databases,
including starting, stopping, and restarting HANA instances.
"""
import logging
import json
from datetime import datetime
import time
import asyncio
from typing import Dict, Any, List, Optional

from tools.command_executor import (
    execute_command, 
    execute_command_for_system, 
    execute_command_as_sap_user,
    get_system_info
)
from tools.hana_status import check_hana_status

# Configure logging
logger = logging.getLogger(__name__)

async def manage_hana_system(
    sid: str,
    instance_number: str = None,
    host: str = None,
    action: str = None,
    auth_context: Optional[Dict[str, Any]] = None,
    wait: bool = True,
    timeout: int = 300
) -> Dict[str, Any]:
    """
    Start, stop or restart HANA database
    
    Args:
        sid (str): SAP System ID
        instance_number (str, optional): Instance number (when not using SID-based configuration)
        host (str, optional): Host where HANA is running (when not using SID-based configuration)
        action (str): Action to perform: 'start', 'stop', or 'restart'
        auth_context (dict, optional): Authentication context
        wait (bool): Whether to wait for action completion
        timeout (int): Maximum time to wait in seconds
        
    Returns:
        dict: Operation result
    """
    try:
        # Validate action
        if action is None or action.lower() not in ['start', 'stop', 'restart']:
            return {
                "status": "error",
                "message": "Invalid action. Use 'start', 'stop', or 'restart'"
            }
        
        action = action.lower()
        
        # Check permissions if auth_context is provided
        if auth_context:
            required_permission = None
            if action == "start":
                required_permission = "HANA_START"
            elif action == "stop":
                required_permission = "HANA_STOP"
            elif action == "restart":
                required_permission = "HANA_RESTART"
                
            if required_permission and not auth_context.get("permissions", {}).get(required_permission, False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": f"Permission denied: {required_permission} permission required"
                    }
        
        # Get instance number and host from system config if not provided
        if not instance_number or not host:
            try:
                system_info = get_system_info(sid, "db")
                if not instance_number:
                    instance_number = system_info.get("instance_number", "00")
                if not host:
                    host = system_info.get("hostname")
                    
                if not host:
                    return {
                        "status": "error",
                        "message": f"Could not determine host for SID {sid}"
                    }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Error retrieving system information for SID {sid}: {e}"
                }
        
        # Log the action
        logger.info(f"Managing HANA system {sid} on {host}: {action}")
        
        # Perform the requested action
        if action == 'start':
            result = await start_hana(sid, instance_number, host, wait, timeout)
        elif action == 'stop':
            result = await stop_hana(sid, instance_number, host, wait, timeout)
        elif action == 'restart':
            result = await restart_hana(sid, instance_number, host, wait, timeout)
        
        return result
    except Exception as e:
        logger.error(f"Error in manage_hana_system: {e}")
        return {
            "status": "error",
            "message": f"Error managing HANA system: {e}"
        }

async def _wait_for_status(
    sid: str,
    instance_number: str,
    host: str,
    expected_status: str,
    timeout: int,
    action: str
) -> Dict[str, Any]:
    """
    Wait for HANA to reach expected status
    
    Args:
        sid (str): SAP System ID
        instance_number (str): Instance number
        host (str): Host where HANA is running
        expected_status (str): Expected status to wait for (RUNNING, STOPPED)
        timeout (int): Maximum time to wait in seconds
        action (str): Action being performed (start, stop, restart)
        
    Returns:
        dict: Operation result
    """
    start_time = time.time()
    interval = 5  # Check every 5 seconds
    
    logger.info(f"Waiting for HANA {sid} to reach status '{expected_status}' (timeout: {timeout}s)")
    
    while time.time() - start_time < timeout:
        try:
            # Check current status
            status_result = await check_hana_status(sid, instance_number, host)
            
            current_status = status_result.get("status")
            logger.debug(f"Current HANA status: {current_status}")
            
            # Check if we've reached the expected status
            if current_status == expected_status:
                return {
                    "status": "success",
                    "message": f"HANA {action} completed successfully. Current status: {current_status}"
                }
                
            # Wait before checking again
            await asyncio.sleep(interval)
            
        except Exception as e:
            logger.error(f"Error checking HANA status: {e}")
            # Continue waiting, as the error might be temporary
    
    # Timeout reached
    return {
        "status": "error",
        "message": f"Timeout waiting for HANA to {action}. Last known status: {status_result.get('status', 'unknown')}"
    }

async def start_hana(
    sid: str,
    instance_number: str = None,
    host: str = None,
    wait: bool = True,
    timeout: int = 300
) -> Dict[str, Any]:
    """
    Start HANA database
    
    Args:
        sid (str): SAP System ID
        instance_number (str, optional): Instance number (when not using SID-based configuration)
        host (str, optional): Host where HANA is running (when not using SID-based configuration)
        wait (bool): Whether to wait for HANA to start
        timeout (int): Maximum time to wait in seconds
        
    Returns:
        dict: Operation result
    """
    try:
        # Get system info if not provided
        if not instance_number or not host:
            system_info = get_system_info(sid, "db")
            if not instance_number:
                instance_number = system_info.get("instance_number", "00")
            if not host:
                host = system_info.get("hostname")
                
            if not host:
                return {
                    "status": "error",
                    "message": f"Could not determine host for SID {sid}"
                }
        
        # Log the action
        logger.info(f"Starting HANA database {sid} on {host}")
        
        # Construct the full path to HDB command
        hdb_path = f"/usr/sap/{sid.upper()}/HDB{instance_number}"
        
        # Execute HDB start command as dbadm user
        start_command = f"{hdb_path}/HDB start"
        result = await execute_command_as_sap_user(sid, "db", start_command, sap_user_type="dbadm")
        
        if result["status"] == "error":
            return {
                "status": "error",
                "message": f"Error starting HANA: {result.get('stderr', '')}"
            }
        
        # Wait for HANA to start if requested
        if wait:
            wait_result = await _wait_for_status(sid, instance_number, host, "RUNNING", timeout, "start")
            return wait_result
        
        return {
            "status": "success",
            "message": f"HANA start command executed successfully for {sid}"
        }
    except Exception as e:
        logger.error(f"Error starting HANA: {e}")
        return {
            "status": "error",
            "message": f"Error starting HANA: {e}"
        }

async def stop_hana(
    sid: str,
    instance_number: str = None,
    host: str = None,
    wait: bool = True,
    timeout: int = 300
) -> Dict[str, Any]:
    """
    Stop HANA database
    
    Args:
        sid (str): SAP System ID
        instance_number (str, optional): Instance number (when not using SID-based configuration)
        host (str, optional): Host where HANA is running (when not using SID-based configuration)
        wait (bool): Whether to wait for HANA to stop
        timeout (int): Maximum time to wait in seconds
        
    Returns:
        dict: Operation result
    """
    try:
        # Get system info if not provided
        if not instance_number or not host:
            system_info = get_system_info(sid, "db")
            if not instance_number:
                instance_number = system_info.get("instance_number", "00")
            if not host:
                host = system_info.get("hostname")
                
            if not host:
                return {
                    "status": "error",
                    "message": f"Could not determine host for SID {sid}"
                }
        
        # Log the action
        logger.info(f"Stopping HANA database {sid} on {host}")
        
        # Construct the full path to HDB command
        hdb_path = f"/usr/sap/{sid.upper()}/HDB{instance_number}"
        
        # Execute HDB stop command as dbadm user
        stop_command = f"{hdb_path}/HDB stop"
        result = await execute_command_as_sap_user(sid, "db", stop_command, sap_user_type="dbadm")
        
        if result["status"] == "error":
            return {
                "status": "error",
                "message": f"Error stopping HANA: {result.get('stderr', '')}"
            }
        
        # Wait for HANA to stop if requested
        if wait:
            wait_result = await _wait_for_status(sid, instance_number, host, "STOPPED", timeout, "stop")
            return wait_result
        
        return {
            "status": "success",
            "message": f"HANA stop command executed successfully for {sid}"
        }
    except Exception as e:
        logger.error(f"Error stopping HANA: {e}")
        return {
            "status": "error",
            "message": f"Error stopping HANA: {e}"
        }

async def restart_hana(
    sid: str,
    instance_number: str = None,
    host: str = None,
    wait: bool = True,
    timeout: int = 600
) -> Dict[str, Any]:
    """
    Restart HANA database
    
    Args:
        sid (str): SAP System ID
        instance_number (str, optional): Instance number (when not using SID-based configuration)
        host (str, optional): Host where HANA is running (when not using SID-based configuration)
        wait (bool): Whether to wait for HANA to restart
        timeout (int): Maximum time to wait in seconds
        
    Returns:
        dict: Operation result
    """
    try:
        # Get system info if not provided
        if not instance_number or not host:
            system_info = get_system_info(sid, "db")
            if not instance_number:
                instance_number = system_info.get("instance_number", "00")
            if not host:
                host = system_info.get("hostname")
                
            if not host:
                return {
                    "status": "error",
                    "message": f"Could not determine host for SID {sid}"
                }
        
        # Log the action
        logger.info(f"Restarting HANA database {sid} on {host}")
        
        # Stop HANA
        stop_result = await stop_hana(sid, instance_number, host, wait=True, timeout=timeout/2)
        if stop_result["status"] == "error":
            return stop_result
        
        # Start HANA
        start_result = await start_hana(sid, instance_number, host, wait=wait, timeout=timeout/2)
        return start_result
    except Exception as e:
        logger.error(f"Error restarting HANA: {e}")
        return {
            "status": "error",
            "message": f"Error restarting HANA: {e}"
        }

# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage HANA database")
    parser.add_argument("--sid", required=True, help="SAP System ID")
    parser.add_argument("--instance", help="Instance number")
    parser.add_argument("--host", help="Host where HANA is running")
    parser.add_argument("--action", required=True, choices=["start", "stop", "restart"], 
                        help="Action to perform")
    parser.add_argument("--no-wait", action="store_true", help="Don't wait for action completion")
    parser.add_argument("--timeout", type=int, default=300, help="Maximum time to wait in seconds")
    
    args = parser.parse_args()
    
    result = asyncio.run(manage_hana_system(
        sid=args.sid,
        instance_number=args.instance,
        host=args.host,
        action=args.action,
        wait=not args.no_wait,
        timeout=args.timeout
    ))
    
    print(json.dumps(result, indent=2))
