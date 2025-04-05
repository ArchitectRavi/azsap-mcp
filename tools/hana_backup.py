#!/usr/bin/env python3
"""
HANA Database Backup Tool

This module provides tools for managing SAP HANA database backups,
including triggering backups for both system and tenant databases.
"""
import logging
import json
import asyncio
from typing import Dict, Any, List, Optional, Union
import os
from datetime import datetime

from tools.command_executor import (
    execute_command, 
    execute_command_for_system, 
    execute_command_as_sap_user,
    get_system_info
)
from tools.hana_status import check_hana_status
from hana_connection import hana_connection, execute_query

# Configure logging
logger = logging.getLogger(__name__)

async def trigger_hana_backup(
    sid: str,
    instance_number: str = None,
    host: str = None,
    backup_type: str = "COMPLETE",
    database_name: str = None,
    use_system_db: bool = False,
    comment: str = None,
    destination_path: str = None,
    auth_context: Optional[Dict[str, Any]] = None,
    wait: bool = True,
    timeout: int = 3600
) -> Dict[str, Any]:
    """
    Trigger a backup for SAP HANA database
    
    Args:
        sid (str): SAP System ID
        instance_number (str, optional): Instance number (when not using SID-based configuration)
        host (str, optional): Host where HANA is running (when not using SID-based configuration)
        backup_type (str): Type of backup to perform ('COMPLETE', 'INCREMENTAL', 'DIFFERENTIAL', 'LOG')
        database_name (str, optional): Name of the tenant database (if not using system DB)
        use_system_db (bool): Whether to back up the system database (True) or tenant database (False)
        comment (str, optional): Comment to add to the backup
        destination_path (str, optional): Custom destination path for the backup
        auth_context (dict, optional): Authentication context
        wait (bool): Whether to wait for backup completion
        timeout (int): Maximum time to wait in seconds
        
    Returns:
        dict: Operation result
    """
    try:
        # Validate backup type
        valid_backup_types = ["COMPLETE", "INCREMENTAL", "DIFFERENTIAL", "LOG"]
        if backup_type not in valid_backup_types:
            return {
                "status": "error",
                "message": f"Invalid backup type: {backup_type}. Must be one of {valid_backup_types}"
            }
        
        # Get system information if not provided
        if not instance_number or not host:
            system_info = await get_system_info(sid, auth_context)
            instance_number = instance_number or system_info.get("instance_number")
            host = host or system_info.get("host")
            
            if not instance_number or not host:
                return {
                    "status": "error",
                    "message": f"Could not determine instance number or host for SID {sid}"
                }
        
        # Check if HANA is running
        hana_status = await check_hana_status(sid, instance_number, host, auth_context)
        if hana_status.get("status") != "success" or "RUNNING" not in hana_status.get("system_status", ""):
            return {
                "status": "error",
                "message": f"HANA database is not running. Current status: {hana_status.get('system_status', 'UNKNOWN')}"
            }
        
        # Prepare backup command
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        backup_id = f"{sid}_{backup_type}_{timestamp}"
        
        # Format comment if provided
        comment_str = f"'{comment}'" if comment else f"'Backup initiated via MCP tool at {timestamp}'"
        
        # Determine database name
        if not use_system_db and not database_name:
            # Try to get tenant database name if not specified
            try:
                with hana_connection(use_system_db=True) as conn:
                    tenant_query = "SELECT DATABASE_NAME FROM SYS.M_DATABASES WHERE NOT DATABASE_NAME = 'SYSTEMDB'"
                    tenant_result = execute_query(conn, tenant_query)
                    if tenant_result and len(tenant_result) > 0:
                        database_name = tenant_result[0]["DATABASE_NAME"]
                    else:
                        return {
                            "status": "error",
                            "message": "No tenant databases found"
                        }
            except Exception as e:
                logger.error(f"Error getting tenant database name: {str(e)}")
                return {
                    "status": "error",
                    "message": f"Error getting tenant database name: {str(e)}"
                }
        
        # Build SQL backup command based on database type
        if use_system_db:
            # System DB backup
            sql_command = f"BACKUP DATA {backup_type} USING BACKINT {comment_str}"
            if destination_path:
                sql_command = f"BACKUP DATA {backup_type} '{destination_path}' {comment_str}"
            
            # Execute backup command via hdbuserstore or direct connection
            command = f"hdbsql -i {instance_number} -d SYSTEMDB -u SYSTEM -p {{SYSTEM_PASSWORD}} \"{sql_command}\""
        else:
            # Tenant DB backup
            db_name = database_name or "TENANTDB"  # Fallback name if not determined
            sql_command = f"BACKUP DATA {backup_type} FOR {db_name} USING BACKINT {comment_str}"
            if destination_path:
                sql_command = f"BACKUP DATA {backup_type} FOR {db_name} '{destination_path}' {comment_str}"
            
            # Execute backup command via hdbuserstore or direct connection
            command = f"hdbsql -i {instance_number} -d SYSTEMDB -u SYSTEM -p {{SYSTEM_PASSWORD}} \"{sql_command}\""
        
        # Log the command (without password)
        logger.info(f"Executing backup command for {'System DB' if use_system_db else f'Tenant DB {database_name}'}")
        
        # Execute the backup command as <sid>adm user
        result = await execute_command_as_sap_user(
            command=command,
            sid=sid,
            host=host,
            auth_context=auth_context
        )
        
        if "ERROR" in result.get("stdout", "") or result.get("exit_code", 1) != 0:
            error_message = result.get("stderr", "") or result.get("stdout", "")
            return {
                "status": "error",
                "message": f"Backup command failed: {error_message}",
                "command_result": result
            }
        
        # If not waiting for completion, return success
        if not wait:
            return {
                "status": "success",
                "message": f"Backup initiated for {'System DB' if use_system_db else f'Tenant DB {database_name}'}",
                "backup_id": backup_id,
                "command_result": result
            }
        
        # Wait for backup to complete by monitoring backup catalog
        backup_status = await _wait_for_backup_completion(
            sid=sid,
            instance_number=instance_number,
            host=host,
            backup_id=backup_id,
            use_system_db=use_system_db,
            database_name=database_name,
            timeout=timeout,
            auth_context=auth_context
        )
        
        return backup_status
        
    except Exception as e:
        logger.error(f"Error triggering HANA backup: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }

async def _wait_for_backup_completion(
    sid: str,
    instance_number: str,
    host: str,
    backup_id: str,
    use_system_db: bool,
    database_name: str = None,
    timeout: int = 3600,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Wait for HANA backup to complete by monitoring the backup catalog
    
    Args:
        sid (str): SAP System ID
        instance_number (str): Instance number
        host (str): Host where HANA is running
        backup_id (str): Backup ID to monitor
        use_system_db (bool): Whether monitoring system DB or tenant DB backup
        database_name (str, optional): Name of the tenant database
        timeout (int): Maximum time to wait in seconds
        auth_context (dict, optional): Authentication context
        
    Returns:
        dict: Operation result
    """
    start_time = datetime.now()
    check_interval = 30  # seconds
    elapsed_time = 0
    
    while elapsed_time < timeout:
        try:
            # Check backup status in catalog
            db_name = "SYSTEMDB" if use_system_db else (database_name or "TENANTDB")
            
            # Query to check backup status
            query = f"""
            SELECT 
                STATE, 
                COMMENT, 
                START_TIME, 
                END_TIME, 
                BACKUP_SIZE_BYTES / 1024 / 1024 AS BACKUP_SIZE_MB 
            FROM M_BACKUP_CATALOG 
            WHERE COMMENT LIKE '%{backup_id}%' 
            ORDER BY START_TIME DESC 
            LIMIT 1
            """
            
            command = f"hdbsql -i {instance_number} -d {db_name} -u SYSTEM -p {{SYSTEM_PASSWORD}} \"{query}\""
            
            result = await execute_command_as_sap_user(
                command=command,
                sid=sid,
                host=host,
                auth_context=auth_context
            )
            
            if "successful" in result.get("stdout", "").lower():
                # Backup completed successfully
                return {
                    "status": "success",
                    "message": f"Backup completed successfully for {db_name}",
                    "backup_id": backup_id,
                    "details": result.get("stdout", "")
                }
            
            if "failed" in result.get("stdout", "").lower() or "error" in result.get("stdout", "").lower():
                # Backup failed
                return {
                    "status": "error",
                    "message": f"Backup failed for {db_name}",
                    "backup_id": backup_id,
                    "details": result.get("stdout", "")
                }
            
            # Wait before checking again
            await asyncio.sleep(check_interval)
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
        except Exception as e:
            logger.error(f"Error checking backup status: {str(e)}")
            # Continue waiting despite error
            await asyncio.sleep(check_interval)
            elapsed_time = (datetime.now() - start_time).total_seconds()
    
    # Timeout reached
    return {
        "status": "warning",
        "message": f"Timeout reached while waiting for backup completion. Backup may still be in progress.",
        "backup_id": backup_id
    }

async def get_backup_catalog(
    sid: str,
    instance_number: str = None,
    host: str = None,
    use_system_db: bool = True,
    database_name: str = None,
    limit: int = 10,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Get the backup catalog information from SAP HANA
    
    Args:
        sid (str): SAP System ID
        instance_number (str, optional): Instance number (when not using SID-based configuration)
        host (str, optional): Host where HANA is running (when not using SID-based configuration)
        use_system_db (bool): Whether to query the system database
        database_name (str, optional): Name of the tenant database (if not using system DB)
        limit (int): Maximum number of backup entries to return
        auth_context (dict, optional): Authentication context
        
    Returns:
        dict: Operation result with backup catalog information
    """
    try:
        # Get system information if not provided
        if not instance_number or not host:
            system_info = await get_system_info(sid, auth_context)
            instance_number = instance_number or system_info.get("instance_number")
            host = host or system_info.get("host")
            
            if not instance_number or not host:
                return {
                    "status": "error",
                    "message": f"Could not determine instance number or host for SID {sid}"
                }
        
        # Determine database name
        db_name = "SYSTEMDB" if use_system_db else (database_name or "TENANTDB")
        
        # If not using system DB and database_name not provided, try to get tenant database name
        if not use_system_db and not database_name:
            try:
                with hana_connection(use_system_db=True) as conn:
                    tenant_query = "SELECT DATABASE_NAME FROM SYS.M_DATABASES WHERE NOT DATABASE_NAME = 'SYSTEMDB'"
                    tenant_result = execute_query(conn, tenant_query)
                    if tenant_result and len(tenant_result) > 0:
                        db_name = tenant_result[0]["DATABASE_NAME"]
            except Exception as e:
                logger.error(f"Error getting tenant database name: {str(e)}")
                return {
                    "status": "error",
                    "message": f"Error getting tenant database name: {str(e)}"
                }
        
        # Query to get backup catalog information
        query = f"""
        SELECT 
            BACKUP_ID,
            ENTRY_TYPE_NAME,
            BACKUP_TYPE,
            STATE_NAME,
            DATABASE_NAME,
            COMMENT,
            START_TIME,
            END_TIME,
            BACKUP_SIZE_BYTES / 1024 / 1024 AS BACKUP_SIZE_MB,
            DESTINATION_TYPE_NAME,
            DESTINATION_PATH
        FROM M_BACKUP_CATALOG 
        ORDER BY START_TIME DESC 
        LIMIT {limit}
        """
        
        command = f"hdbsql -i {instance_number} -d {db_name} -u SYSTEM -p {{SYSTEM_PASSWORD}} -j -A \"{query}\""
        
        result = await execute_command_as_sap_user(
            command=command,
            sid=sid,
            host=host,
            auth_context=auth_context
        )
        
        if result.get("exit_code", 1) != 0:
            error_message = result.get("stderr", "") or result.get("stdout", "")
            return {
                "status": "error",
                "message": f"Failed to get backup catalog: {error_message}"
            }
        
        # Parse JSON output from hdbsql
        try:
            backup_catalog = json.loads(result.get("stdout", "[]"))
            return {
                "status": "success",
                "message": f"Successfully retrieved backup catalog for {db_name}",
                "database_name": db_name,
                "backup_catalog": backup_catalog
            }
        except json.JSONDecodeError:
            # If not JSON format, try to parse tabular output
            lines = result.get("stdout", "").strip().split("\n")
            if len(lines) > 1:
                headers = lines[0].split(",")
                data = []
                for line in lines[1:]:
                    values = line.split(",")
                    if len(values) == len(headers):
                        entry = {headers[i].strip(): values[i].strip() for i in range(len(headers))}
                        data.append(entry)
                
                return {
                    "status": "success",
                    "message": f"Successfully retrieved backup catalog for {db_name}",
                    "database_name": db_name,
                    "backup_catalog": data
                }
            else:
                return {
                    "status": "warning",
                    "message": f"No backup entries found for {db_name}",
                    "database_name": db_name,
                    "backup_catalog": []
                }
    
    except Exception as e:
        logger.error(f"Error getting backup catalog: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }

# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="SAP HANA Backup Tool")
    parser.add_argument("--sid", required=True, help="SAP System ID")
    parser.add_argument("--instance", help="Instance number")
    parser.add_argument("--host", help="Host where HANA is running")
    parser.add_argument("--action", choices=["backup", "catalog"], default="catalog", help="Action to perform")
    parser.add_argument("--type", choices=["COMPLETE", "INCREMENTAL", "DIFFERENTIAL", "LOG"], default="COMPLETE", help="Backup type")
    parser.add_argument("--system-db", action="store_true", help="Use system database")
    parser.add_argument("--tenant", help="Tenant database name")
    parser.add_argument("--comment", help="Backup comment")
    parser.add_argument("--destination", help="Backup destination path")
    parser.add_argument("--no-wait", action="store_true", help="Don't wait for backup completion")
    parser.add_argument("--timeout", type=int, default=3600, help="Timeout in seconds")
    
    args = parser.parse_args()
    
    loop = asyncio.get_event_loop()
    
    if args.action == "backup":
        result = loop.run_until_complete(trigger_hana_backup(
            sid=args.sid,
            instance_number=args.instance,
            host=args.host,
            backup_type=args.type,
            database_name=args.tenant,
            use_system_db=args.system_db,
            comment=args.comment,
            destination_path=args.destination,
            wait=not args.no_wait,
            timeout=args.timeout
        ))
    else:
        result = loop.run_until_complete(get_backup_catalog(
            sid=args.sid,
            instance_number=args.instance,
            host=args.host,
            use_system_db=args.system_db,
            database_name=args.tenant
        ))
    
    print(json.dumps(result, indent=2))
