#!/usr/bin/env python3
"""
SAP Prompts for MCP Server

This module provides structured prompt definitions for SAP operations,
demonstrating how to create guided workflows using the MCP protocol.
"""
import logging
import json
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

async def get_hana_backup_prompt(sid: str, use_system_db: bool = False, database_name: str = None):
    """
    Generate a structured prompt for HANA backup operations.
    
    This prompt guides users through the process of backing up a HANA database,
    providing context about the current backup status and available options.
    
    Args:
        sid (str): SAP System ID
        use_system_db (bool): Whether to use the system database
        database_name (str, optional): Name of tenant database if not using system DB
        
    Returns:
        List[Dict[str, Any]]: List of messages for the prompt
    """
    try:
        # Import locally to avoid circular imports
        from tools.hana_backup import get_backup_catalog
        from tools.hana_status import check_hana_status
        
        # Get current backup status
        backup_status = await get_backup_catalog(
            sid=sid,
            use_system_db=use_system_db,
            database_name=database_name,
            limit=5
        )
        
        # Get HANA status
        hana_status = await check_hana_status(
            sid=sid
        )
        
        # Determine database name for display
        db_name = "System DB" if use_system_db else (database_name or "Tenant DB")
        
        # Format backup history for display
        backup_history = "No recent backups found."
        if backup_status.get("status") == "success" and backup_status.get("backup_catalog"):
            backup_entries = backup_status.get("backup_catalog")
            backup_history = "\n\n### Recent Backup History\n\n"
            backup_history += "| Backup ID | Type | Status | Start Time | End Time | Size (MB) |\n"
            backup_history += "|-----------|------|--------|------------|----------|----------|\n"
            
            for entry in backup_entries:
                backup_id = entry.get("BACKUP_ID", "N/A")
                backup_type = entry.get("BACKUP_TYPE", "N/A")
                state = entry.get("STATE_NAME", "N/A")
                start_time = entry.get("START_TIME", "N/A")
                end_time = entry.get("END_TIME", "N/A")
                size_mb = entry.get("BACKUP_SIZE_MB", "N/A")
                
                backup_history += f"| {backup_id} | {backup_type} | {state} | {start_time} | {end_time} | {size_mb} |\n"
        
        # Create the prompt messages
        messages = [
            {
                "role": "system",
                "content": {
                    "type": "text",
                    "text": f"You are assisting with SAP HANA backup operations for {sid}. The current database is {db_name}."
                }
            },
            {
                "role": "user",
                "content": {
                    "type": "text",
                    "text": f"I need to create a backup for the SAP HANA {db_name} in system {sid}."
                }
            },
            {
                "role": "system",
                "content": {
                    "type": "text",
                    "text": f"""
# HANA Backup Assistant

## Current System Status
- **System ID:** {sid}
- **Database:** {db_name}
- **HANA Status:** {hana_status.get('system_status', 'Unknown')}

{backup_history}

## Available Backup Types
- **COMPLETE:** Full database backup
- **INCREMENTAL:** Backup of changes since last COMPLETE backup
- **DIFFERENTIAL:** Backup of changes since last COMPLETE or INCREMENTAL backup
- **LOG:** Backup of transaction logs

## Backup Process
1. Select the backup type
2. Specify any additional parameters (comment, destination)
3. Confirm to start the backup
4. Monitor backup progress
"""
                }
            }
        ]
        
        return messages
        
    except Exception as e:
        logger.error(f"Error generating HANA backup prompt: {str(e)}")
        return [
            {
                "role": "system",
                "content": {
                    "type": "text",
                    "text": f"Error preparing HANA backup prompt: {str(e)}"
                }
            }
        ]

async def get_hana_monitoring_prompt(sid: str):
    """
    Generate a structured prompt for HANA monitoring operations.
    
    This prompt provides a comprehensive overview of a HANA system's status,
    including system health, disk usage, and performance metrics.
    
    Args:
        sid (str): SAP System ID
        
    Returns:
        List[Dict[str, Any]]: List of messages for the prompt
    """
    try:
        # Import locally to avoid circular imports
        from tools.hana_status import check_hana_status
        
        # Get HANA status
        hana_status = await check_hana_status(
            sid=sid
        )
        
        # Create the prompt messages
        messages = [
            {
                "role": "system",
                "content": {
                    "type": "text",
                    "text": f"You are assisting with SAP HANA monitoring for system {sid}."
                }
            },
            {
                "role": "user",
                "content": {
                    "type": "text",
                    "text": f"Show me the current status of SAP HANA system {sid}."
                }
            },
            {
                "role": "system",
                "content": {
                    "type": "text",
                    "text": f"""
# HANA System Monitoring Dashboard

## System Overview
- **System ID:** {sid}
- **Status:** {hana_status.get('system_status', 'Unknown')}
- **Version:** {hana_status.get('version', 'Unknown')}
- **Instance Number:** {hana_status.get('instance_number', 'Unknown')}

## Services Status
{hana_status.get('services_status', 'Service status information not available')}

## System Resources
- **Memory Usage:** {hana_status.get('memory_usage', 'Unknown')}
- **CPU Usage:** {hana_status.get('cpu_usage', 'Unknown')}

## Available Monitoring Actions
- Check disk space
- View backup status
- Check table usage
- Monitor performance metrics
- View system logs
"""
                }
            }
        ]
        
        return messages
        
    except Exception as e:
        logger.error(f"Error generating HANA monitoring prompt: {str(e)}")
        return [
            {
                "role": "system",
                "content": {
                    "type": "text",
                    "text": f"Error preparing HANA monitoring prompt: {str(e)}"
                }
            }
        ]

# Add more SAP prompts as needed
