#!/usr/bin/env python3
"""
SAP Resources for MCP Server

This module provides resource definitions for SAP data,
allowing clients to reference SAP information using URIs.
"""
import logging
import json
from typing import Dict, Any, List, Optional, Union
import re
import os
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

class SapResourceProvider:
    """Provider for SAP-related resources."""
    
    def __init__(self):
        """Initialize the SAP resource provider."""
        self.resource_handlers = {
            r'^sap://([^/]+)/hana/backup-catalog$': self._handle_backup_catalog,
            r'^sap://([^/]+)/hana/status$': self._handle_hana_status,
            r'^sap://([^/]+)/system/logs$': self._handle_system_logs,
            r'^azure://([^/]+)/vm/([^/]+)/metrics$': self._handle_vm_metrics,
        }
    
    async def get_resource(self, uri: str) -> Dict[str, Any]:
        """
        Get a resource by URI.
        
        Args:
            uri (str): Resource URI
            
        Returns:
            Dict[str, Any]: Resource data or error
        """
        try:
            # Find matching handler
            for pattern, handler in self.resource_handlers.items():
                match = re.match(pattern, uri)
                if match:
                    return await handler(uri, match)
            
            # No handler found
            return {
                "status": "error",
                "message": f"No handler found for resource URI: {uri}",
                "content_type": "text/plain",
                "content": f"Resource not found: {uri}"
            }
        
        except Exception as e:
            logger.error(f"Error handling resource URI {uri}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error handling resource: {str(e)}",
                "content_type": "text/plain",
                "content": f"Error: {str(e)}"
            }
    
    async def _handle_backup_catalog(self, uri: str, match) -> Dict[str, Any]:
        """
        Handle backup catalog resource.
        
        Args:
            uri (str): Resource URI
            match: Regex match object
            
        Returns:
            Dict[str, Any]: Resource data
        """
        try:
            sid = match.group(1)
            
            # Import locally to avoid circular imports
            from tools.hana_backup import get_backup_catalog
            
            # Get backup catalog data
            result = await get_backup_catalog(sid=sid, limit=20)
            
            if result.get("status") == "success":
                # Format as JSON resource
                return {
                    "status": "success",
                    "message": f"Retrieved backup catalog for {sid}",
                    "content_type": "application/json",
                    "content": json.dumps(result.get("backup_catalog", []), indent=2)
                }
            else:
                return {
                    "status": "error",
                    "message": result.get("message", "Unknown error"),
                    "content_type": "text/plain",
                    "content": f"Error retrieving backup catalog: {result.get('message', 'Unknown error')}"
                }
        
        except Exception as e:
            logger.error(f"Error handling backup catalog resource: {str(e)}")
            return {
                "status": "error",
                "message": f"Error handling backup catalog resource: {str(e)}",
                "content_type": "text/plain",
                "content": f"Error: {str(e)}"
            }
    
    async def _handle_hana_status(self, uri: str, match) -> Dict[str, Any]:
        """
        Handle HANA status resource.
        
        Args:
            uri (str): Resource URI
            match: Regex match object
            
        Returns:
            Dict[str, Any]: Resource data
        """
        try:
            sid = match.group(1)
            
            # Import locally to avoid circular imports
            from tools.hana_status import check_hana_status
            
            # Get HANA status data
            result = await check_hana_status(sid=sid)
            
            if result.get("status") == "success":
                # Format as JSON resource
                return {
                    "status": "success",
                    "message": f"Retrieved HANA status for {sid}",
                    "content_type": "application/json",
                    "content": json.dumps(result, indent=2)
                }
            else:
                return {
                    "status": "error",
                    "message": result.get("message", "Unknown error"),
                    "content_type": "text/plain",
                    "content": f"Error retrieving HANA status: {result.get('message', 'Unknown error')}"
                }
        
        except Exception as e:
            logger.error(f"Error handling HANA status resource: {str(e)}")
            return {
                "status": "error",
                "message": f"Error handling HANA status resource: {str(e)}",
                "content_type": "text/plain",
                "content": f"Error: {str(e)}"
            }
    
    async def _handle_system_logs(self, uri: str, match) -> Dict[str, Any]:
        """
        Handle system logs resource.
        
        Args:
            uri (str): Resource URI
            match: Regex match object
            
        Returns:
            Dict[str, Any]: Resource data
        """
        try:
            sid = match.group(1)
            
            # Import locally to avoid circular imports
            from tools.command_executor import execute_command_as_sap_user
            
            # Execute command to get system logs
            result = await execute_command_as_sap_user(
                command="tail -n 100 /var/log/messages",
                sid=sid
            )
            
            if result.get("exit_code") == 0:
                # Format as text resource
                return {
                    "status": "success",
                    "message": f"Retrieved system logs for {sid}",
                    "content_type": "text/plain",
                    "content": result.get("stdout", "No log data available")
                }
            else:
                return {
                    "status": "error",
                    "message": f"Error retrieving system logs: {result.get('stderr', 'Unknown error')}",
                    "content_type": "text/plain",
                    "content": f"Error: {result.get('stderr', 'Unknown error')}"
                }
        
        except Exception as e:
            logger.error(f"Error handling system logs resource: {str(e)}")
            return {
                "status": "error",
                "message": f"Error handling system logs resource: {str(e)}",
                "content_type": "text/plain",
                "content": f"Error: {str(e)}"
            }
    
    async def _handle_vm_metrics(self, uri: str, match) -> Dict[str, Any]:
        """
        Handle VM metrics resource.
        
        Args:
            uri (str): Resource URI
            match: Regex match object
            
        Returns:
            Dict[str, Any]: Resource data
        """
        try:
            subscription_id = match.group(1)
            vm_name = match.group(2)
            
            # Import locally to avoid circular imports
            from tools.azure_tools.vm_operations import get_vm_metrics
            
            # Get VM metrics
            result = await get_vm_metrics(
                subscription_id=subscription_id,
                vm_name=vm_name,
                metric_names=["Percentage CPU", "Available Memory Bytes", "Disk Read Operations/Sec", "Disk Write Operations/Sec"]
            )
            
            if result.get("status") == "success":
                # Format as JSON resource
                return {
                    "status": "success",
                    "message": f"Retrieved metrics for VM {vm_name}",
                    "content_type": "application/json",
                    "content": json.dumps(result.get("metrics", {}), indent=2)
                }
            else:
                return {
                    "status": "error",
                    "message": result.get("message", "Unknown error"),
                    "content_type": "text/plain",
                    "content": f"Error retrieving VM metrics: {result.get('message', 'Unknown error')}"
                }
        
        except Exception as e:
            logger.error(f"Error handling VM metrics resource: {str(e)}")
            return {
                "status": "error",
                "message": f"Error handling VM metrics resource: {str(e)}",
                "content_type": "text/plain",
                "content": f"Error: {str(e)}"
            }

# Create singleton instance
sap_resource_provider = SapResourceProvider()

async def get_sap_resource(uri: str) -> Dict[str, Any]:
    """
    Get a SAP resource by URI.
    
    Args:
        uri (str): Resource URI
        
    Returns:
        Dict[str, Any]: Resource data or error
    """
    return await sap_resource_provider.get_resource(uri)
