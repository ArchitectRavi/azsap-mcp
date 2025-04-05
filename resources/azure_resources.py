#!/usr/bin/env python3
"""
Azure Resources for MCP Server

This module provides resource definitions for Azure data,
allowing clients to reference Azure information using URIs.
"""
import logging
import json
from typing import Dict, Any, List, Optional, Union
import re
import os
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

class AzureResourceProvider:
    """Provider for Azure-related resources."""
    
    def __init__(self):
        """Initialize the Azure resource provider."""
        self.resource_handlers = {
            r'^azure://([^/]+)/vm/([^/]+)/metrics$': self._handle_vm_metrics,
            r'^azure://([^/]+)/vm/([^/]+)/status$': self._handle_vm_status,
            r'^azure://([^/]+)/vm/([^/]+)/disks$': self._handle_vm_disks,
            r'^azure://([^/]+)/resource-group/([^/]+)$': self._handle_resource_group,
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
    
    async def _handle_vm_status(self, uri: str, match) -> Dict[str, Any]:
        """
        Handle VM status resource.
        
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
            from tools.azure_tools.vm_operations import get_vm_status
            
            # Get VM status
            result = await get_vm_status(
                subscription_id=subscription_id,
                vm_name=vm_name
            )
            
            if result.get("status") == "success":
                # Format as JSON resource
                return {
                    "status": "success",
                    "message": f"Retrieved status for VM {vm_name}",
                    "content_type": "application/json",
                    "content": json.dumps(result, indent=2)
                }
            else:
                return {
                    "status": "error",
                    "message": result.get("message", "Unknown error"),
                    "content_type": "text/plain",
                    "content": f"Error retrieving VM status: {result.get('message', 'Unknown error')}"
                }
        
        except Exception as e:
            logger.error(f"Error handling VM status resource: {str(e)}")
            return {
                "status": "error",
                "message": f"Error handling VM status resource: {str(e)}",
                "content_type": "text/plain",
                "content": f"Error: {str(e)}"
            }
    
    async def _handle_vm_disks(self, uri: str, match) -> Dict[str, Any]:
        """
        Handle VM disks resource.
        
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
            from tools.azure_tools.vm_operations import list_disks
            
            # Get VM disks
            result = await list_disks(
                subscription_id=subscription_id,
                vm_name=vm_name
            )
            
            if result.get("status") == "success":
                # Format as JSON resource
                return {
                    "status": "success",
                    "message": f"Retrieved disks for VM {vm_name}",
                    "content_type": "application/json",
                    "content": json.dumps(result.get("disks", []), indent=2)
                }
            else:
                return {
                    "status": "error",
                    "message": result.get("message", "Unknown error"),
                    "content_type": "text/plain",
                    "content": f"Error retrieving VM disks: {result.get('message', 'Unknown error')}"
                }
        
        except Exception as e:
            logger.error(f"Error handling VM disks resource: {str(e)}")
            return {
                "status": "error",
                "message": f"Error handling VM disks resource: {str(e)}",
                "content_type": "text/plain",
                "content": f"Error: {str(e)}"
            }
    
    async def _handle_resource_group(self, uri: str, match) -> Dict[str, Any]:
        """
        Handle resource group resource.
        
        Args:
            uri (str): Resource URI
            match: Regex match object
            
        Returns:
            Dict[str, Any]: Resource data
        """
        try:
            subscription_id = match.group(1)
            resource_group = match.group(2)
            
            # Import locally to avoid circular imports
            from tools.azure_tools.resource_operations import get_resource_group
            
            # Get resource group
            result = await get_resource_group(
                subscription_id=subscription_id,
                resource_group=resource_group
            )
            
            if result.get("status") == "success":
                # Format as JSON resource
                return {
                    "status": "success",
                    "message": f"Retrieved resource group {resource_group}",
                    "content_type": "application/json",
                    "content": json.dumps(result.get("resources", {}), indent=2)
                }
            else:
                return {
                    "status": "error",
                    "message": result.get("message", "Unknown error"),
                    "content_type": "text/plain",
                    "content": f"Error retrieving resource group: {result.get('message', 'Unknown error')}"
                }
        
        except Exception as e:
            logger.error(f"Error handling resource group resource: {str(e)}")
            return {
                "status": "error",
                "message": f"Error handling resource group resource: {str(e)}",
                "content_type": "text/plain",
                "content": f"Error: {str(e)}"
            }

# Create singleton instance
azure_resource_provider = AzureResourceProvider()

async def get_azure_resource(uri: str) -> Dict[str, Any]:
    """
    Get an Azure resource by URI.
    
    Args:
        uri (str): Resource URI
        
    Returns:
        Dict[str, Any]: Resource data or error
    """
    return await azure_resource_provider.get_resource(uri)
