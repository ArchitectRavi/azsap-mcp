#!/usr/bin/env python3
"""
SAP Prompt and Resource Handlers for MCP Server

This module provides handlers for SAP-related prompts and resources
that can be registered with the MCP server.
"""
import logging
import json
from typing import Dict, Any, List, Optional, Union

# Configure logging
logger = logging.getLogger(__name__)

async def register_prompt_handlers(mcp):
    """
    Register all prompt handlers with the MCP server.
    
    Args:
        mcp: MCP server instance
    """
    @mcp.prompt("hana-backup")
    async def hana_backup_prompt(sid: str, use_system_db: bool = False, database_name: str = None):
        """
        Generate a guided prompt for HANA backup operations.
        
        Args:
            sid (str): SAP System ID
            use_system_db (bool): Whether to use the system database
            database_name (str, optional): Name of tenant database if not using system DB
            
        Returns:
            List of messages for the prompt
        """
        try:
            # Import locally
            from prompts.sap_prompts import get_hana_backup_prompt
            
            messages = await get_hana_backup_prompt(
                sid=sid,
                use_system_db=use_system_db,
                database_name=database_name
            )
            
            return messages
        except Exception as e:
            logging.error(f"Error generating HANA backup prompt: {str(e)}", exc_info=True)
            return [
                {
                    "role": "system",
                    "content": {
                        "type": "text",
                        "text": f"Error preparing HANA backup prompt: {str(e)}"
                    }
                }
            ]

    @mcp.prompt("hana-monitoring")
    async def hana_monitoring_prompt(sid: str):
        """
        Generate a guided prompt for HANA monitoring operations.
        
        Args:
            sid (str): SAP System ID
            
        Returns:
            List of messages for the prompt
        """
        try:
            # Import locally
            from prompts.sap_prompts import get_hana_monitoring_prompt
            
            messages = await get_hana_monitoring_prompt(
                sid=sid
            )
            
            return messages
        except Exception as e:
            logging.error(f"Error generating HANA monitoring prompt: {str(e)}", exc_info=True)
            return [
                {
                    "role": "system",
                    "content": {
                        "type": "text",
                        "text": f"Error preparing HANA monitoring prompt: {str(e)}"
                    }
                }
            ]
            
    @mcp.prompt("sap-quality-check")
    async def sap_quality_check_prompt(inputs: Dict[str, str] = None):
        """
        Generate a guided prompt for quality checking SAP VMs against Microsoft's best practices.
        
        Args:
            inputs (Dict[str, str], optional): Input parameters (unused for this prompt)
            
        Returns:
            List of messages for the prompt
        """
        try:
            prompt_text = """# SAP Quality Check

This tool validates SAP deployments on Azure against Microsoft's best practices.

## Available Operations

1. **Run Quality Check on VM**: Validate a VM's compliance with SAP on Azure best practices
2. **Get Quality Check Definitions**: View supported configurations and tests

## Required Information

To run a quality check, you'll need:
- VM name
- SAP component type (HANA, Oracle, MSSQL, Db2, ASE)
- VM role (DB, ASCS, APP)
- Resource group (optional)
- VM OS (Windows, SUSE, RedHat, OracleLinux)

## Example

To check if a VM meets SAP HANA requirements:
```
run_sap_quality_check_tool(
    vm_name="vm-hana-prod",
    vm_role="DB",
    sap_component_type="HANA",
    resource_group="rg-sap-prod",
    vm_os="SUSE"
)
```

For extended checks on Linux VMs, provide SSH credentials:
```
run_sap_quality_check_tool(
    vm_name="vm-hana-prod",
    vm_role="DB",
    sap_component_type="HANA",
    resource_group="rg-sap-prod",
    vm_os="SUSE",
    ssh_host="vm-hana-prod.contoso.com",
    ssh_username="azureadmin"
)
```

To view supported configurations:
```
get_sap_quality_check_definitions_tool()
```
"""
            
            return [
                {
                    "role": "system",
                    "content": {
                        "type": "text",
                        "text": prompt_text
                    }
                }
            ]
        except Exception as e:
            logging.error(f"Error generating SAP Quality Check prompt: {str(e)}", exc_info=True)
            return [
                {
                    "role": "system",
                    "content": {
                        "type": "text",
                        "text": f"Error preparing SAP Quality Check prompt: {str(e)}"
                    }
                }
            ]

async def register_resource_handlers(mcp):
    """
    Register all resource handlers with the MCP server.
    
    Args:
        mcp: MCP server instance
    """
    @mcp.resource("sap://{uri}")
    async def handle_sap_resource(uri: str):
        """
        Handle SAP resources.
        
        Args:
            uri (str): Resource URI
            
        Returns:
            Resource data
        """
        try:
            # Import locally
            from resources.sap_resources import get_sap_resource
            
            result = await get_sap_resource(uri)
            
            if result.get("status") == "success":
                return {
                    "content": result.get("content", ""),
                    "contentType": result.get("content_type", "text/plain")
                }
            else:
                return {
                    "content": result.get("content", f"Error: {result.get('message', 'Unknown error')}"),
                    "contentType": "text/plain"
                }
        except Exception as e:
            logging.error(f"Error handling SAP resource: {str(e)}", exc_info=True)
            return {
                "content": f"Error handling resource: {str(e)}",
                "contentType": "text/plain"
            }

    @mcp.resource("azure://{uri}")
    async def handle_azure_resource(uri: str):
        """
        Handle Azure resources.
        
        Args:
            uri (str): Resource URI
            
        Returns:
            Resource data
        """
        try:
            # Import locally
            from resources.sap_resources import get_sap_resource
            
            result = await get_sap_resource(uri)
            
            if result.get("status") == "success":
                return {
                    "content": result.get("content", ""),
                    "contentType": result.get("content_type", "text/plain")
                }
            else:
                return {
                    "content": result.get("content", f"Error: {result.get('message', 'Unknown error')}"),
                    "contentType": "text/plain"
                }
        except Exception as e:
            logging.error(f"Error handling Azure resource: {str(e)}", exc_info=True)
            return {
                "content": f"Error handling resource: {str(e)}",
                "contentType": "text/plain"
            }
