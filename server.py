"""
SAP HANA MCP Server

A Model Context Protocol (MCP) server for SAP HANA built with the Python MCP SDK.
This server provides standardized tools for interacting with SAP HANA databases.

References:
- MCP Specification: https://modelcontextprotocol.io/
"""

import asyncio
import argparse
from typing import Any, Dict, List, Optional, Union
from mcp.server.fastmcp import FastMCP, Context
from mcp.server.sse import SseServerTransport
from mcp.server import Server   
import uvicorn
import logging
import json
import os
from dotenv import load_dotenv
from hana_connection import hana_connection, execute_query, get_table_schema
from azure.identity import DefaultAzureCredential
from azure.mgmt.apimanagement import ApiManagementClient
from azure.mgmt.apimanagement.models import AuthorizationContract, AuthorizationAccessPolicyContract, AuthorizationLoginRequestContract
import decimal
import sys

# Load environment variables
load_dotenv()

# Initialize the MCP server
mcp = FastMCP("sap-hana-mcp")

# Define server info
server_info = {
    "name": "sap-hana-mcp",
    "version": "1.0.0"
}

# Set server info directly on mcp instance
mcp.server_info = server_info

# Log initialization info
logging.info("SAP HANA MCP Server initialized with updated server info")
print("SAP HANA MCP Server initialized with updated server info", file=sys.stderr)

# Custom JSON encoder for handling Decimal objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)

# Format utilities for tool results
def format_result_content(result: Union[Dict[str, Any], str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    """Format the result content for MCP response.
    
    This helper function standardizes the format of results returned by MCP tools.
    
    Args:
        result: The result to format, can be a dictionary, string, or list of dictionaries
        
    Returns:
        A properly formatted MCP response
    """
    # If result is already in MCP format, return it as is
    if isinstance(result, dict) and "content" in result and "isError" in result:
        return result
    
    # If result is a string, wrap it in a text content item
    if isinstance(result, str):
        return {
            "content": [{"type": "text", "text": result}],
            "isError": False
        }
    
    # If result is a list of dictionaries, assume it's already content items
    if isinstance(result, list) and all(isinstance(item, dict) for item in result):
        return {
            "content": result,
            "isError": False
        }
    
    # Otherwise, convert to string and wrap in a text content item
    return {
        "content": [{"type": "text", "text": str(result)}],
        "isError": False
    }

# Add a global server initialization flag
_server_initialized = False

# Initialize server tools and configuration
def initialize_server():
    """
    Initialize server tools and configurations.
    This function pre-initializes components that might take time to load.
    """
    global _server_initialized
    
    if _server_initialized:
        logging.info("Server already initialized, skipping initialization")
        return
    
    try:
        logging.info("Pre-initializing SAP HANA MCP server tools and connections")
        # Pre-initialize any slow-loading components here
        # This ensures these operations happen before the MCP protocol handshake
        
        # For example, test database connections if needed
        # or pre-cache system view structures
        
        _server_initialized = True
        logging.info("SAP HANA MCP server initialization completed successfully")
    except Exception as e:
        logging.error(f"Error during server initialization: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())

@mcp.tool()
async def get_system_overview(use_system_db: bool = True) -> Dict[str, Any]:
    """Get an overview of the SAP HANA system status, including host information,
    service status, and system resource usage.
    
    This uses the M_HOST_INFORMATION, M_SERVICES, and M_SERVICE_MEMORY system views.
    
    Args:
        use_system_db: Whether to use the system database (recommended for administration)
    """
    # Import tools from the tools directory
    from tools.system_overview import get_system_overview as get_system_overview_impl
    return await get_system_overview_impl(use_system_db)

@mcp.tool()
async def get_disk_usage(use_system_db: bool = True) -> Dict[str, Any]:
    """Get disk usage information for the SAP HANA system, including volume sizes,
    data files, and log files.
    
    This uses the M_VOLUME_FILES, M_DISKS, and M_DATA_VOLUMES system views.
    
    Args:
        use_system_db: Whether to use the system database (recommended for administration)
    """
    # Import tools from the tools directory
    from tools.disk_usage import get_disk_usage as get_disk_usage_impl
    return await get_disk_usage_impl(use_system_db)

@mcp.tool()
async def get_db_info(use_system_db: bool = True) -> Dict[str, Any]:
    """Get database information from SAP HANA.
    
    This uses the M_DATABASE system view.
    
    Args:
        use_system_db: Whether to use the system database (recommended for administration)
    """
    try:
        from tools.system_info import get_db_info as get_db_info_impl
        return await get_db_info_impl(use_system_db)
    except Exception as e:
        logging.error(f"Error getting database information: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting database information: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def get_backup_catalog(use_system_db: bool = True) -> Dict[str, Any]:
    """Get the backup catalog information from SAP HANA.
    
    This uses the M_BACKUP_CATALOG system view.
    
    Args:
        use_system_db: Whether to use the system database (recommended for administration)
    """
    try:
        from tools.system_info import get_backup_catalog as get_backup_catalog_impl
        return await get_backup_catalog_impl(use_system_db)
    except Exception as e:
        logging.error(f"Error getting backup catalog: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting backup catalog: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def get_failed_backups(use_system_db: bool = True) -> Dict[str, Any]:
    """Get information about failed or canceled backups from SAP HANA.
    
    This uses the M_BACKUP_CATALOG and M_BACKUP_CATALOG_FILES system views.
    
    Args:
        use_system_db: Whether to use the system database (recommended for administration)
    """
    try:
        from tools.system_info import get_failed_backups as get_failed_backups_impl
        return await get_failed_backups_impl(use_system_db)
    except Exception as e:
        logging.error(f"Error getting failed backups: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting failed backups: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def get_tablesize_on_disk(use_system_db: bool = True) -> Dict[str, Any]:
    """Get table sizes on disk from SAP HANA.
    
    This uses the PUBLIC.M_TABLE_PERSISTENCE_STATISTICS system view.
    
    Args:
        use_system_db: Whether to use the system database (recommended for administration)
    """
    try:
        from tools.system_info import get_tablesize_on_disk as get_tablesize_on_disk_impl
        return await get_tablesize_on_disk_impl(use_system_db)
    except Exception as e:
        logging.error(f"Error getting table sizes on disk: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting table sizes on disk: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def get_table_used_memory(use_system_db: bool = True) -> Dict[str, Any]:
    """Get memory usage by table type (column vs row) from SAP HANA.
    
    This uses the SYS.M_TABLES system view.
    
    Args:
        use_system_db: Whether to use the system database (recommended for administration)
    """
    try:
        from tools.system_info import get_table_used_memory as get_table_used_memory_impl
        return await get_table_used_memory_impl(use_system_db)
    except Exception as e:
        logging.error(f"Error getting table memory usage: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting table memory usage: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def check_disk_space(sid: str = None, host: str = None, filesystem: str = None, auth_context: Dict[str, Any] = None) -> Dict[str, Any]:
    """Check disk space on SAP/HANA systems.
    
    This tool executes disk space commands on the specified host and parses the output
    to provide structured information about disk usage.
    
    Args:
        sid: SAP System ID (optional, will use default from config if not provided)
        host: The hostname or IP address of the target system (optional, will use default from config if not provided)
        filesystem: Optional specific filesystem to check
        auth_context: Authentication context for SSH connection
    """
    try:
        # If SID is not provided and host is not provided, use the first one from executor_config
        if not sid and not host:
            from tools.command_executor import load_system_config
            config = load_system_config()
            if config and "systems" in config and config["systems"]:
                # Get the first system
                test_sid = next(iter(config["systems"].keys()))
                sid = test_sid
                logging.info(f"Using default SID from config: {sid}")
            
        from tools.disk_check import check_disk_space as check_disk_space_impl
        result = await check_disk_space_impl(sid=sid, host=host, filesystem=filesystem, auth_context=auth_context)
        
        # Format the result for MCP
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error checking disk space: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error checking disk space: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def check_hana_volumes(sid: str = None, host: str = None, auth_context: Dict[str, Any] = None) -> Dict[str, Any]:
    """Check HANA data volumes and their disk usage.
    
    This tool identifies HANA data volumes on the specified host and checks their usage.
    
    Args:
        sid: SAP System ID (optional, will use default from config if not provided)
        host: The hostname or IP address of the target system (optional, will use default from config if not provided)
        auth_context: Authentication context for SSH connection
    """
    try:
        # If SID is not provided and host is not provided, use the first one from executor_config
        if not sid and not host:
            from tools.command_executor import load_system_config
            config = load_system_config()
            if config and "systems" in config and config["systems"]:
                # Get the first system
                test_sid = next(iter(config["systems"].keys()))
                sid = test_sid
                logging.info(f"Using default SID from config: {sid}")
            
        from tools.disk_check import check_hana_volumes as check_hana_volumes_impl
        result = await check_hana_volumes_impl(sid=sid, host=host, auth_context=auth_context)
        
        # Format the result for MCP
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error checking HANA volumes: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error checking HANA volumes: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def manage_hana_system(sid: str = None, instance_number: str = None, host: str = None, action: str = None, 
                             auth_context: Dict[str, Any] = None, wait: bool = True, 
                             timeout: int = 300) -> Dict[str, Any]:
    """Manage HANA database system (start, stop, restart).
    
    This tool executes the appropriate commands to start, stop, or restart a HANA database.
    
    Args:
        sid: SAP System ID (optional, will use default from config if not provided)
        instance_number: HANA instance number (optional, will use default from config if not provided)
        host: The hostname or IP address of the target system (optional, will use default from config if not provided)
        action: Action to perform (start, stop, restart)
        auth_context: Authentication context for SSH connection
        wait: Whether to wait for the operation to complete
        timeout: Maximum time to wait in seconds
    """
    try:
        # Validate the action
        if not action or action.lower() not in ["start", "stop", "restart"]:
            return {
                "content": [{"type": "text", "text": f"Invalid action: {action}. Must be one of: start, stop, restart"}],
                "isError": True
            }
        
        # Authentication check removed as we're using SSH credentials from executor_config
        # Users are already authenticated via the SSH credentials in the executor config
        
        # If SID is not provided, use the first one from executor_config
        if not sid:
            from tools.command_executor import load_system_config
            config = load_system_config()
            if config and "systems" in config and config["systems"]:
                # Get the first system that has type containing "HANA"
                hana_systems = [sid for sid, system in config["systems"].items() 
                              if "type" in system and "HANA" in system["type"]]
                if hana_systems:
                    sid = hana_systems[0]
                    logging.info(f"Using default HANA SID from config: {sid}")
                else:
                    # Fallback to first system if no HANA system found
                    sid = next(iter(config["systems"].keys()))
                    logging.info(f"No HANA system found, using first system SID: {sid}")
            
        from tools.hana_control import manage_hana_system as manage_hana_system_impl
        result = await manage_hana_system_impl(sid=sid, instance_number=instance_number, host=host, 
                                            action=action, auth_context=auth_context, 
                                            wait=wait, timeout=timeout)
        
        # Format the result for MCP
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error managing HANA system: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error managing HANA system: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def check_hana_status(sid: str = None, instance_number: str = None, host: str = None, 
                           auth_context: Dict[str, Any] = None) -> Dict[str, Any]:
    """Check HANA database status.
    
    This tool retrieves the status of HANA services, version information, and system overview.
    
    Args:
        sid: SAP System ID (optional, will use default from config if not provided)
        instance_number: HANA instance number (optional, will use default from config if not provided)
        host: The hostname or IP address of the target system (optional, will use default from config if not provided)
        auth_context: Authentication context for SSH connection
    """
    try:
        # Authentication check removed as we're using SSH credentials from executor_config
        # Users are already authenticated via the SSH credentials in the executor config
        
        # If SID is not provided, use the first one from executor_config
        if not sid:
            from tools.command_executor import load_system_config
            config = load_system_config()
            if config and "systems" in config and config["systems"]:
                # Get the first system that has type containing "HANA"
                hana_systems = [sid for sid, system in config["systems"].items() 
                              if "type" in system and "HANA" in system["type"]]
                if hana_systems:
                    sid = hana_systems[0]
                    logging.info(f"Using default HANA SID from config: {sid}")
                else:
                    # Fallback to first system if no HANA system found
                    sid = next(iter(config["systems"].keys()))
                    logging.info(f"No HANA system found, using first system SID: {sid}")
            
        # Now call the implementation
        from tools.hana_status import check_hana_status as check_hana_status_impl
        result = await check_hana_status_impl(sid=sid, instance_number=instance_number, 
                                             host=host, auth_context=auth_context)
        
        # Format the result for MCP
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error checking HANA status: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error checking HANA status: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def get_hana_version(sid: str = None, instance_number: str = None, host: str = None, 
                           auth_context: Dict[str, Any] = None) -> Dict[str, Any]:
    """Get HANA database version information.
    
    This tool retrieves detailed version information for a HANA database.
    
    Args:
        sid: SAP System ID (optional, will use default from config if not provided)
        instance_number: HANA instance number (optional, will use default from config if not provided)
        host: The hostname or IP address of the target system (optional, will use default from config if not provided)
        auth_context: Authentication context for SSH connection
    """
    try:
        # Authentication check removed as we're using SSH credentials from executor_config
        # Users are already authenticated via the SSH credentials in the executor config
        
        # If SID is not provided, use the first one from executor_config
        if not sid:
            from tools.command_executor import load_system_config
            config = load_system_config()
            if config and "systems" in config and config["systems"]:
                # Get the first system that has type containing "HANA"
                hana_systems = [sid for sid, system in config["systems"].items() 
                              if "type" in system and "HANA" in system["type"]]
                if hana_systems:
                    sid = hana_systems[0]
                    logging.info(f"Using default HANA SID from config: {sid}")
                else:
                    # Fallback to first system if no HANA system found
                    sid = next(iter(config["systems"].keys()))
                    logging.info(f"No HANA system found, using first system SID: {sid}")
            
        from tools.hana_status import get_hana_version as get_hana_version_impl
        result = await get_hana_version_impl(sid=sid, instance_number=instance_number, host=host, auth_context=auth_context)
        
        # Format the result for MCP
        return {
            "content": [{"type": "text", "text": f"HANA Version: {result}"}],
            "isError": False
        }
    except Exception as e:
        logging.error(f"Error getting HANA version: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting HANA version: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def list_sap_systems(auth_context: Dict[str, Any] = None) -> Dict[str, Any]:
    """List all configured SAP systems.
    
    This tool retrieves the list of all SAP systems configured in the executor_config.json file.
    
    Args:
        auth_context: Authentication context for SSH connection
    """
    try:
        # Authentication check removed as we're using SSH credentials from executor_config
        # Users are already authenticated via the SSH credentials in the executor config
            
        from tools.command_executor import list_systems
        systems = list_systems()
        
        # Format the result for MCP
        content = [{"type": "text", "text": "Configured SAP Systems:"}]
        
        for system in systems:
            system_info = f"SID: {system['sid']}\n"
            system_info += f"Description: {system['description']}\n"
            system_info += f"Type: {system['type']}\n"
            system_info += f"Components: {', '.join(system['components'])}"
            
            content.append({"type": "text", "text": system_info})
        
        return {
            "content": content,
            "isError": False
        }
    except Exception as e:
        logging.error(f"Error listing SAP systems: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error listing SAP systems: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def get_vm_status(
    sid: str = None, 
    vm_name: str = None, 
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Get the status of an Azure VM.
    
    This tool retrieves the current status of an Azure VM, including power state,
    provisioning state, and other details.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.azure_tools.vm_operations import get_vm_status as get_vm_status_impl
        result = await get_vm_status_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error getting VM status: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting VM status: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def start_vm(
    sid: str = None, 
    vm_name: str = None, 
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None,
    wait: bool = True,
    timeout: int = 300
) -> Dict[str, Any]:
    """Start an Azure VM.
    
    This tool starts an Azure VM and optionally waits for it to reach the running state.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
        wait: Whether to wait for the VM to reach the running state
        timeout: Maximum time to wait in seconds
    """
    try:
        from tools.azure_tools.vm_operations import start_vm as start_vm_impl
        result = await start_vm_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context,
            wait=wait,
            timeout=timeout
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error starting VM: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error starting VM: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def stop_vm(
    sid: str = None, 
    vm_name: str = None, 
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None,
    deallocate: bool = True,
    wait: bool = True,
    timeout: int = 300
) -> Dict[str, Any]:
    """Stop an Azure VM.
    
    This tool stops an Azure VM and optionally waits for it to reach the stopped state.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
        deallocate: Whether to deallocate the VM (releases compute resources and reduces cost)
        wait: Whether to wait for the VM to reach the stopped state
        timeout: Maximum time to wait in seconds
    """
    try:
        from tools.azure_tools.vm_operations import stop_vm as stop_vm_impl
        result = await stop_vm_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context,
            deallocate=deallocate,
            wait=wait,
            timeout=timeout
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error stopping VM: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error stopping VM: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def restart_vm(
    sid: str = None, 
    vm_name: str = None, 
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None,
    wait: bool = True,
    timeout: int = 600
) -> Dict[str, Any]:
    """Restart an Azure VM.
    
    This tool restarts an Azure VM and optionally waits for it to reach the running state.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
        wait: Whether to wait for the VM to reach the running state
        timeout: Maximum time to wait in seconds
    """
    try:
        from tools.azure_tools.vm_operations import restart_vm as restart_vm_impl
        result = await restart_vm_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context,
            wait=wait,
            timeout=timeout
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error restarting VM: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error restarting VM: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def list_vms(
    sid: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """List Azure VMs.
    
    This tool lists Azure VMs in a subscription or resource group.
    
    Args:
        sid: SAP System ID (optional, will use resource group mapping from config if provided)
        resource_group: Azure resource group (optional, will list VMs in all resource groups if not provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.azure_tools.vm_operations import list_vms as list_vms_impl
        result = await list_vms_impl(
            sid=sid,
            resource_group=resource_group,
            subscription_id=subscription_id,
            auth_context=auth_context
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error listing VMs: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error listing VMs: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def get_nsg_rules(
    nsg_name: str,
    resource_group: str = None,
    subscription_id: str = None,
    sid: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Get rules for a Network Security Group.
    
    This tool retrieves the security rules for an Azure Network Security Group.
    
    Args:
        nsg_name: NSG name
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        sid: SAP System ID (optional, will use resource group mapping from config if provided)
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.azure_tools.nsg_operations import get_nsg_rules as get_nsg_rules_impl
        result = await get_nsg_rules_impl(
            nsg_name=nsg_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            sid=sid,
            auth_context=auth_context
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error getting NSG rules: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting NSG rules: {str(e)}"}],
            "isError": True
        }

@mcp.tool("list_nsgs")
async def list_nsgs(
    resource_group: str = None,
    subscription_id: str = None,
    sid: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """List Network Security Groups.
    
    This tool lists Azure Network Security Groups in a subscription or resource group.
    
    Args:
        resource_group: Azure resource group (optional, will list NSGs in all resource groups if not provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        sid: SAP System ID (optional, will use resource group mapping from config if provided)
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.azure_tools.nsg_operations import list_nsgs as list_nsgs_impl
        result = await list_nsgs_impl(
            resource_group=resource_group,
            subscription_id=subscription_id,
            sid=sid,
            auth_context=auth_context
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error listing NSGs: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error listing NSGs: {str(e)}"}],
            "isError": True
        }

@mcp.tool("add_nsg_rule")
async def add_nsg_rule(
    nsg_name: str,
    rule_name: str,
    priority: int,
    direction: str,
    access: str,
    protocol: str,
    source_address_prefix: str = None,
    source_address_prefixes: List[str] = None,
    source_port_range: str = None,
    source_port_ranges: List[str] = None,
    destination_address_prefix: str = None,
    destination_address_prefixes: List[str] = None,
    destination_port_range: str = None,
    destination_port_ranges: List[str] = None,
    description: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    sid: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Add a rule to a Network Security Group.
    
    This tool creates a new security rule in an Azure Network Security Group.
    
    Args:
        nsg_name: NSG name
        rule_name: Rule name
        priority: Rule priority (100-4096)
        direction: Rule direction ("Inbound" or "Outbound")
        access: Rule access ("Allow" or "Deny")
        protocol: Rule protocol ("Tcp", "Udp", "Icmp", "*")
        source_address_prefix: Source address prefix (optional)
        source_address_prefixes: Source address prefixes (optional)
        source_port_range: Source port range (optional)
        source_port_ranges: Source port ranges (optional)
        destination_address_prefix: Destination address prefix (optional)
        destination_address_prefixes: Destination address prefixes (optional)
        destination_port_range: Destination port range (optional)
        destination_port_ranges: Destination port ranges (optional)
        description: Rule description (optional)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        sid: SAP System ID (optional, will use resource group mapping from config if provided)
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.azure_tools.nsg_operations import add_nsg_rule as add_nsg_rule_impl
        result = await add_nsg_rule_impl(
            nsg_name=nsg_name,
            rule_name=rule_name,
            priority=priority,
            direction=direction,
            access=access,
            protocol=protocol,
            source_address_prefix=source_address_prefix,
            source_address_prefixes=source_address_prefixes,
            source_port_range=source_port_range,
            source_port_ranges=source_port_ranges,
            destination_address_prefix=destination_address_prefix,
            destination_address_prefixes=destination_address_prefixes,
            destination_port_range=destination_port_range,
            destination_port_ranges=destination_port_ranges,
            description=description,
            resource_group=resource_group,
            subscription_id=subscription_id,
            sid=sid,
            auth_context=auth_context
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error adding NSG rule: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error adding NSG rule: {str(e)}"}],
            "isError": True
        }

@mcp.tool("remove_nsg_rule")
async def remove_nsg_rule(
    nsg_name: str,
    rule_name: str,
    resource_group: str = None,
    subscription_id: str = None,
    sid: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Remove a rule from a Network Security Group.
    
    This tool deletes a security rule from an Azure Network Security Group.
    
    Args:
        nsg_name: NSG name
        rule_name: Rule name
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        sid: SAP System ID (optional, will use resource group mapping from config if provided)
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.azure_tools.nsg_operations import remove_nsg_rule as remove_nsg_rule_impl
        result = await remove_nsg_rule_impl(
            nsg_name=nsg_name,
            rule_name=rule_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            sid=sid,
            auth_context=auth_context
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error removing NSG rule: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error removing NSG rule: {str(e)}"}],
            "isError": True
        }

@mcp.tool("update_nsg_rule")
async def update_nsg_rule(
    nsg_name: str,
    rule_name: str,
    priority: int = None,
    direction: str = None,
    access: str = None,
    protocol: str = None,
    source_address_prefix: str = None,
    source_address_prefixes: List[str] = None,
    source_port_range: str = None,
    source_port_ranges: List[str] = None,
    destination_address_prefix: str = None,
    destination_address_prefixes: List[str] = None,
    destination_port_range: str = None,
    destination_port_ranges: List[str] = None,
    description: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    sid: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Update an existing rule in a Network Security Group.
    
    This tool modifies an existing security rule in an Azure Network Security Group.
    Only the parameters that are specified will be updated, others will remain unchanged.
    
    Args:
        nsg_name: NSG name
        rule_name: Rule name
        priority: Rule priority (100-4096) (optional)
        direction: Rule direction ("Inbound" or "Outbound") (optional)
        access: Rule access ("Allow" or "Deny") (optional)
        protocol: Rule protocol ("Tcp", "Udp", "Icmp", "*") (optional)
        source_address_prefix: Source address prefix (optional)
        source_address_prefixes: Source address prefixes (optional)
        source_port_range: Source port range (optional)
        source_port_ranges: Source port ranges (optional)
        destination_address_prefix: Destination address prefix (optional)
        destination_address_prefixes: Destination address prefixes (optional)
        destination_port_range: Destination port range (optional)
        destination_port_ranges: Destination port ranges (optional)
        description: Rule description (optional)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        sid: SAP System ID (optional, will use resource group mapping from config if provided)
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.azure_tools.nsg_operations import update_nsg_rule as update_nsg_rule_impl
        result = await update_nsg_rule_impl(
            nsg_name=nsg_name,
            rule_name=rule_name,
            priority=priority,
            direction=direction,
            access=access,
            protocol=protocol,
            source_address_prefix=source_address_prefix,
            source_address_prefixes=source_address_prefixes,
            source_port_range=source_port_range,
            source_port_ranges=source_port_ranges,
            destination_address_prefix=destination_address_prefix,
            destination_address_prefixes=destination_address_prefixes,
            destination_port_range=destination_port_range,
            destination_port_ranges=destination_port_ranges,
            description=description,
            resource_group=resource_group,
            subscription_id=subscription_id,
            sid=sid,
            auth_context=auth_context
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error updating NSG rule: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error updating NSG rule: {str(e)}"}],
            "isError": True
        }

@mcp.tool("get_sap_inventory_summary")
async def get_sap_inventory_summary_tool(
    resource_group: str = None,
    subscription_id: str = None,
    sid: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Get summary of SAP systems and components in Azure.
    
    This tool analyzes Azure resources to identify SAP systems and their components.
    It uses naming patterns and tags to identify different SAP components like databases,
    application servers, and central services.
    
    Args:
        resource_group: Azure resource group to filter resources (optional)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        sid: SAP System ID to filter resources (optional)
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.sap_inventory.inventory_summary import get_sap_inventory_summary
        result = await get_sap_inventory_summary(
            resource_group=resource_group,
            subscription_id=subscription_id,
            sid=sid,
            auth_context=auth_context
        )
        # Refined: Check status and format accordingly
        if isinstance(result, dict) and 'status' in result:
            if result['status'] == 'success':
                # Pass only the summary data to the formatter on success
                return format_result_content(result.get('summary', 'Summary data missing'))
            else:
                # Return the error message from the implementation
                return {
                    "content": [{"type": "text", "text": result.get('message', 'Unknown error occurred')}],
                    "isError": True
                }
        else:
            # Handle unexpected format from implementation
            logging.warning(f"Unexpected result format from get_sap_inventory_summary: {result}")
            return {
                "content": [{"type": "text", "text": "Unexpected result format from tool implementation."}],
                "isError": True
            }
    except ModuleNotFoundError:
        logging.error("Failed to import inventory_summary tool implementation.", exc_info=True)
        return {"content": [{"type": "text", "text": "Tool implementation (inventory_summary) not found."}], "isError": True}
    except Exception as e:
        logging.error(f"Error getting SAP inventory summary: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting SAP inventory summary: {str(e)}"}],
            "isError": True
        }

@mcp.tool("check_sap_vm_compliance")
async def check_sap_vm_compliance_tool(
    vm_name: str,
    sap_component_type: str = "HANA",  # Options: HANA, AnyDB, App
    resource_group: str = None,
    subscription_id: str = None,
    sid: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Check if a VM complies with SAP best practices.
    
    This tool checks an Azure VM against SAP best practices for the specified component type.
    It validates VM series, memory, CPU cores, premium storage, and accelerated networking.
    
    Args:
        vm_name: Azure VM name
        sap_component_type: SAP component type (HANA, AnyDB, App)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        sid: SAP System ID (optional, will use resource group mapping from config if provided)
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.sap_inventory.vm_compliance import check_vm_compliance
        result = await check_vm_compliance(
            vm_name=vm_name,
            sap_component_type=sap_component_type,
            resource_group=resource_group,
            subscription_id=subscription_id,
            sid=sid,
            auth_context=auth_context
        )
        # Refined: Check status and format accordingly
        if isinstance(result, dict) and 'status' in result:
            if result['status'] == 'success':
                # Return the structured success data as JSON
                return {
                    "content": [{"type": "json", "json": json.dumps(result, cls=DecimalEncoder)}], 
                    "isError": False
                }
            else:
                # Return the error message from the implementation
                return {
                    "content": [{"type": "text", "text": result.get('message', 'Unknown error occurred')}],
                    "isError": True
                }
        else:
            # Handle unexpected format from implementation
            logging.warning(f"Unexpected result format from check_vm_compliance: {result}")
            return {
                "content": [{"type": "text", "text": "Unexpected result format from tool implementation."}],
                "isError": True
            }
    except ModuleNotFoundError:
        logging.error("Failed to import vm_compliance tool implementation.", exc_info=True)
        return {"content": [{"type": "text", "text": "Tool implementation (vm_compliance) not found."}], "isError": True}
    except Exception as e:
        logging.error(f"Error checking SAP VM compliance: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error checking SAP VM compliance: {str(e)}"}],
            "isError": True
        }

@mcp.tool("run_sap_workbook_check")
async def run_sap_workbook_check_tool(
    check_name: str, # Name of the check/query to run (must match key in workbook_checker.py)
    vis_id: Optional[str] = None, # Azure Resource ID of the VIS to scope the query
    subscription_ids: Optional[List[str]] = None, # Target subscription ID(s)
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Executes a predefined KQL query check from the SAP Inventory Workbook definition.
    
    Uses Azure Resource Graph to run checks based on the workbook's logic.
    Requires the check name to be predefined in the implementation.
    
    Args:
        check_name (str): The key of the KQL query to execute.
        vis_id (str, optional): The Azure Resource ID of the Virtual Instance for SAP solutions.
        subscription_ids (List[str], optional): Specific subscription IDs to query.
        auth_context (Dict[str, Any], optional): Authentication context.
        
    Returns:
        Dict[str, Any]: Query results or error message.
    """
    try:
        # Import locally
        from tools.sap_inventory.workbook_checker import run_sap_workbook_check
        
        result = await run_sap_workbook_check(
            check_name=check_name,
            vis_id=vis_id,
            subscription_ids=subscription_ids,
            auth_context=auth_context
        )
        
        # Process result
        if isinstance(result, dict) and 'status' in result:
            if result['status'] == 'success':
                # Return the data array as JSON
                return {
                    "content": [{"type": "json", "json": json.dumps(result.get('data', []), cls=DecimalEncoder)}],
                    "isError": False
                }
            else:
                # Return error message
                return {"content": [{"type": "text", "text": result.get('message', 'Unknown error occurred')}], "isError": True}
        else:
            # Handle unexpected format
            logging.warning(f"Unexpected result format from run_sap_workbook_check: {result}")
            return {"content": [{"type": "text", "text": "Unexpected result format from workbook check implementation."}], "isError": True}
            
    except ModuleNotFoundError:
        logging.error("Failed to import workbook_checker tool implementation.", exc_info=True)
        return {"content": [{"type": "text", "text": "Tool implementation (workbook_checker) not found."}], "isError": True}
    except Exception as e:
        logging.error(f"Error running SAP workbook check: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error running SAP workbook check: {str(e)}"}],
            "isError": True
        }

@mcp.tool("run_sap_quality_check")
async def run_sap_quality_check_tool(
    vm_name: str, 
    vm_role: str = "DB", 
    sap_component_type: str = "HANA", 
    resource_group: Optional[str] = None, 
    subscription_id: Optional[str] = None, 
    sid: Optional[str] = None, 
    vm_os: str = "SUSE", 
    high_availability: bool = False, 
    ssh_host: Optional[str] = None, 
    ssh_username: Optional[str] = None, 
    ssh_password: Optional[str] = None, 
    ssh_key_path: Optional[str] = None, 
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Validates SAP on Azure VM deployments against Microsoft's best practices.
    
    Performs comprehensive quality checks on SAP systems in Azure based on Microsoft's recommendations.
    Similar to the PowerShell-based QualityCheck tool but implemented as an MCP tool.
    
    Args:
        vm_name (str): Azure VM name to check
        vm_role (str): SAP component role (DB, ASCS, APP)
        sap_component_type (str): SAP component type (HANA, Oracle, MSSQL, Db2, ASE)
        resource_group (str, optional): Resource group name
        subscription_id (str, optional): Subscription ID
        sid (str, optional): SAP System ID
        vm_os (str): VM operating system (Windows, SUSE, RedHat, OracleLinux)
        high_availability (bool): Whether the system is configured for high availability
        ssh_host (str, optional): SSH hostname or IP (required for extended Linux checks)
        ssh_username (str, optional): SSH username (required for extended Linux checks)
        ssh_password (str, optional): SSH password (optional, either this or ssh_key_path is needed)
        ssh_key_path (str, optional): SSH key path (optional, either this or ssh_password is needed)
        auth_context (Dict[str, Any], optional): Authentication context
        
    Returns:
        Dict[str, Any]: Quality check results or error message
    """
    try:
        # Import locally
        from tools.sap_inventory.quality_check import run_quality_check
        
        result = await run_quality_check(
            vm_name=vm_name,
            vm_role=vm_role,
            sap_component_type=sap_component_type,
            resource_group=resource_group,
            subscription_id=subscription_id,
            sid=sid,
            vm_os=vm_os,
            high_availability=high_availability,
            ssh_host=ssh_host,
            ssh_username=ssh_username,
            ssh_password=ssh_password,
            ssh_key_path=ssh_key_path,
            auth_context=auth_context
        )
        
        # Process result
        if isinstance(result, dict) and 'status' in result:
            if result['status'] == 'success':
                # Return the data as JSON
                return {
                    "content": [{"type": "json", "json": json.dumps(result.get('data', {}), cls=DecimalEncoder)}],
                    "isError": False
                }
            else:
                # Return error message
                return {"content": [{"type": "text", "text": result.get('message', 'Unknown error occurred')}], "isError": True}
        else:
            # Handle unexpected format
            logging.warning(f"Unexpected result format from run_quality_check: {result}")
            return {"content": [{"type": "text", "text": "Unexpected result format from quality check implementation."}], "isError": True}
            
    except ModuleNotFoundError:
        logging.error("Failed to import quality_check module.", exc_info=True)
        return {"content": [{"type": "text", "text": "Quality check implementation not found."}], "isError": True}
    except Exception as e:
        logging.error(f"Error running SAP quality check: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error running SAP quality check: {str(e)}"}],
            "isError": True
        }

@mcp.tool("get_sap_quality_check_definitions")
async def get_sap_quality_check_definitions_tool(
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Get definitions and supported configurations for SAP quality checks.
    
    Returns information about supported VM sizes, OS and database combinations,
    and available checks from the QualityCheck configuration.
    
    Args:
        auth_context (Dict[str, Any], optional): Authentication context
        
    Returns:
        Dict[str, Any]: Quality check definitions or error message
    """
    try:
        # Import locally
        from tools.sap_inventory.quality_check import get_quality_check_definitions
        
        result = await get_quality_check_definitions()
        
        # Process result
        if isinstance(result, dict) and 'status' in result:
            if result['status'] == 'success':
                # Return the data as JSON
                return {
                    "content": [{"type": "json", "json": json.dumps(result.get('data', {}), cls=DecimalEncoder)}],
                    "isError": False
                }
            else:
                # Return error message
                return {"content": [{"type": "text", "text": result.get('message', 'Unknown error occurred')}], "isError": True}
        else:
            # Handle unexpected format
            logging.warning(f"Unexpected result format from get_quality_check_definitions: {result}")
            return {"content": [{"type": "text", "text": "Unexpected result format from quality check implementation."}], "isError": True}
            
    except ModuleNotFoundError:
        logging.error("Failed to import quality_check module.", exc_info=True)
        return {"content": [{"type": "text", "text": "Quality check implementation not found."}], "isError": True}
    except Exception as e:
        logging.error(f"Error getting SAP quality check definitions: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting SAP quality check definitions: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def get_resource_groups(
    subscription_id: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Get Azure resource groups.
    
    This tool retrieves the list of resource groups in an Azure subscription.
    
    Args:
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.azure_tools.resource_info import get_resource_groups as get_resource_groups_impl
        result = await get_resource_groups_impl(
            subscription_id=subscription_id,
            auth_context=auth_context
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error getting resource groups: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting resource groups: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def get_vm_details(
    sid: str = None,
    vm_name: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Get detailed information about an Azure VM.
    
    This tool retrieves detailed information about an Azure VM, including hardware profile,
    storage profile, network interfaces, and disks.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.azure_tools.resource_info import get_vm_details as get_vm_details_impl
        result = await get_vm_details_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error getting VM details: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting VM details: {str(e)}"}],
            "isError": True
        }

@mcp.tool()
async def get_vm_metrics(
    sid: str = None,
    vm_name: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    metric_names: List[str] = None,
    time_grain: str = "PT1H",
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Get metrics for an Azure VM.
    
    This tool retrieves metrics for an Azure VM, such as CPU usage, memory usage,
    disk I/O, and network I/O.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        metric_names: List of metric names to retrieve (optional)
        time_grain: Time grain for metrics (e.g., "PT1H" for 1 hour, "PT5M" for 5 minutes)
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.azure_tools.resource_info import get_vm_metrics as get_vm_metrics_impl
        result = await get_vm_metrics_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            metric_names=metric_names,
            time_grain=time_grain,
            auth_context=auth_context
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error getting VM metrics: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting VM metrics: {str(e)}"}],
            "isError": True
        }

@mcp.tool("add_disk")
async def add_disk(
    sid: str = None,
    vm_name: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None,
    disk_name: str = None,
    disk_size_gb: int = 32,
    disk_type: str = "Standard_LRS",
    lun: int = None,
    caching: str = "None"
) -> Dict[str, Any]:
    """Add a new managed disk to an Azure VM.
    
    This tool creates and attaches a new managed disk to an Azure VM.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
        disk_name: Name for the new disk
        disk_size_gb: Size of the disk in GB
        disk_type: Storage account type (e.g., "Standard_LRS", "Premium_LRS", "StandardSSD_LRS", "UltraSSD_LRS")
        lun: Logical unit number for the disk (optional, will be assigned automatically if not provided)
        caching: Caching type for the disk (e.g., "None", "ReadOnly", "ReadWrite")
    """
    try:
        from tools.azure_tools.vm_operations import add_disk as add_disk_impl
        result = await add_disk_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context,
            disk_name=disk_name,
            disk_size_gb=disk_size_gb,
            disk_type=disk_type,
            lun=lun,
            caching=caching
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error adding disk to VM: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error adding disk to VM: {str(e)}"}],
            "isError": True
        }

@mcp.tool("extend_disk")
async def extend_disk(
    sid: str = None,
    vm_name: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None,
    disk_name: str = None,
    new_disk_size_gb: int = 64
) -> Dict[str, Any]:
    """Extend an existing managed disk on an Azure VM.
    
    This tool resizes an existing managed disk attached to an Azure VM.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
        disk_name: Name of the disk to extend
        new_disk_size_gb: New size of the disk in GB (must be larger than current size)
    """
    try:
        from tools.azure_tools.vm_operations import extend_disk as extend_disk_impl
        result = await extend_disk_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context,
            disk_name=disk_name,
            new_disk_size_gb=new_disk_size_gb
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error extending disk: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error extending disk: {str(e)}"}],
            "isError": True
        }

@mcp.tool("remove_disk")
async def remove_disk(
    sid: str = None,
    vm_name: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None,
    disk_name: str = None,
    delete_disk: bool = False
) -> Dict[str, Any]:
    """Remove a managed disk from an Azure VM.
    
    This tool detaches and optionally deletes a managed disk from an Azure VM.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
        disk_name: Name of the disk to remove
        delete_disk: Whether to delete the disk after detaching it
    """
    try:
        from tools.azure_tools.vm_operations import remove_disk as remove_disk_impl
        result = await remove_disk_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context,
            disk_name=disk_name,
            delete_disk=delete_disk
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error removing disk: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error removing disk: {str(e)}"}],
            "isError": True
        }

@mcp.tool("list_disks")
async def list_disks(
    sid: str = None,
    vm_name: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """List all disks attached to an Azure VM.
    
    This tool retrieves information about all disks attached to an Azure VM,
    including the OS disk and data disks.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
    """
    try:
        from tools.azure_tools.vm_operations import list_disks as list_disks_impl
        result = await list_disks_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error listing disks: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error listing disks: {str(e)}"}],
            "isError": True
        }

@mcp.tool("prepare_disk")
async def prepare_disk(
    sid: str = None,
    vm_name: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None,
    disk_name: str = None,
    mount_point: str = None,
    filesystem: str = "xfs",
    owner: str = None,
    group: str = None,
    permissions: str = "755"
) -> Dict[str, Any]:
    """Format and mount a new disk on a Linux VM.
    
    This tool formats a new disk with the specified filesystem and mounts it at the specified mount point.
    It also adds an entry to /etc/fstab to ensure the disk is mounted on system boot.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
        disk_name: Name of the disk to prepare
        mount_point: Directory where the disk should be mounted
        filesystem: Filesystem type to create (e.g., "ext4", "xfs")
        owner: User owner for the mount point
        group: Group owner for the mount point
        permissions: Permissions for the mount point (e.g., "755")
    """
    try:
        from tools.azure_tools.vm_operations import prepare_disk as prepare_disk_impl
        result = await prepare_disk_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context,
            disk_name=disk_name,
            mount_point=mount_point,
            filesystem=filesystem,
            owner=owner,
            group=group,
            permissions=permissions
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error preparing disk: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error preparing disk: {str(e)}"}],
            "isError": True
        }

@mcp.tool("extend_filesystem")
async def extend_filesystem(
    sid: str = None,
    vm_name: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None,
    disk_name: str = None,
    mount_point: str = None
) -> Dict[str, Any]:
    """Extend a filesystem after resizing the underlying disk.
    
    This tool extends the filesystem on a disk that has been resized using the extend_disk tool.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
        disk_name: Name of the disk with the extended filesystem
        mount_point: Mount point of the filesystem to extend
    """
    try:
        from tools.azure_tools.vm_operations import extend_filesystem as extend_filesystem_impl
        result = await extend_filesystem_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context,
            disk_name=disk_name,
            mount_point=mount_point
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error extending filesystem: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error extending filesystem: {str(e)}"}],
            "isError": True
        }

@mcp.tool("cleanup_disk")
async def cleanup_disk(
    sid: str = None,
    vm_name: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None,
    disk_name: str = None,
    mount_point: str = None,
    force: bool = False
) -> Dict[str, Any]:
    """Unmount and clean up a disk before removal.
    
    This tool unmounts a disk and removes its entry from /etc/fstab before the disk is detached.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
        disk_name: Name of the disk to clean up
        mount_point: Mount point of the disk to clean up
        force: Whether to force unmount even if the disk is busy
    """
    try:
        from tools.azure_tools.vm_operations import cleanup_disk as cleanup_disk_impl
        result = await cleanup_disk_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context,
            disk_name=disk_name,
            mount_point=mount_point,
            force=force
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error cleaning up disk: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error cleaning up disk: {str(e)}"}],
            "isError": True
        }

@mcp.tool("resize_vm")
async def resize_vm(
    sid: str = None,
    vm_name: str = None,
    resource_group: str = None,
    subscription_id: str = None,
    component: str = None,
    auth_context: Dict[str, Any] = None,
    new_size: str = "Standard_D2s_v3",
    wait: bool = True,
    timeout: int = 600
) -> Dict[str, Any]:
    """Resize an Azure VM.
    
    This tool resizes an Azure VM by changing its size/SKU. The VM will be stopped
    before resizing and started again after the operation completes.
    
    Args:
        sid: SAP System ID (optional, will use VM mappings from config if provided)
        vm_name: Azure VM name (optional if sid is provided)
        resource_group: Azure resource group (optional if sid is provided)
        subscription_id: Azure subscription ID (optional, will use default from config if not provided)
        component: Component name (e.g., "db", "app") when using sid
        auth_context: Authentication context with Azure permissions
        new_size: The new VM size to resize to (e.g., "Standard_D2s_v3", "Standard_D4s_v3")
        wait: Whether to wait for the operation to complete
        timeout: Maximum time to wait in seconds
    """
    try:
        from tools.azure_tools.vm_operations import resize_vm as resize_vm_impl
        result = await resize_vm_impl(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            subscription_id=subscription_id,
            component=component,
            auth_context=auth_context,
            new_size=new_size,
            wait=wait,
            timeout=timeout
        )
        return format_result_content(result)
    except Exception as e:
        logging.error(f"Error resizing VM: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error resizing VM: {str(e)}"}],
            "isError": True
        }

async def main():
    """
    Main entry point for the MCP server.
    """
    parser = argparse.ArgumentParser(description='SAP HANA MCP Server')
    parser.add_argument('--transport', choices=['stdio', 'http'], default='http',
                        help='Transport type (stdio or http)')
    parser.add_argument('--host', default='0.0.0.0',
                        help='Host to bind to (HTTP only)')
    parser.add_argument('--port', type=int, default=3000,
                        help='Port to bind to (HTTP only)')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    parser.add_argument('--auto-reload', action='store_true',
                        help='Enable auto-reload for development')
    parser.add_argument('--log-file', default=None,
                        help='Log file path (if not specified, logs to stderr)')
    
    args = parser.parse_args()
    
    # Set up logging to avoid interfering with JSON-RPC communication
    log_handlers = []
    
    # Always log to stderr for development visibility
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(logging.Formatter('STDERR: %(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    log_handlers.append(stderr_handler)
    
    # Add file logging if specified
    if args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        log_handlers.append(file_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        handlers=log_handlers,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Explicitly configure uvicorn logger to prevent it from using stdout
    uvicorn_logger = logging.getLogger("uvicorn")
    for handler in uvicorn_logger.handlers:
        uvicorn_logger.removeHandler(handler)
    for handler in log_handlers:
        uvicorn_logger.addHandler(handler)
    
    # Configure uvicorn.access logger
    uvicorn_access_logger = logging.getLogger("uvicorn.access")
    for handler in uvicorn_access_logger.handlers:
        uvicorn_access_logger.removeHandler(handler)
    for handler in log_handlers:
        uvicorn_access_logger.addHandler(handler)
    
    # Load environment variables
    load_dotenv()
    
    # Perform initialization tasks in the background
    initialize_server()
    
    # Configure transport options based on the selected transport type
    if args.transport == 'stdio':
        # For stdio transport, we'll handle it separately
        return {'transport': 'stdio', 'debug': args.debug}
    else:
        # For HTTP transport, use the sse_app method with Starlette
        from starlette.applications import Starlette
        from starlette.routing import Mount
        from starlette.middleware import Middleware
        from starlette.middleware.cors import CORSMiddleware
        from uvicorn import Config, Server
        import socket
        
        # Function to check if a port is already in use
        def is_port_in_use(port, host='localhost'):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind((host, port))
                    return False
                except socket.error:
                    return True
        
        # Find an available port starting from the provided port
        initial_port = args.port
        port = initial_port
        
        # Try ports in range [initial_port, initial_port + 10]
        max_port = initial_port + 10
        while is_port_in_use(port, args.host) and port < max_port:
            logging.info(f"Port {port} is already in use, trying port {port + 1}")
            port += 1
        
        if port >= max_port:
            logging.error(f"Could not find an available port in range [{initial_port}, {max_port})")
            return {'error': 'no_ports_available'}
        
        if port != initial_port:
            logging.info(f"Using port {port} instead of {initial_port}")
        
        # Configure CORS middleware
        middleware = [
            Middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_methods=["*"],
                allow_headers=["*"],
            )
        ]
        
        # Customize the MCP server to be more responsive during initialization
        mcp_app = mcp.sse_app()
        
        # Create a Starlette app with the MCP SSE app mounted
        app = Starlette(
            routes=[Mount('/', app=mcp_app)],
            middleware=middleware
        )
        
        # Configure and run the Uvicorn server with log configuration
        config = Config(
            app=app, 
            host=args.host, 
            port=port, 
            log_level="debug" if args.debug else "info",
            reload=args.auto_reload,  # Enable auto-reload based on command-line argument
            log_config=None,  # Disable default Uvicorn logging configuration
        )
        server = Server(config)

        #Print the server info
        print(f"Server started on {args.host}:{port}")
        logging.info(f"Server started on {args.host}:{port}")
        print(f"\n=== MCP Server Starting ===")
        print(f"Binding to {args.host}:{port}")
        print (f"To connect from another machine, use {args.host}:{port}")
        print(f"Debug mode: {args.debug}")
        print(f"Auto-reload: {args.auto_reload}")
        print(f"Log file: {args.log_file}")
        print(f"Transport: {args.transport}")
        print(f"\n=== MCP Server Started ===")
        try:
            await server.serve()
        except Exception as e:
            logging.error(f"Error starting server: {str(e)}")
            return {'error': 'server_start_failed', 'message': str(e)}

if __name__ == '__main__':
    import sys
    
    result = asyncio.run(main())
    
    # Handle results
    if result and result.get('error'):
        error_type = result.get('error')
        if error_type == 'no_ports_available':
            sys.stderr.write("ERROR: Could not find an available port for the SAP HANA MCP server\n")
        elif error_type == 'server_start_failed':
            sys.stderr.write(f"ERROR: Failed to start server: {result.get('message', 'Unknown error')}\n")
        sys.stderr.flush()
        sys.exit(1)
    
    # If using stdio transport, handle it separately
    if result and result.get('transport') == 'stdio':
        # Set stderr for logging (Claude Desktop will capture this)
        sys.stderr.write("Starting SAP HANA MCP server with stdio transport...\n")
        sys.stderr.flush()
        
        try:
            # Use the mcp instance to run with stdio transport
            sys.stderr.write("Running MCP server with stdio transport\n")
            sys.stderr.flush()
            
            # The FastMCP class has built-in support for stdio via run_stdio_async
            asyncio.run(mcp.run_stdio_async())
        except Exception as e:
            sys.stderr.write(f"Error in stdio transport: {str(e)}\n")
            import traceback
            sys.stderr.write(traceback.format_exc())
            sys.stderr.flush()
            sys.exit(1)
