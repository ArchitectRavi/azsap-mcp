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
from mcp.server.fastmcp import FastMCP
import logging
import json
import os
from dotenv import load_dotenv
from hana_connection import hana_connection, execute_query, get_table_schema
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
def format_result_content(result: Any) -> List[Dict[str, Any]]:
    """Format result into MCP content format."""
    if isinstance(result, str):
        return [{"type": "text", "text": result}]
    elif isinstance(result, dict):
        if "error" in result:
            return [{"type": "text", "text": f"Error: {result['error']}"}]
        else:
            # Format dictionary as markdown table
            return [{"type": "text", "text": json.dumps(result, indent=2, cls=DecimalEncoder)}]
    elif isinstance(result, list):
        if not result:
            return [{"type": "text", "text": "No results found."}]
        
        if isinstance(result[0], dict):
            # Format list of dictionaries as markdown table
            table_str = "| " + " | ".join(result[0].keys()) + " |\n"
            table_str += "| " + " | ".join(["---"] * len(result[0].keys())) + " |\n"
            
            for row in result:
                # Convert any Decimal values to float
                formatted_values = []
                for val in row.values():
                    if isinstance(val, decimal.Decimal):
                        formatted_values.append(str(float(val)))
                    else:
                        formatted_values.append(str(val))
                
                table_str += "| " + " | ".join(formatted_values) + " |\n"
            
            return [{"type": "text", "text": table_str}]
        else:
            # Format list as bullet points
            bullet_list = "\n".join([f"* {item}" for item in result])
            return [{"type": "text", "text": bullet_list}]
    else:
        # Default formatting for other types
        return [{"type": "text", "text": str(result)}]

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

async def main():
    """
    Main entry point for the MCP server.
    """
    parser = argparse.ArgumentParser(description='SAP HANA MCP Server')
    parser.add_argument('--transport', choices=['stdio', 'http'], default='http',
                        help='Transport type (stdio or http)')
    parser.add_argument('--host', default='localhost',
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
