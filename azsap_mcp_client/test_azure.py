#!/usr/bin/env python3
"""
Test script for Azure MCP client using the official MCP SDK
"""
import os
import sys
import asyncio
import logging
import argparse
from dotenv import load_dotenv

# Add the parent directory to Python path to resolve imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from azsap_mcp_client.client import AzureMCPClient

# Load environment variables from .env file
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Server configuration
SERVER_HOST = os.getenv("SERVER_HOST", "10.30.30.188")
SERVER_PORT = int(os.getenv("SERVER_PORT", 3000))

async def test_azure_mcp(transport_mode="http", host=SERVER_HOST, port=SERVER_PORT, use_stdio=False):
    """Test Azure functionality through MCP protocol"""
    
    if use_stdio:
        print("Using stdio transport to connect to local MCP server")
        # Create client with stdio transport
        client = AzureMCPClient(
            server_id="azsap-mcp",
            command="python",
            args=["server.py"],  # The server script
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
    else:
        print(f"Connecting to MCP server at {host}:{port} using {transport_mode} transport")
        # Create client with specified HTTP/SSE transport
        client = AzureMCPClient(
            host=host, 
            port=port,
            server_id="azsap-mcp",
            transport_mode=transport_mode
        )
    
    try:
        # Connect to the server
        await client.connect()
        
        # List available tools
        tools = await client.list_tools()
        logger.info(f"Available tools: {tools}")
        
        # Test VM status
        vm_status = await client.get_vm_status(
            sid="D54",  # Example SAP system ID
            component="db", 
            resource_group="s45-1-rg"  # Example resource group
        )
        logger.info(f"VM status response: {vm_status}")
        
        # List VMs
        vms = await client.list_vms(
            sid="D54",  # Example SAP system ID
            resource_group="s45-1-rg"  # Example resource group
        )
        logger.info(f"VMs list response: {vms}")
        
        # Close the client
        await client.close()
        
    except Exception as e:
        logger.error(f"Error during MCP test: {str(e)}")
        # Close the client if it exists
        if hasattr(client, 'close'):
            await client.close()

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Test Azure MCP client')
    parser.add_argument('--transport', choices=['http', 'sse'], default='http',
                      help='Transport mode to use (http or sse)')
    parser.add_argument('--host', default=SERVER_HOST,
                       help='MCP server hostname or IP address')
    parser.add_argument('--port', type=int, default=SERVER_PORT,
                       help='MCP server port number')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--stdio', action='store_true',
                       help='Use stdio transport instead of HTTP/SSE')
    
    args = parser.parse_args()
    
    # Set logging level
    logging_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=logging_level)
    
    # Run the test
    asyncio.run(test_azure_mcp(
        transport_mode=args.transport,
        host=args.host,
        port=args.port,
        use_stdio=args.stdio
    ))
