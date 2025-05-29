#!/usr/bin/env python3
"""
MCP client for Azure operations that connects to a remote MCP server
using the official MCP SDK.
"""
import os
import sys
import json
import asyncio
import logging
import aiohttp
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Dict, List, Any, Optional, Union, Tuple, AsyncGenerator, Literal, Callable

from mcp import ClientSession, StdioServerParameters, types
from mcp import stdio_client

logger = logging.getLogger(__name__)

class SimpleJsonRpcMessage:
    """
    A simple implementation of a JSON-RPC message object that matches what the MCP SDK expects
    """
    def __init__(self, data):
        self.data = data
        
        # Set common attributes expected by MCP SDK
        self.id = data.get("id")
        self.method = data.get("method")
        self.params = data.get("params")
        self.result = data.get("result")
        self.error = data.get("error")
        
        # Determine message type (request/response/notification)
        if "result" in data or "error" in data:
            self.type = "response"
        elif "method" in data and "id" in data:
            self.type = "request"
        elif "method" in data and "id" not in data:
            self.type = "notification"
        else:
            self.type = "unknown"
            
        # Add root property expected by the SDK
        self.root = self
        
        # If this is a response to an initialization request, ensure it has the required fields
        if self.type == "response" and self.result and data.get("method") == "initialize":
            # Make sure result has the expected structure for InitializeResult
            if not isinstance(self.result, dict):
                self.result = {
                    "server_info": {
                        "name": "azsap-mcp",
                        "version": "1.0.0"
                    },
                    "capabilities": {}
                }
            else:
                # Ensure server_info exists
                if "server_info" not in self.result:
                    self.result["server_info"] = {
                        "name": "azsap-mcp",
                        "version": "1.0.0"
                    }
                # Ensure capabilities exists
                if "capabilities" not in self.result:
                    self.result["capabilities"] = {}
        
    def dict(self):
        """Return the message as a dictionary"""
        return self.data
        
    def __str__(self):
        """Return the message as a JSON string"""
        return json.dumps(self.data)


class HttpReader:
    """
    HTTP reader implementation for the MCP SDK following the Streamable HTTP transport protocol
    """
    
    def __init__(self, session, queue, transport):
        """
        Initialize the HTTP reader.
        
        Args:
            session: aiohttp client session
            queue: Asyncio queue for receiving messages
            transport: Parent HttpTransport instance
        """
        self.session = session
        self.queue = queue
        self.transport = transport
        self._sse_task = None
    
    async def read(self) -> str:
        """Read a message from the server (required by MCP SDK)"""
        try:
            # Wait for a message on the queue
            message = await self.queue.get()
            logger.debug(f"Read message from queue: {message[:200]}...")
            return message
        except Exception as e:
            logger.error(f"Error reading message: {str(e)}")
            # Return an error response
            error_json = {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": f"Transport error: {str(e)}"
                },
                "id": None
            }
            return json.dumps(error_json)
    
    async def _process_sse(self):
        """Process SSE events from the server"""
        if not self.transport or not self.transport.sse_response:
            logger.error("No SSE response available for processing")
            return
            
        try:
            logger.debug("Starting SSE event processing")
            async for line in self.transport.sse_response.content:
                line = line.decode('utf-8').strip()
                logger.debug(f"SSE event received: {line}")
                
                # Skip empty lines and comments
                if not line or line.startswith(':'):
                    continue
                
                # Handle data lines
                if line.startswith('data:'):
                    data = line[5:].strip()
                    logger.debug(f"Processing SSE data: {data}")
                    
                    # Process JSON-RPC messages
                    try:
                        # Add the message to the queue
                        await self.queue.put(data)
                    except Exception as e:
                        logger.error(f"Error processing SSE data: {str(e)}")
        except Exception as e:
            logger.error(f"Error in SSE processing: {str(e)}")
        finally:
            logger.debug("SSE event processing stopped")


class HttpWriter:
    """HTTP writer implementation for the MCP SDK following the Streamable HTTP transport protocol"""
    
    def __init__(self, session, queue, base_url, sse_mode=False, transport=None):
        """
        Initialize the HTTP writer.
        
        Args:
            session: aiohttp client session
            queue: Asyncio queue for sending messages
            base_url: MCP endpoint URL
            sse_mode: Whether to use SSE mode
            transport: Parent HttpTransport instance
        """
        self.session = session
        self.queue = queue
        self.base_url = base_url
        self.sse_mode = sse_mode
        self.transport = transport
    
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
    
    async def send(self, data: str) -> None:
        """Send a message to the server (MCP SDK expected method)"""
        if not self.session:
            raise RuntimeError("HTTP session not initialized")
            
        try:
            # Handle both string and JSONRPCMessage objects
            if isinstance(data, str):
                request_str = data
                logger.debug(f"Sending string message: {request_str[:200]}...")
                try:
                    request = json.loads(data)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse request as JSON: {e}")
                    request = {"jsonrpc": "2.0", "method": "unknown", "id": None}
            else:
                # Handle JSONRPCMessage objects by converting to dict then serializing
                try:
                    request = data.dict() if hasattr(data, "dict") else data
                    request_str = json.dumps(request)
                    logger.debug(f"Converted JSONRPCMessage to dict: {request_str[:200]}...")
                except Exception as e:
                    logger.error(f"Error converting message to JSON: {str(e)}")
                    # Fallback to string representation
                    request_str = str(data)
                    request = {"jsonrpc": "2.0", "method": "unknown", "id": None}
            
            method = request.get("method", "")
            logger.debug(f"Processing message with method: {method}")
            
            # Prepare headers according to the protocol
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream"
            }
            
            # Add session ID to headers if available (except for initialize request)
            if method != "initialize" and self.transport and self.transport.session_id:
                headers["Mcp-Session-Id"] = self.transport.session_id
            
            logger.debug(f"Sending request to MCP endpoint with headers: {headers}")
            
            # Per protocol, all requests go to the same MCP endpoint
            try:
                logger.debug(f"HTTP POST to {self.base_url}")
                async with self.session.post(
                    self.base_url, 
                    json=request,
                    headers=headers
                ) as response:
                    logger.debug(f"Got response with status: {response.status}")
                    logger.debug(f"Response headers: {response.headers}")
                    
                    # Check if we got a session ID header in the response
                    session_id = response.headers.get("Mcp-Session-Id")
                    if session_id and self.transport:
                        self.transport.set_session_id(session_id)
                    
                    if response.status == 200:
                        # Process both direct responses and SSE streams
                        content_type = response.headers.get("Content-Type", "")
                        logger.debug(f"Response content type: {content_type}")
                        
                        if "text/event-stream" in content_type:
                            logger.debug("Received SSE stream response")
                            # Handle SSE stream - setup the transport's SSE response if needed
                            if self.sse_mode and self.transport:
                                self.transport.sse_response = response
                                # Start SSE processing in the reader
                                if hasattr(self.transport._reader, "_process_sse"):
                                    self.transport._reader._sse_task = asyncio.create_task(
                                        self.transport._reader._process_sse()
                                    )
                        else:
                            # Handle direct JSON response
                            logger.debug("Receiving direct JSON response")
                            try:
                                response_text = await response.text()
                                logger.debug(f"Raw response text: {response_text[:200]}...")
                                
                                if not response_text:
                                    logger.warning("Empty response received")
                                    response_json = {"jsonrpc": "2.0", "result": None, "id": request.get("id")}
                                else:
                                    response_json = json.loads(response_text)
                                    logger.debug(f"Parsed response JSON: {json.dumps(response_json)[:200]}...")
                            except Exception as e:
                                logger.error(f"Error parsing response: {str(e)}")
                                response_json = {
                                    "jsonrpc": "2.0",
                                    "error": {"code": -32700, "message": f"Parse error: {str(e)}"}, 
                                    "id": request.get("id")
                                }
                            
                            # Special handling for initialization response
                            if method == "initialize":
                                logger.debug("Processing initialization response")
                                # Make sure the whole response is a valid InitializeResult structure
                                if "result" in response_json:
                                    # Create a properly formatted InitializeResult
                                    result = response_json["result"]
                                    
                                    # Ensure structure matches what the SDK expects
                                    if not isinstance(result, dict):
                                        logger.debug("Fixing invalid result format (not a dict)")
                                        response_json["result"] = {
                                            "server_info": {
                                                "name": "azsap-mcp",
                                                "version": "1.0.0"
                                            },
                                            "capabilities": {}
                                        }
                                    else:
                                        # Make sure server_info exists and has required fields
                                        if "server_info" not in result:
                                            logger.debug("Adding missing server_info")
                                            result["server_info"] = {
                                                "name": "azsap-mcp",
                                                "version": "1.0.0"
                                            }
                                        elif not isinstance(result["server_info"], dict):
                                            logger.debug("Fixing invalid server_info format")
                                            result["server_info"] = {
                                                "name": "azsap-mcp", 
                                                "version": "1.0.0"
                                            }
                                        else:
                                            # Ensure name and version exist
                                            if "name" not in result["server_info"]:
                                                result["server_info"]["name"] = "azsap-mcp"
                                            if "version" not in result["server_info"]:
                                                result["server_info"]["version"] = "1.0.0"
                                        
                                        # Make sure capabilities exists
                                        if "capabilities" not in result:
                                            logger.debug("Adding missing capabilities")
                                            result["capabilities"] = {}
                                elif "error" not in response_json:
                                    # If neither result nor error exists, create a valid response
                                    logger.debug("Creating complete initialization response structure")
                                    response_json = {
                                        "jsonrpc": "2.0", 
                                        "result": {
                                            "server_info": {
                                                "name": "azsap-mcp",
                                                "version": "1.0.0"
                                            },
                                            "capabilities": {}
                                        },
                                        "id": request.get("id")
                                    }
                            
                            # Convert response to JSON string and put in queue
                            response_str = json.dumps(response_json)
                            logger.debug(f"Sending to queue: {response_str[:200]}...")
                            await self.queue.put(response_str)
                    elif response.status == 202:
                        # This is an "Accepted" response for notifications/responses
                        # No need to send anything to the queue as no response is expected
                        logger.debug("Server accepted notification/response (status 202)")
                    else:
                        # Handle error responses
                        error_text = await response.text()
                        logger.error(f"HTTP error response: {response.status}, {error_text}")
                        error_json = {
                            "jsonrpc": "2.0",
                            "error": {
                                "code": response.status,
                                "message": f"HTTP {response.status}: {error_text}"
                            },
                            "id": request.get("id")
                        }
                        error_str = json.dumps(error_json)
                        logger.debug(f"Sending error to queue: {error_str}")
                        await self.queue.put(error_str)
            except aiohttp.ClientError as e:
                logger.error(f"HTTP request error: {str(e)}")
                error_json = {
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32603,
                        "message": f"HTTP request error: {str(e)}"
                    },
                    "id": request.get("id")
                }
                await self.queue.put(json.dumps(error_json))
        except Exception as e:
            logger.error(f"Error in HTTP transport writer: {str(e)}")
            error_json = {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": f"Transport error: {str(e)}"
                },
                "id": None
            }
            await self.queue.put(json.dumps(error_json))
            
    # For backward compatibility
    async def __call__(self, data: str) -> None:
        """Call method that delegates to send (backward compatibility)"""
        await self.send(data)

class HttpTransport:
    """
    HTTP transport implementation for the MCP SDK following the Streamable HTTP transport protocol
    """
    def __init__(self, base_url: str, mode: str = "http"):
        """
        Initialize the HTTP transport.
        
        Args:
            base_url: Base URL of the MCP server (e.g., http://localhost:3000)
            mode: Transport mode - "http" or "sse"
        """
        self.base_url = base_url
        
        # Ensure base_url doesn't end with a slash
        if self.base_url.endswith("/"):
            self.base_url = self.base_url[:-1]
        
        # The MCP endpoint is the root path, not /mcp (as shown in server.py)
        self.mcp_endpoint = f"{self.base_url}"
        
        # Transport mode - http or sse
        self.mode = mode
        
        # For SSE mode
        self.session = None
        self.sse_response = None
        self._reader = None
        self._writer = None
        
        # Session ID for ongoing communication
        self.session_id = None
    
    def set_session_id(self, session_id: str):
        """Set the session ID for ongoing communication"""
        self.session_id = session_id
    
    async def __aenter__(self):
        """Enter the transport context and return reader/writer pair"""
        # Create the HTTP session
        self.session = aiohttp.ClientSession()
        
        # Create a message queue for communication between reader and writer
        message_queue = asyncio.Queue()
        
        # Create reader and writer
        self._reader = HttpReader(self.session, message_queue, self)
        self._writer = HttpWriter(
            self.session, 
            message_queue, 
            self.mcp_endpoint,  # Use the MCP endpoint path
            self.mode == "sse",
            self
        )
        
        return self._reader, self._writer
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the transport context and clean up resources"""
        if self.session:
            # Clean up the SSE response if active
            if self.mode == "sse" and self.sse_response:
                try:
                    self.sse_response.close()
                except:
                    pass
                self.sse_response = None
            
            # Close the HTTP session
            await self.session.close()
            self.session = None
        
        # Clean up reader/writer
        self._reader = None
        self._writer = None
    
    async def _establish_sse_connection(self):
        """Establish a Server-Sent Events connection"""
        if self.mode != "sse" or not self.session:
            return
        
        # This is handled by the writer when sending the first message in SSE mode
        pass

class AzureMCPClient:
    """
    MCP client specifically for Azure operations using the official MCP SDK
    to connect to MCP servers via stdio or HTTP transport.
    """
    
    def __init__(
        self,
        host: str = None,
        port: int = None,
        server_id: str = "azsap-mcp",
        # Stdio transport parameters (if using stdio)
        command: str = None,
        args: List[str] = None,
        env: Dict[str, str] = None,
        cwd: str = None,
        # Transport mode for HTTP (http or sse)
        transport_mode: Literal["http", "sse"] = "http",
    ):
        """
        Initialize the client with server connection details
        
        Args:
            host: The hostname or IP address of the remote MCP server (for HTTP transport)
            port: The port number of the remote MCP server (for HTTP transport)
            server_id: Unique identifier for this server connection
            command: Command to execute for stdio transport
            args: Command arguments for stdio transport
            env: Environment variables for stdio transport
            cwd: Working directory for stdio transport
            transport_mode: For HTTP transport, use "http" for standard HTTP or "sse" for Server-Sent Events
        """
        self.server_id = server_id
        self.exit_stack = AsyncExitStack()
        self.session: Optional[ClientSession] = None
        
        # Determine transport type based on provided parameters
        if host and port:
            self.transport_type = "http"
            self.transport_mode = transport_mode
            self.host = host
            self.port = port
            self.base_url = f"http://{host}:{port}"
        elif command:
            self.transport_type = "stdio"
            self.transport_mode = "stdio"
            self.stdio_params = StdioServerParameters(
                command=command,
                args=args or [],
                env=env or {},
                cwd=cwd,
            )
        else:
            raise ValueError("Either HTTP (host+port) or stdio (command) parameters must be provided")
        
        # Connection state
        self.is_connected = False
        self.available_tools = []
        self.transport = None
    
    async def connect(self) -> bool:
        """
        Connect to the MCP server and handle initialization
        
        Returns:
            bool: True if the connection was successful, False otherwise
        """
        max_retries = 3
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            retry_count += 1
            try:
                logger.info(f"Connecting to MCP server {self.server_id} via {self.transport_mode} (attempt {retry_count}/{max_retries})")
                
                # Different connection logic based on transport type
                if self.transport_type == "http":
                    logger.info(f"Connecting to MCP server at {self.base_url} using {self.transport_mode} transport")
                    
                    # Create the transport with more debugging
                    logger.debug("Creating HTTP transport")
                    self.transport = HttpTransport(self.base_url, self.transport_mode)
                    
                    # Connect and get streams
                    logger.debug("Entering transport context to get reader/writer")
                    reader, writer = await self.transport.__aenter__()
                    
                    # Create the client session with positional arguments
                    logger.debug("Creating ClientSession")
                    self.session = ClientSession(reader, writer)
                
                elif self.transport_type == "stdio":
                    logger.info(f"Connecting to MCP server via stdio transport")
                    
                    # Use the MCP SDK's stdio_client function to create the transport
                    logger.debug("Creating stdio transport")
                    transport = await self.exit_stack.enter_async_context(
                        stdio_client(self.stdio_params)
                    )
                    
                    # Get reader and writer from transport
                    reader, writer = transport
                    
                    # Create the client session 
                    logger.debug("Creating ClientSession with stdio transport")
                    self.session = ClientSession(reader, writer)
                else:
                    raise ValueError(f"Unsupported transport type: {self.transport_type}")
                
                # Initialize the session with timeout - don't pass parameters
                logger.debug("Starting initialization")
                init_task = asyncio.create_task(self.session.initialize())
                
                try:
                    # Wait for initialization with timeout - increase from 10s to 30s
                    logger.debug("Waiting for initialization to complete (timeout: 30s)")
                    init_result = await asyncio.wait_for(init_task, timeout=30.0)
                    
                    # Add a small delay to ensure the server is ready
                    logger.debug("Adding a short delay to ensure server is fully initialized")
                    await asyncio.sleep(2.0)
                    
                    # If we got here, initialization succeeded
                    logger.info(f"Successfully connected to MCP server: {init_result}")
                    
                    # Establish SSE connection if needed (HTTP+SSE mode only)
                    if self.transport_type == "http" and self.transport_mode == "sse":
                        logger.debug("Establishing SSE connection")
                        await self.transport._establish_sse_connection()
                    
                    # Successfully initialized
                    self.is_connected = True
                    return True
                
                except asyncio.TimeoutError:
                    logger.error("MCP initialization timed out after 30 seconds")
                    last_error = RuntimeError("Initialization timed out")
                    # Cancel the task to prevent it from running in the background
                    if not init_task.done():
                        logger.debug("Cancelling initialization task")
                        init_task.cancel()
                        try:
                            await init_task
                        except asyncio.CancelledError:
                            pass
                    
                    # Clean up the session - the ClientSession doesn't have a close method
                    # Just set it to None
                    self.session = None
                    
                    # Clean up transport based on type
                    if self.transport_type == "http" and self.transport:
                        logger.debug("Cleaning up HTTP transport")
                        await self.transport.__aexit__(None, None, None)
                        self.transport = None
            except Exception as e:
                logger.error(f"Connection attempt {retry_count} failed: {str(e)}")
                last_error = e
                
                # Clean up any partial connections - the ClientSession doesn't have a close method
                self.session = None
                
                # Clean up transport based on type
                if self.transport_type == "http" and self.transport:
                    logger.debug("Cleaning up HTTP transport after exception")
                    await self.transport.__aexit__(None, None, None)
                    self.transport = None
                # Note: For stdio transport, the exit_stack will handle cleanup
        
        # If we got here, all retries failed
        logger.error(f"Failed to connect to MCP server after {max_retries} attempts")
        
        if last_error:
            logger.error(f"Failed to connect to MCP server: {str(last_error)}")
        
        return False
    
    async def _cleanup_session(self):
        """Clean up the current session without closing the exit stack."""
        self.is_connected = False
        self.available_tools = []
        self.session = None
    
    async def close(self):
        """Close the connection to the MCP server"""
        if self.is_connected:
            self.is_connected = False
            self.available_tools = []
            
            logger.info(f"Disconnected from MCP server {self.server_id}")
        
        # Close the exit stack to release all resources
        await self.exit_stack.aclose()
    
    async def _fetch_tools(self) -> None:
        """
        Fetch available tools from the MCP server
        """
        if not self.is_connected or not self.session:
            raise RuntimeError("Not connected to MCP server")
            
        tools_retry = 3
        retry_delay = 1.0
        
        for tools_attempt in range(1, tools_retry + 1):
            try:
                logger.debug(f"Listing tools (attempt {tools_attempt}/{tools_retry})")
                response = await self.session.list_tools()
                self.available_tools = response.tools
                break
            except Exception as tools_error:
                if tools_attempt == tools_retry:
                    logger.warning(f"Failed to list tools after {tools_retry} attempts: {str(tools_error)}")
                    # Continue with empty tools list rather than failing
                    self.available_tools = []
                else:
                    logger.debug(f"Tool listing attempt {tools_attempt} failed, retrying: {str(tools_error)}")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 1.5
    
    async def list_tools(self) -> List[str]:
        """
        List available tools on the MCP server
        
        Returns:
            List of available tool names
        """
        if not self.is_connected:
            await self.connect()
            
        # If we have already fetched tools, return their names
        if self.available_tools:
            return [tool.name for tool in self.available_tools]
            
        # Otherwise, fetch tools and return their names
        await self._fetch_tools()
        return [tool.name for tool in self.available_tools]
    
    async def execute_tool(self, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a tool on the MCP server
        
        Args:
            tool_name: Name of the tool to execute
            params: Parameters to pass to the tool
            
        Returns:
            Dict: Tool execution result
            
        Raises:
            Exception: If tool execution fails
        """
        if not self.is_connected:
            await self.connect()
            
        logger.info(f"Executing tool {tool_name} with parameters: {params}")
        
        try:
            # Call the tool using the SDK
            result = await self.session.call_tool(tool_name, params)
            
            # Log the raw result for debugging
            logger.debug(f"Raw result from tool {tool_name}: {result}")
            
            # Format the result
            if hasattr(result, 'content'):
                return {
                    "content": result.content,
                    "isError": False
                }
            else:
                # Handle case where result doesn't have content attribute
                logger.warning(f"Tool {tool_name} returned result without content attribute")
                return {
                    "content": [{"type": "text", "text": str(result)}],
                    "isError": False
                }
                
        except Exception as e:
            logger.error(f"Failed to execute tool {tool_name}: {str(e)}")
            return {
                "content": [{"type": "text", "text": f"Failed to execute tool {tool_name}: {str(e)}"}],
                "isError": True
            }
    
    # Convenience methods for specific Azure operations
    
    async def get_vm_status(self, sid: str, component: str, resource_group: str) -> Dict[str, Any]:
        """
        Get the status of an Azure VM
        """
        return await self.execute_tool("get_vm_status", {
            "sid": sid,
            "component": component,
            "resource_group": resource_group
        })
    
    async def list_vms(self, sid: str, resource_group: str) -> Dict[str, Any]:
        """
        List all VMs in a resource group
        """
        return await self.execute_tool("list_vms", {
            "sid": sid,
            "resource_group": resource_group
        })
    
    async def start_vm(self, sid: str, component: str, resource_group: str) -> Dict[str, Any]:
        """
        Start an Azure VM
        """
        return await self.execute_tool("start_vm", {
            "sid": sid,
            "component": component,
            "resource_group": resource_group
        })
    
    async def stop_vm(self, sid: str, component: str, resource_group: str) -> Dict[str, Any]:
        """
        Stop an Azure VM
        """
        return await self.execute_tool("stop_vm", {
            "sid": sid,
            "component": component,
            "resource_group": resource_group,
            "deallocate": True  # Default to deallocating to save costs
        })
        
    async def __aenter__(self):
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
