# SAP on Azure MCP Server for Microsoft Partners

**Note: This project is currently in active development and supports only a limited set of scenarios.**

A simplified Model Context Protocol (MCP) server for SAP HANA built with the Python MCP SDK version 1.2.1.

## Overview

This implementation follows the official MCP guidelines with a focus on simplicity and maintainability. It provides tools for interacting with SAP HANA databases through a standardized protocol that can be used by any MCP-compatible client.

## Features

- **Multiple Transport Options**: Supports both STDIO and HTTP/SSE transports
- **Focus on Core Functionality**: Implements essential tools to keep the codebase clean
- **MCP Standard Compliance**: Built using the official Python MCP SDK 1.2.1
- **Clean Connection Management**: Support for both System DB and Tenant DB
- **Structured Error Handling**: Consistent error reporting across all tools
- **Security Headers**: Proper security headers for HTTP transport
- **Health Monitoring**: Health check endpoint for server status
- **Docker Support**: Ready-to-use Dockerfile for containerized deployment

## Implemented Tools

The server currently implements these essential tools:

### System Management Tools
1. **get_system_overview**: Get comprehensive system status information including host details, service status, and memory usage
2. **get_disk_usage**: Monitor disk space usage across volumes, data files, and log files

### Database Information Tools
1. **get_db_info**: Retrieve database information from the M_DATABASE system view
2. **get_backup_catalog**: Get backup catalog information from the M_BACKUP_CATALOG system view
3. **get_failed_backups**: Retrieve information about failed or canceled backups

### Performance Analysis Tools
1. **get_tablesize_on_disk**: Get table sizes on disk from the PUBLIC.M_TABLE_PERSISTENCE_STATISTICS system view
2. **get_table_used_memory**: Get memory usage by table type (column vs row) from the SYS.M_TABLES system view

### Note
The server is actively being developed and more tools will be added in future releases.

## Project Structure

```
├── server.py               # Main MCP server implementation
├── hana_connection.py      # HANA database connection management
├── requirements.txt        # Project dependencies
├── .env.template           # Environment variables template
├── Dockerfile              # Docker configuration for containerization
├── tools/                  # Directory for future tool implementations
├── resources/              # Directory for future resource implementations
└── prompts/                # Directory for future prompt templates
```

## Getting Started

### Prerequisites

- Python 3.8+
- SAP HANA instance
- MCP-compatible client (e.g., Claude desktop app)

### Installation

1. Clone the repository
2. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
4. Create a `.env` file from the template:
   ```
   cp .env.template .env
   ```
5. Edit the `.env` file with your SAP HANA connection details

### Running the Server

#### Option 1: STDIO Transport (Default)

For local use with desktop clients like Claude Desktop:

```bash
python server.py
```

#### Option 2: HTTP/SSE Transport

For web-based or remote clients:

```bash
python server.py --transport http --host localhost --port 3000
```

Additional options:
- `--debug`: Enable debug logging
- `--host`: Hostname to bind (default: localhost)
- `--port`: Port to bind (default: 3000)

### Docker Deployment

Build and run the server as a Docker container:

```bash
# Build the Docker image
docker build -t sap-hana-mcp .

# Run with environment variables
docker run -p 3000:3000 \
  -e HANA_HOST=your-hana-host \
  -e HANA_PORT=30215 \
  -e HANA_USER=your-user \
  -e HANA_PASSWORD=your-password \
  -e HANA_SCHEMA=your-schema \
  sap-hana-mcp
```

## Testing with MCP Inspector

The MCP Inspector is a visual testing tool for MCP servers developed by Docker. It allows you to interact with the server and test the implemented tools.

### Using MCP Inspector

You can use the MCP Inspector without installing it permanently by using `npx`:

1. Start the SAP HANA MCP server in one terminal:
   ```bash
   python server.py --transport http --host localhost --port 3000
   ```

2. In another terminal, use npx to run the MCP Inspector and connect to your server:
   ```bash
   npx @modelcontextprotocol/inspector http://localhost:3000
   ```

3. The inspector will start both:
   - A client UI (default port 5173)
   - An MCP proxy server (default port 3000)

4. Open your browser to the client UI (typically http://localhost:5173) to use the inspector

5. You can customize the ports if needed:
   ```bash
   CLIENT_PORT=8080 SERVER_PORT=9000 npx @modelcontextprotocol/inspector http://localhost:3000
   ```

### Testing the Server

1. Once the MCP Inspector UI opens in your browser, you'll see the available tools.

2. Test each tool by:
   - Selecting a tool from the list
   - Providing any required parameters
   - Executing the tool and viewing the results

3. Test the dynamic column discovery by trying tools with different SAP HANA instances. The tools will adapt to the available columns in your specific SAP HANA version.

4. For more details on ways to use the inspector, see the [Inspector section of the MCP docs site](https://modelcontextprotocol.io/docs/tools/inspector).

### Troubleshooting

- If you encounter connection issues, ensure that:
  - The server is running and accessible on the specified port
  - Your SAP HANA connection details in the `.env` file are correct
  - You have proper network access to the SAP HANA instance

- For detailed error information, check the server logs which provide comprehensive debugging information about any issues that occur during tool execution.

## Using with Claude Desktop App

1. Ensure you have the latest version of the Claude Desktop app
2. Configure Claude to use the MCP server by adding it to your `claude_desktop_config.json` file:

```json
{
  "mcp_servers": [
    {
      "name": "sap-hana-mcp",
      "command": "python",
      "args": ["/path/to/your/server.py"]
    }
  ]
}
```

## Using with HTTP Transport Clients

Many MCP clients support HTTP/SSE transport. Configure these clients to connect to:

```
http://localhost:3000/mcp
```

### Health Check

The HTTP transport provides a health check endpoint at:

```
http://localhost:3000/health
```

## Claude Desktop Integration

To integrate this MCP server with Claude Desktop, follow these steps:

1. Ensure the server is running with STDIO transport (this is the default configuration)

2. Configure Claude Desktop with the following settings:

```json
{
  "sap-hana": {
    "command": "python3",
    "args": [
      "server.py",
      "--transport", "stdio",
      "--log-file", "mcp-server.log"
    ],
    "env": {
      "PYTHONUNBUFFERED": "1",
      "PYTHONPATH": "."
    },
    "cwd": ".",
    "initializationTimeoutMs": 60000,
    "shutdownTimeoutMs": 15000
  }
}
```

3. Set up your SAP HANA connection environment variables in `.env`:

```env
HANA_HOST=your_host
HANA_PORT=your_port
HANA_USER=SYSTEM
HANA_PASSWORD=your_password
HANA_SCHEMA=your_schema
```

4. Restart Claude Desktop to apply the changes

### Troubleshooting

- Ensure the server is running with STDIO transport
- Verify the Python executable path in the configuration
- Check the server logs for connection issues
- Ensure the working directory points to the server's root directory

## Security Considerations

This server implements several security best practices:

- Input validation for all SQL queries
- Proper error handling without leaking sensitive information
- Security headers for HTTP transport:
  - Content-Security-Policy
  - X-Content-Type-Options
  - X-Frame-Options
  - Strict-Transport-Security

## Future Enhancements

This implementation focuses on core tools. Future enhancements will include:

- **Resources**: For sharing files and data with the LLM
- **Prompts**: For providing specialized templates for different tasks
- **Additional Tools**: For more advanced HANA operations
- **Authentication**: For secure remote connections
- **Progress Reporting**: For long-running operations

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
