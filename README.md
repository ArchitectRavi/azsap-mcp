# SAP on Azure MCP Server for Microsoft Partners

**Note: This project is currently in active development and supports only a limited set of scenarios.**

A comprehensive Model Context Protocol (MCP) server for SAP HANA and Azure operations built with the Python MCP SDK version 1.2.1.

## 🚀 Quick Start

**New to this project?** 
- **Configuration & Setup**: See **[SETUP_GUIDE.md](SETUP_GUIDE.md)** for complete configuration instructions
- **Quick Reference**: See **[CONFIG_REQUIREMENTS.md](CONFIG_REQUIREMENTS.md)** for configuration overview
- **Automated Setup**: Run `setup.ps1` to copy template files automatically

## Overview

This MCP server provides 30+ specialized tools for SAP on Azure administration through a standardized protocol. Built with the official Python MCP SDK 1.2.1, it follows MCP best practices with a focus on simplicity, maintainability, and security. The server can be used by any MCP-compatible client including Claude Desktop, web applications, and custom integrations.

## Architecture & Features

### MCP Protocol Compliance
- **Standard MCP Implementation**: Built using official Python MCP SDK 1.2.1
- **Multiple Transport Options**: Supports both STDIO and HTTP/SSE transports
- **Resource & Tool Support**: Implements MCP tools with structured error handling
- **Health Monitoring**: HTTP transport includes health check endpoints

### Core Capabilities
- **SAP HANA Integration**: Direct connectivity with System DB and Tenant DB support
- **Azure VM Lifecycle**: Complete Azure VM management for SAP workloads
- **SAP System Administration**: SSH-based SAP system monitoring and control
- **Role-Based Access Control**: User authentication and permission management
- **Security**: Comprehensive input validation and security headers
- **Containerization**: Docker-ready deployment configuration

## MCP Tools (30+ Available)

The server implements specialized tools organized by functional area:

### SAP HANA Database Tools (7 tools)
- **System Monitoring**: `get_system_overview`, `get_disk_usage`, `get_db_info`
- **Backup Management**: `get_backup_catalog`, `get_failed_backups`
- **Performance Analysis**: `get_tablesize_on_disk`, `get_table_used_memory`

### Azure VM Management Tools (7 tools)
- **VM Lifecycle**: `start_vm`, `stop_vm`, `restart_vm`, `resize_vm`
- **VM Information**: `list_vms`, `get_vm_status`, `get_vm_details`

### Azure Disk Management Tools (6 tools)
- **Disk Operations**: `add_disk`, `extend_disk`, `remove_disk`
- **Disk Management**: `list_disks`, `prepare_disk`, `cleanup_disk`

### SAP System Management Tools (5 tools)
- **HANA Operations**: `check_hana_status`, `get_hana_version`, `manage_hana_system`
- **System Monitoring**: `check_disk_space`, `check_hana_volumes`

### SAP Quality & Compliance Tools (3 tools)
- **Quality Validation**: `run_sap_quality_check`, `get_sap_quality_check_definitions`
- **Compliance Check**: `check_sap_vm_compliance`

### Azure Resource Management Tools (6 tools)
- **Resource Discovery**: `get_resource_groups`, `get_sap_inventory_summary`
- **Network Security**: `list_nsgs`, `get_nsg_rules`, `add_nsg_rule`, `update_nsg_rule`

*For detailed tool documentation and usage examples, see the [SETUP_GUIDE.md](SETUP_GUIDE.md).*

## Project Structure

```
├── server.py               # Main MCP server implementation
├── hana_connection.py      # HANA database connection management
├── requirements.txt        # Project dependencies
├── .env                    # Environment variables (HANA & Azure credentials)
├── Dockerfile              # Docker configuration for containerization
├── SETUP_GUIDE.md          # Complete configuration setup instructions
├── CONFIG_REQUIREMENTS.md  # Quick configuration reference
├── setup.ps1               # PowerShell setup script for templates
├── config/                 # Configuration files directory
│   ├── *.template.json     # Template configuration files
│   ├── executor_config.json    # SAP systems and SSH configuration
│   ├── azure_config.json       # Azure subscription and VM mappings
│   └── auth_config.json        # User authentication and roles
├── tools/                  # MCP tool implementations
│   ├── command_executor.py     # SSH command execution for SAP systems
│   ├── azure_tools/            # Azure VM and resource management
│   ├── sap_inventory/          # SAP system discovery and inventory
│   └── *.py                    # Individual tool implementations
└── auth/                   # Authentication and authorization
    └── authentication.py       # User authentication and RBAC
```

## Getting Started

### Prerequisites
- Python 3.8+
- SAP HANA instance access
- Azure subscription (for Azure tools)
- MCP-compatible client (Claude Desktop, web apps, etc.)

### Installation & Configuration
```bash
# 1. Clone the repository
git clone <repository-url>
cd azsap-mcp

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure the server (see SETUP_GUIDE.md for details)
# Option A: Use PowerShell script (Windows)
.\setup.ps1

# Option B: Manual configuration
# Copy template files and configure:
# - .env (already exists with samples)
# - config/executor_config.json
# - config/azure_config.json  
# - config/auth_config.json
```

📖 **Complete setup instructions**: [SETUP_GUIDE.md](SETUP_GUIDE.md)

### Running the Server

#### STDIO Transport (Claude Desktop)
```bash
python server.py
```

#### HTTP/SSE Transport (Web clients)
```bash
python server.py --transport http --host localhost --port 3000
```

**Available options:**
- `--debug`: Enable debug logging
- `--host`: Hostname to bind (default: localhost)  
- `--port`: Port to bind (default: 3000)

#### Docker Deployment
```bash
# Build image
docker build -t azsap-mcp .

# Run with environment
docker run -p 3000:3000 \
  -e HANA_HOST=your-host \
  -e HANA_PORT=30215 \
  azsap-mcp
```

## MCP Client Integration

### Claude Desktop App
Add to your `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "azsap-mcp": {
      "command": "python",
      "args": ["/path/to/server.py"],
      "env": {
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
```

### HTTP/SSE Clients
Connect to: `http://localhost:3000/mcp`

Health check: `http://localhost:3000/health`

## Testing & Development

### Using MCP Inspector
Test the server interactively with the visual MCP Inspector:

```bash
# Start server in HTTP mode
python server.py --transport http --port 3000

# Launch inspector (in another terminal)
npx @modelcontextprotocol/inspector http://localhost:3000

# Open browser to http://localhost:5173
```

The inspector provides a web UI to test all available tools with parameter validation and result visualization.

### Development Notes
- All tools include comprehensive error handling and validation
- SQL queries use parameterized statements for security
- Azure operations support both individual VM operations and SID-based system operations
- SSH connections are managed with proper timeout and error handling

For detailed development information, see [SETUP_GUIDE.md](SETUP_GUIDE.md).

## Troubleshooting

### Common Issues
- **Connection Failures**: Verify `.env` configuration and network access to HANA/Azure
- **Authentication Errors**: Check Azure credentials and SAP HANA user permissions  
- **Tool Failures**: Enable debug logging with `--debug` flag for detailed error information
- **Client Integration**: Ensure MCP client configuration matches server transport mode

### Debug Information
```bash
# Enable detailed logging
python server.py --debug

# Check specific configuration
python -c "from hana_connection import test_connection; test_connection()"
```

### Getting Help
- Check server logs for detailed error messages
- Review [SETUP_GUIDE.md](SETUP_GUIDE.md) for configuration details
- Use MCP Inspector for interactive testing and debugging

## Security & Best Practices

### Security Features
- **Input Validation**: Parameterized SQL queries and command sanitization
- **Error Handling**: Structured error responses without sensitive information leakage
- **Authentication**: Role-based access control with user permissions
- **HTTP Security**: Comprehensive security headers (CSP, HSTS, X-Frame-Options)
- **Transport Security**: Support for secure HTTPS/WSS connections

### Best Practices Implemented
- **MCP Compliance**: Follows official MCP specification v1.2.1
- **Error Consistency**: Uniform error format across all tools
- **Resource Management**: Proper connection pooling and cleanup
- **Logging**: Comprehensive debug logging for troubleshooting
- **Configuration Management**: Template-based configuration with validation

## Roadmap & Future Enhancements

### Short Term
- **Enhanced Resources**: File sharing and data exchange with LLM clients
- **Custom Prompts**: Specialized templates for SAP on Azure scenarios
- **Extended Tools**: Additional HANA administrative operations

### Long Term  
- **Multi-tenant Support**: Support for multiple SAP landscapes
- **Progress Reporting**: Real-time status for long-running operations
- **Advanced Analytics**: SAP system performance insights and recommendations
- **Integration APIs**: REST APIs for external tool integration

## Contributing

We welcome contributions! Please:
1. Read the [SETUP_GUIDE.md](SETUP_GUIDE.md) for development setup
2. Follow the existing code patterns and MCP best practices
3. Add tests for new tools and functionality
4. Submit Pull Requests with clear descriptions

## License & Support

This project is in active development. For issues and feature requests, please use the GitHub issue tracker.
