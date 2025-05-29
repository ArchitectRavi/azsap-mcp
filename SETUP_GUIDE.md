# AZSAP-MCP Configuration Setup Guide

## Overview

When cloning the azsap-mcp repository, you need to configure several files to make the MCP server work with your SAP systems and Azure environment. This guide outlines all the required configuration files and their setup process.

## Required Configuration Files

The azsap-mcp project requires the following configuration files to be set up from their respective templates:

### 1. Environment Variables (`.env`)
**Status**: ✅ Already exists with sample data  
**Location**: `/.env`  
**Purpose**: Contains HANA database connection details and Azure credentials

### 2. System Executor Configuration
**Status**: 📋 Needs to be created from template  
**Template**: `/config/executor_config.template.json`  
**Target**: `/config/executor_config.json`  
**Purpose**: Defines SAP systems, SSH connections, and component mappings

### 3. Azure Configuration  
**Status**: 📋 Needs to be created from template  
**Template**: `/config/azure_config.template.json`  
**Target**: `/config/azure_config.json`  
**Purpose**: Contains Azure subscription details and VM mappings for SAP systems

### 4. Authentication Configuration
**Status**: 📋 Needs to be created from template  
**Template**: `/config/auth_config.template.json`  
**Target**: `/config/auth_config.json`  
**Purpose**: Defines user authentication and role-based permissions

## Step-by-Step Configuration

### Step 1: Environment Variables Configuration

The `.env` file already exists with the following structure. Update it with your actual values:

```bash
# SAP HANA Connection Configuration
HANA_HOST=your_hana_host_ip
HANA_PORT=30215
HANA_USER=SYSTEM
HANA_PASSWORD=your_hana_password
HANA_SCHEMA=SAPHANADB

# System Database Configuration (optional)
HANA_SYSTEM_PORT=30213
HANA_SYSTEM_USER=SYSTEM
HANA_SYSTEM_PASSWORD=your_system_password
HANA_SYSTEM_SCHEMA=SYSTEM

# Azure Authentication (choose one method)
AZURE_TENANT_ID=your_tenant_id
AZURE_CLIENT_ID=your_client_id
AZURE_CLIENT_SECRET=your_client_secret

# Server Configuration
SERVER_HOST=0.0.0.0
SERVER_PORT=3000
SERVER_TRANSPORT=http
DEBUG=false
```

### Step 2: System Executor Configuration

Copy and configure the executor configuration:

```powershell
Copy-Item "config\executor_config.template.json" "config\executor_config.json"
```

Edit `config/executor_config.json` with your SAP systems details:

```json
{
    "systems": {
        "YOUR_SID": {
            "description": "Your SAP System",
            "type": "SAP_HANA",
            "sid": "YOUR_SID",
            "ssh": {
                "username": "your_ssh_username",
                "key_file": "path/to/your/private_key.pem",
                "use_key_auth": true,
                "key_requires_passphrase": false
            },
            "sap_users": {
                "sidadm": {
                    "username": "yoursidadm",
                    "password": "your_sidadm_password"
                },
                "dbadm": {
                    "username": "yourhdbadm", 
                    "password": "your_hdbadm_password"
                }
            },
            "components": {
                "db": {
                    "type": "database",
                    "hostname": "your_db_hostname_or_ip",
                    "instance_number": "00"
                },
                "app": {
                    "type": "application",
                    "hostname": "your_app_hostname_or_ip",
                    "instance_number": "00"
                }
            }
        }
    },
    "ssh": {
        "username": "default_ssh_username",
        "password": "",
        "use_key_auth": true,
        "key_requires_passphrase": false,
        "port": 22,
        "connection_timeout": 10
    }
}
```

### Step 3: Azure Configuration

Copy and configure the Azure configuration:

```powershell
Copy-Item "config\azure_config.template.json" "config\azure_config.json"
```

Edit `config/azure_config.json` with your Azure details:

```json
{
    "subscription_id": "your-azure-subscription-id",
    "tenant_id": "your-azure-tenant-id",
    "client_id": "your-service-principal-client-id",
    "client_secret": "your-service-principal-secret",
    "default_resource_group": "your-default-resource-group",
    "systems": {
        "YOUR_SID": {
            "description": "Your SAP System",
            "resource_group": "your-sap-resource-group",
            "components": {
                "db": {
                    "type": "database",
                    "vm_name": "your-database-vm-name",
                    "nsg_name": "your-database-nsg-name"
                },
                "app": {
                    "type": "application", 
                    "vm_name": "your-application-vm-name",
                    "nsg_name": "your-application-nsg-name"
                }
            }
        }
    }
}
```

### Step 4: Authentication Configuration

Copy and configure the authentication settings:

```powershell
Copy-Item "config\auth_config.template.json" "config\auth_config.json"
```

Edit `config/auth_config.json` with your user authentication details:

```json
{
    "users": {
        "admin": "your_admin_password",
        "sap_user": "your_sap_user_password",
        "hana_user": "your_hana_user_password"
    },
    "user_roles": {
        "admin": ["ADMIN"],
        "sap_user": ["SAP_ADMIN"],
        "hana_user": ["HANA_ADMIN"]
    },
    "role_permissions": {
        "ADMIN": [
            "SAP_VIEW", "SAP_START", "SAP_STOP", "SAP_RESTART",
            "HANA_VIEW", "HANA_START", "HANA_STOP", "HANA_RESTART",
            "OS_VIEW", "HANA_ADMIN"
        ],
        "SAP_ADMIN": [
            "SAP_VIEW", "SAP_START", "SAP_STOP", "SAP_RESTART",
            "OS_VIEW"
        ],
        "HANA_ADMIN": [
            "HANA_VIEW", "HANA_START", "HANA_STOP", "HANA_RESTART",
            "OS_VIEW", "HANA_ADMIN"
        ]
    }
}
```

## Configuration Dependencies

### How the MCP Tools Use These Configurations

1. **Command Executor Tools** (`tools/command_executor.py`):
   - Loads `config/executor_config.json` using `load_system_config()`
   - Uses SSH configuration for connecting to SAP systems
   - References system SIDs and component hostnames

2. **Azure Tools** (`tools/azure_tools/auth.py`):
   - Loads `config/azure_config.json` using `get_azure_config()`
   - Uses Azure credentials for VM management operations
   - Maps SAP SIDs to Azure VM names and resource groups

3. **HANA Connection** (`hana_connection.py`):
   - Reads environment variables from `.env` file
   - Establishes database connections using HANA_* variables

4. **Authentication** (`auth/authentication.py`):
   - Loads `config/auth_config.json` for user validation
   - Enforces role-based access control for MCP operations

## Authentication Methods for Azure

The system supports multiple Azure authentication methods (in order of preference):

1. **Azure CLI** (if already logged in)
2. **Service Principal** (using client_id + client_secret)
3. **Federated credentials** (if configured)
4. **Managed Identity** (if running in Azure)
5. **DefaultAzureCredential** (fallback)

## Validation Commands

After configuration, you can validate your setup:

```powershell
# Test the MCP server
python server.py --debug

# Test Azure authentication
python test_azure_vm_cli.py

# Test HANA connection
python -c "from hana_connection import hana_connection; print('HANA connection test:', hana_connection())"
```

## Security Considerations

1. **Never commit sensitive files to version control**:
   - `.env` (contains passwords and secrets)
   - `config/*.json` (contains system details and credentials)

2. **Use secure SSH key authentication** when possible instead of passwords

3. **Follow Azure security best practices** for service principal management

4. **Regularly rotate passwords and secrets**

## Quick Setup Script

For convenience, you can run this PowerShell script to copy all template files:

```powershell
# Copy all template files to their target locations
Copy-Item "config\executor_config.template.json" "config\executor_config.json"
Copy-Item "config\azure_config.template.json" "config\azure_config.json" 
Copy-Item "config\auth_config.template.json" "config\auth_config.json"

Write-Host "Template files copied. Please edit the following files with your actual values:"
Write-Host "1. .env"
Write-Host "2. config\executor_config.json"
Write-Host "3. config\azure_config.json"
Write-Host "4. config\auth_config.json"
```

## Next Steps

After completing the configuration:

1. Install dependencies: `pip install -r requirements.txt`
2. Test your configuration with the validation commands above
3. Run the MCP server: `python server.py`
4. Integrate with your MCP client (e.g., Claude Desktop)

For more details on specific tools and their usage, refer to the main README.md file.
