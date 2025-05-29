# AZSAP-MCP Configuration Summary

## Required Configuration Files for New Users

When cloning the azsap-mcp repository, you must configure **4 files** before the MCP server can work:

### 🔧 Configuration Files Overview

| File | Status | Purpose | Required Data |
|------|--------|---------|---------------|
| `.env` | ✅ **Exists** | HANA & Azure credentials | Database connection, Azure auth |
| `config/executor_config.json` | 📋 **Create from template** | SAP systems & SSH | System SIDs, hostnames, SSH keys |
| `config/azure_config.json` | 📋 **Create from template** | Azure subscription & VMs | Subscription ID, VM names, resource groups |
| `config/auth_config.json` | 📋 **Create from template** | User authentication | User passwords, roles, permissions |

### 🚀 Quick Setup

Run the setup script to copy templates:
```powershell
.\setup.ps1
```

Or manually copy files:
```powershell
Copy-Item "config\*.template.json" "config\" -Force
Get-ChildItem "config\*.template.json" | ForEach-Object { 
    $target = $_.Name -replace "\.template", ""
    Copy-Item $_.FullName "config\$target"
}
```

### 📝 What You Need to Provide

#### For `.env` file:
- HANA database host IP and credentials
- Azure tenant ID, client ID, and secret

#### For `executor_config.json`:
- SAP System IDs (SIDs)
- SSH usernames and private key paths
- SAP system hostnames/IPs
- SAP user passwords (sidadm, hdbadm)

#### For `azure_config.json`:
- Azure subscription ID
- Resource group names
- VM names for each SAP component
- Network security group names

#### For `auth_config.json`:
- User passwords for MCP access
- Role assignments

### 🔗 How MCP Tools Use These Configs

- **Command Executor**: Uses `executor_config.json` for SSH connections to SAP systems
- **Azure Tools**: Uses `azure_config.json` for VM management operations
- **HANA Connection**: Uses `.env` variables for database connections
- **Authentication**: Uses `auth_config.json` for user access control

### ⚠️ Security Notes

- **Never commit these files to Git** (they contain secrets)
- Use SSH keys instead of passwords when possible
- Regularly rotate credentials
- Follow Azure security best practices

### 🧪 Test Your Configuration

```powershell
# Test MCP server
python server.py --debug

# Test Azure connection
python test_azure_vm_cli.py

# Test HANA connection
python -c "from hana_connection import hana_connection; print(hana_connection())"
```

### 📖 Full Documentation

See `SETUP_GUIDE.md` for complete step-by-step configuration instructions.
