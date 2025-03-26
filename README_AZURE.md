# Azure VM Management Tools for SAP MCP

This module provides tools for managing Azure resources related to SAP systems, including VMs, NSGs, and other Azure resources.

## Features

- VM Operations: Start, stop, restart, and check status of Azure VMs
- NSG Operations: List, get, and modify NSG rules
- Resource Information: Get resource groups, VM details, and metrics

## Configuration

### Setup Azure Configuration

1. Copy the template file to create your configuration:
   ```bash
   cp config/azure_config.template.json config/azure_config.json
   ```

2. Edit the `config/azure_config.json` file with your Azure credentials and system mappings:
   ```json
   {
       "subscription_id": "your-subscription-id",
       "tenant_id": "your-tenant-id",
       "client_id": "your-client-id",
       "client_secret": "your-client-secret",
       "default_resource_group": "your-default-resource-group",
       "systems": {
           "YOUR_SID": {
               "description": "Your SAP System",
               "resource_group": "your-resource-group",
               "components": {
                   "db": {
                       "type": "database",
                       "vm_name": "your-db-vm-name",
                       "nsg_name": "your-db-nsg-name"
                   },
                   "app": {
                       "type": "application",
                       "vm_name": "your-app-vm-name",
                       "nsg_name": "your-app-nsg-name"
                   }
               }
           }
       }
   }
   ```

### Authentication Methods

The tools support multiple authentication methods:

1. **Service Principal**: Provide `tenant_id`, `client_id`, and `client_secret` in the configuration file.
2. **Default Azure Credential**: If service principal credentials are not provided, the tools will use DefaultAzureCredential, which tries multiple authentication methods including environment variables, managed identity, and interactive login.

## Usage

### VM Operations

```python
from tools.azure_tools.vm_operations import get_vm_status, start_vm, stop_vm, restart_vm, list_vms

# Get VM status by SID
status = await get_vm_status(sid="YOUR_SID", component="db")

# Get VM status by VM name
status = await get_vm_status(vm_name="your-vm-name", resource_group="your-resource-group")

# Start VM
result = await start_vm(sid="YOUR_SID", component="db", wait=True)

# Stop VM
result = await stop_vm(sid="YOUR_SID", component="db", deallocate=True, wait=True)

# Restart VM
result = await restart_vm(sid="YOUR_SID", component="db", wait=True)

# List VMs
vms = await list_vms(resource_group="your-resource-group")
```

### NSG Operations

```python
from tools.azure_tools.nsg_operations import get_nsg_rules, add_nsg_rule, remove_nsg_rule, list_nsgs

# Get NSG rules
rules = await get_nsg_rules(nsg_name="your-nsg-name", resource_group="your-resource-group")

# Add NSG rule
result = await add_nsg_rule(
    nsg_name="your-nsg-name",
    resource_group="your-resource-group",
    rule_name="allow-ssh",
    priority=100,
    direction="Inbound",
    access="Allow",
    protocol="Tcp",
    source_address_prefix="*",
    destination_port_range="22"
)

# Remove NSG rule
result = await remove_nsg_rule(
    nsg_name="your-nsg-name",
    resource_group="your-resource-group",
    rule_name="allow-ssh"
)

# List NSGs
nsgs = await list_nsgs(resource_group="your-resource-group")
```

### Resource Information

```python
from tools.azure_tools.resource_info import get_resource_groups, get_vm_details, get_vm_metrics

# Get resource groups
resource_groups = await get_resource_groups()

# Get VM details
vm_details = await get_vm_details(sid="YOUR_SID", component="db")

# Get VM metrics
vm_metrics = await get_vm_metrics(
    sid="YOUR_SID",
    component="db",
    metric_names=["Percentage CPU", "Available Memory Bytes"],
    time_grain="PT1H"
)
```

## Testing

Run the test scripts to verify the functionality:

```bash
python test_azure_vm.py
python test_azure_nsg.py
```

## Dependencies

- azure-identity
- azure-mgmt-compute
- azure-mgmt-network
- azure-mgmt-resource
- azure-mgmt-monitor

## Security Considerations

- The `azure_config.json` file contains sensitive information and should not be committed to version control.
- The file is already added to `.gitignore` to prevent accidental commits.
- Use service principals with the least privilege required for your operations.
- Consider using managed identities in production environments.
