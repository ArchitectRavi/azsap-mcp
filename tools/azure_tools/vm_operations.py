#!/usr/bin/env python3
"""
Azure VM Operations Module

This module provides functions for managing Azure VMs related to SAP systems,
including starting, stopping, and checking the status of VMs.
"""
import logging
import asyncio
from typing import Dict, Any, List, Optional, Union

from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import VirtualMachine
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

from tools.azure_tools.auth import (
    get_azure_credential, 
    get_subscription_id, 
    get_resource_group,
    get_vm_name
)
from tools.command_executor import get_system_info

# Configure logging
logger = logging.getLogger(__name__)

async def get_vm_status(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    component: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Get the status of an Azure VM
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: VM status information
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_VIEW", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_VIEW permission required"
                    }
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Get VM instance view
        vm = compute_client.virtual_machines.get(resource_group, vm_name, expand="instanceView")
        
        # Extract status information
        statuses = vm.instance_view.statuses if vm.instance_view else []
        power_state = next((s.display_status for s in statuses if s.code.startswith("PowerState/")), "Unknown")
        provision_state = next((s.display_status for s in statuses if s.code.startswith("ProvisioningState/")), "Unknown")
        
        # Get VM details
        vm_size = vm.hardware_profile.vm_size
        os_type = vm.storage_profile.os_disk.os_type
        location = vm.location
        
        # Get network interfaces
        network_interfaces = []
        if vm.network_profile and vm.network_profile.network_interfaces:
            for nic in vm.network_profile.network_interfaces:
                nic_id = nic.id
                nic_name = nic_id.split("/")[-1] if nic_id else "Unknown"
                network_interfaces.append(nic_name)
        
        return {
            "status": "success",
            "vm_status": {
                "name": vm_name,
                "resource_group": resource_group,
                "power_state": power_state,
                "provisioning_state": provision_state,
                "size": vm_size,
                "os_type": os_type,
                "location": location,
                "network_interfaces": network_interfaces
            }
        }
    except ResourceNotFoundError as e:
        logger.error(f"VM not found: {e}")
        return {
            "status": "error",
            "message": f"VM not found: {vm_name}"
        }
    except Exception as e:
        logger.error(f"Error getting VM status: {e}")
        return {
            "status": "error",
            "message": f"Error getting VM status: {str(e)}"
        }

async def start_vm(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    component: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None,
    wait: bool = True,
    timeout: int = 300
) -> Dict[str, Any]:
    """
    Start an Azure VM
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        wait (bool): Whether to wait for the operation to complete. Defaults to True.
        timeout (int): Maximum time to wait in seconds. Defaults to 300.
        
    Returns:
        Dict[str, Any]: Operation result
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_START", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_START permission required"
                    }
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Check current VM status
        vm_status = await get_vm_status(sid, vm_name, resource_group, subscription_id, component)
        if vm_status["status"] == "error":
            return vm_status
            
        current_power_state = vm_status["vm_status"]["power_state"]
        if "running" in current_power_state.lower():
            return {
                "status": "success",
                "message": f"VM {vm_name} is already running"
            }
        
        # Start the VM
        logger.info(f"Starting VM {vm_name} in resource group {resource_group}")
        start_result = compute_client.virtual_machines.begin_start(resource_group, vm_name)
        
        # Wait for the operation to complete if requested
        if wait:
            logger.info(f"Waiting for VM {vm_name} to start (timeout: {timeout}s)")
            start_time = asyncio.get_event_loop().time()
            
            while asyncio.get_event_loop().time() - start_time < timeout:
                # Check if the operation is done
                if start_result.done():
                    break
                    
                # Wait before checking again
                await asyncio.sleep(10)
                
                # Check VM status
                status_result = await get_vm_status(sid, vm_name, resource_group, subscription_id, component)
                if status_result["status"] == "success":
                    current_power_state = status_result["vm_status"]["power_state"]
                    if "running" in current_power_state.lower():
                        return {
                            "status": "success",
                            "message": f"VM {vm_name} started successfully",
                            "vm_status": status_result["vm_status"]
                        }
            
            # Timeout reached
            return {
                "status": "error",
                "message": f"Timeout waiting for VM {vm_name} to start"
            }
        
        return {
            "status": "success",
            "message": f"VM {vm_name} start operation initiated"
        }
    except ResourceNotFoundError as e:
        logger.error(f"VM not found: {e}")
        return {
            "status": "error",
            "message": f"VM not found: {vm_name}"
        }
    except Exception as e:
        logger.error(f"Error starting VM: {e}")
        return {
            "status": "error",
            "message": f"Error starting VM: {str(e)}"
        }

async def stop_vm(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    component: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None,
    deallocate: bool = True,
    wait: bool = True,
    timeout: int = 300
) -> Dict[str, Any]:
    """
    Stop an Azure VM
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        deallocate (bool): Whether to deallocate the VM. Defaults to True.
        wait (bool): Whether to wait for the operation to complete. Defaults to True.
        timeout (int): Maximum time to wait in seconds. Defaults to 300.
        
    Returns:
        Dict[str, Any]: Operation result
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_STOP", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_STOP permission required"
                    }
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Check current VM status
        vm_status = await get_vm_status(sid, vm_name, resource_group, subscription_id, component)
        if vm_status["status"] == "error":
            return vm_status
            
        current_power_state = vm_status["vm_status"]["power_state"]
        if "stopped" in current_power_state.lower() or "deallocated" in current_power_state.lower():
            return {
                "status": "success",
                "message": f"VM {vm_name} is already stopped"
            }
        
        # Stop the VM
        logger.info(f"Stopping VM {vm_name} in resource group {resource_group} (deallocate: {deallocate})")
        
        if deallocate:
            stop_result = compute_client.virtual_machines.begin_deallocate(resource_group, vm_name)
        else:
            stop_result = compute_client.virtual_machines.begin_power_off(resource_group, vm_name)
        
        # Wait for the operation to complete if requested
        if wait:
            logger.info(f"Waiting for VM {vm_name} to stop (timeout: {timeout}s)")
            start_time = asyncio.get_event_loop().time()
            
            while asyncio.get_event_loop().time() - start_time < timeout:
                # Check if the operation is done
                if stop_result.done():
                    break
                    
                # Wait before checking again
                await asyncio.sleep(10)
                
                # Check VM status
                status_result = await get_vm_status(sid, vm_name, resource_group, subscription_id, component)
                if status_result["status"] == "success":
                    current_power_state = status_result["vm_status"]["power_state"]
                    if "stopped" in current_power_state.lower() or "deallocated" in current_power_state.lower():
                        return {
                            "status": "success",
                            "message": f"VM {vm_name} stopped successfully",
                            "vm_status": status_result["vm_status"]
                        }
            
            # Timeout reached
            return {
                "status": "error",
                "message": f"Timeout waiting for VM {vm_name} to stop"
            }
        
        return {
            "status": "success",
            "message": f"VM {vm_name} stop operation initiated"
        }
    except ResourceNotFoundError as e:
        logger.error(f"VM not found: {e}")
        return {
            "status": "error",
            "message": f"VM not found: {vm_name}"
        }
    except Exception as e:
        logger.error(f"Error stopping VM: {e}")
        return {
            "status": "error",
            "message": f"Error stopping VM: {str(e)}"
        }

async def restart_vm(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    component: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None,
    wait: bool = True,
    timeout: int = 600
) -> Dict[str, Any]:
    """
    Restart an Azure VM
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        wait (bool): Whether to wait for the operation to complete. Defaults to True.
        timeout (int): Maximum time to wait in seconds. Defaults to 600.
        
    Returns:
        Dict[str, Any]: Operation result
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_RESTART", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_RESTART permission required"
                    }
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Restart the VM
        logger.info(f"Restarting VM {vm_name} in resource group {resource_group}")
        restart_result = compute_client.virtual_machines.begin_restart(resource_group, vm_name)
        
        # Wait for the operation to complete if requested
        if wait:
            logger.info(f"Waiting for VM {vm_name} to restart (timeout: {timeout}s)")
            start_time = asyncio.get_event_loop().time()
            
            while asyncio.get_event_loop().time() - start_time < timeout:
                # Check if the operation is done
                if restart_result.done():
                    break
                    
                # Wait before checking again
                await asyncio.sleep(10)
                
                # Check VM status
                status_result = await get_vm_status(sid, vm_name, resource_group, subscription_id, component)
                if status_result["status"] == "success":
                    current_power_state = status_result["vm_status"]["power_state"]
                    if "running" in current_power_state.lower():
                        return {
                            "status": "success",
                            "message": f"VM {vm_name} restarted successfully",
                            "vm_status": status_result["vm_status"]
                        }
            
            # Timeout reached
            return {
                "status": "error",
                "message": f"Timeout waiting for VM {vm_name} to restart"
            }
        
        return {
            "status": "success",
            "message": f"VM {vm_name} restart operation initiated"
        }
    except ResourceNotFoundError as e:
        logger.error(f"VM not found: {e}")
        return {
            "status": "error",
            "message": f"VM not found: {vm_name}"
        }
    except Exception as e:
        logger.error(f"Error restarting VM: {e}")
        return {
            "status": "error",
            "message": f"Error restarting VM: {str(e)}"
        }

async def list_vms(
    sid: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    List Azure VMs
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: List of VMs
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_VIEW", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_VIEW permission required"
                    }
        
        # Get resource group from config if not provided and SID is provided
        if sid and not resource_group:
            try:
                resource_group = get_resource_group(sid, resource_group)
            except Exception as e:
                logger.warning(f"Could not get resource group for SID {sid}: {e}")
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # List VMs
        vms = []
        
        if resource_group:
            logger.info(f"Listing VMs in resource group {resource_group}")
            vm_list = compute_client.virtual_machines.list(resource_group)
        else:
            logger.info(f"Listing VMs in subscription {subscription_id}")
            vm_list = compute_client.virtual_machines.list_all()
        
        for vm in vm_list:
            vm_resource_group = vm.id.split("/")[4] if vm.id else "Unknown"
            
            # Get VM status
            try:
                instance_view = compute_client.virtual_machines.get(
                    vm_resource_group, 
                    vm.name, 
                    expand="instanceView"
                ).instance_view
                
                statuses = instance_view.statuses if instance_view else []
                power_state = next((s.display_status for s in statuses if s.code.startswith("PowerState/")), "Unknown")
                provision_state = next((s.display_status for s in statuses if s.code.startswith("ProvisioningState/")), "Unknown")
            except Exception as e:
                logger.warning(f"Could not get status for VM {vm.name}: {e}")
                power_state = "Unknown"
                provision_state = "Unknown"
            
            vms.append({
                "name": vm.name,
                "resource_group": vm_resource_group,
                "location": vm.location,
                "size": vm.hardware_profile.vm_size if vm.hardware_profile else "Unknown",
                "power_state": power_state,
                "provisioning_state": provision_state
            })
        
        return {
            "status": "success",
            "vms": vms
        }
    except Exception as e:
        logger.error(f"Error listing VMs: {e}")
        return {
            "status": "error",
            "message": f"Error listing VMs: {str(e)}"
        }

async def resize_vm(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    component: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None,
    new_size: str = "Standard_D2s_v3",
    wait: bool = True,
    timeout: int = 600
) -> Dict[str, Any]:
    """
    Resize an Azure VM
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        new_size (str): New VM size to resize to. Defaults to "Standard_D2s_v3".
        wait (bool): Whether to wait for the operation to complete. Defaults to True.
        timeout (int): Maximum time to wait in seconds. Defaults to 600.
        
    Returns:
        Dict[str, Any]: Operation result
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_MANAGE", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_MANAGE permission required"
                    }
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Get current VM status
        vm = compute_client.virtual_machines.get(resource_group, vm_name, expand="instanceView")
        current_size = vm.hardware_profile.vm_size
        
        # Check if VM needs to be stopped
        statuses = vm.instance_view.statuses if vm.instance_view else []
        power_state = next((s.display_status for s in statuses if s.code.startswith("PowerState/")), "Unknown")
        
        if power_state != "VM deallocated":
            # Stop the VM first
            logger.info(f"Stopping VM {vm_name} before resizing")
            stop_result = await stop_vm(
                vm_name=vm_name,
                resource_group=resource_group,
                subscription_id=subscription_id,
                auth_context=auth_context,
                wait=True,
                deallocate=True
            )
            if stop_result["status"] != "success":
                return {
                    "status": "error",
                    "message": f"Failed to stop VM: {stop_result['message']}"
                }
        
        # Resize the VM
        logger.info(f"Resizing VM {vm_name} from {current_size} to {new_size}")
        vm.hardware_profile.vm_size = new_size
        
        # Update the VM
        async_operation = compute_client.virtual_machines.begin_create_or_update(
            resource_group_name=resource_group,
            vm_name=vm_name,
            parameters=vm
        )
        
        if wait:
            vm = async_operation.result(timeout=timeout)
            
            # Start the VM again
            logger.info(f"Starting VM {vm_name} after resize")
            start_result = await start_vm(
                vm_name=vm_name,
                resource_group=resource_group,
                subscription_id=subscription_id,
                auth_context=auth_context,
                wait=True
            )
            if start_result["status"] != "success":
                return {
                    "status": "error",
                    "message": f"Failed to start VM after resize: {start_result['message']}"
                }
                
            return {
                "status": "success",
                "message": f"Successfully resized VM from {current_size} to {new_size}",
                "previous_size": current_size,
                "new_size": new_size
            }
        else:
            return {
                "status": "pending",
                "message": "VM resize operation initiated",
                "operation_id": async_operation.operation_id
            }
    except ResourceNotFoundError as e:
        return {
            "status": "error",
            "message": f"VM not found: {vm_name}"
        }
    except HttpResponseError as e:
        return {
            "status": "error",
            "message": f"Failed to resize VM: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Unexpected error resizing VM: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }

# Mapping of disk type shorthand to Azure values
DISK_TYPES = {
    "standard": "Standard_LRS",
    "standard_ssd": "StandardSSD_LRS",
    "premium": "Premium_LRS",
    "premium_v2": "PremiumV2_LRS",
    "ultra": "UltraSSD_LRS"
}

async def add_disk(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    component: Optional[str] = None,
    disk_name: Optional[str] = None,
    disk_size_gb: int = 128,
    disk_type: str = "Standard_LRS",
    lun: Optional[int] = None,
    caching: str = "ReadWrite",
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Add a new managed disk to an Azure VM
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        disk_name (str, optional): Name for the new disk. Defaults to None (auto-generated).
        disk_size_gb (int): Size of the disk in GB. Defaults to 128.
        disk_type (str): Type of disk. Defaults to "Standard_LRS".
        lun (int, optional): Logical Unit Number for the disk. Defaults to None (auto-assigned).
        caching (str): Disk caching type. Defaults to "ReadWrite".
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: Operation result
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_MANAGE", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_MANAGE permission required"
                    }
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Get VM to determine location and for attaching disk
        vm = compute_client.virtual_machines.get(resource_group, vm_name)
        location = vm.location
        
        # Auto-generate disk name if not provided
        if not disk_name:
            disk_name = f"{vm_name}-disk-{int(time.time())}"
        
        # Resolve disk type - allow shorthand or full Azure value
        if disk_type.lower() in DISK_TYPES:
            disk_type = DISK_TYPES[disk_type.lower()]
        
        # Create the disk
        logger.info(f"Creating disk {disk_name} with size {disk_size_gb}GB and type {disk_type}")
        disk_creation = compute_client.disks.begin_create_or_update(
            resource_group,
            disk_name,
            {
                'location': location,
                'disk_size_gb': disk_size_gb,
                'creation_data': {
                    'create_option': 'Empty'
                },
                'sku': {
                    'name': disk_type
                }
            }
        )
        disk = disk_creation.result()
        
        # Get existing data disks
        data_disks = vm.storage_profile.data_disks or []
        
        # Auto-assign LUN if not specified
        if lun is None:
            used_luns = set(disk.lun for disk in data_disks)
            lun = next((i for i in range(64) if i not in used_luns), None)
            if lun is None:
                return {
                    "status": "error",
                    "message": "No available LUN found. VM has maximum number of disks attached."
                }
        
        # Check if LUN is already in use
        if any(disk.lun == lun for disk in data_disks):
            return {
                "status": "error",
                "message": f"LUN {lun} is already in use."
            }
        
        # Attach the disk to the VM
        logger.info(f"Attaching disk {disk_name} to VM {vm_name} at LUN {lun}")
        data_disks.append({
            'lun': lun,
            'name': disk_name,
            'create_option': 'Attach',
            'managed_disk': {
                'id': disk.id
            },
            'caching': caching
        })
        
        # Update the VM
        vm_update = compute_client.virtual_machines.begin_update(
            resource_group,
            vm_name,
            {
                'storage_profile': {
                    'data_disks': data_disks
                }
            }
        )
        updated_vm = vm_update.result()
        
        # Find the attached disk in the updated VM
        attached_disk = next((disk for disk in updated_vm.storage_profile.data_disks if disk.name == disk_name), None)
        
        return {
            "status": "success",
            "message": f"Successfully added disk {disk_name} to VM {vm_name}",
            "disk_details": {
                "name": disk_name,
                "id": disk.id,
                "size_gb": disk_size_gb,
                "type": disk_type,
                "lun": lun,
                "caching": caching
            }
        }
    except ResourceNotFoundError as e:
        return {
            "status": "error",
            "message": f"Resource not found: {str(e)}"
        }
    except HttpResponseError as e:
        return {
            "status": "error",
            "message": f"Failed to add disk: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Unexpected error adding disk: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }

async def extend_disk(
    sid: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    disk_name: str = None,
    new_disk_size_gb: int = 256,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Extend/resize an existing Azure managed disk
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        disk_name (str): Name of the disk to resize. Required.
        new_disk_size_gb (int): New size of the disk in GB. Defaults to 256.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: Operation result
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_MANAGE", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_MANAGE permission required"
                    }
        
        # Validate required parameters
        if not disk_name:
            return {
                "status": "error",
                "message": "Disk name is required"
            }
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Get the current disk
        try:
            disk = compute_client.disks.get(resource_group, disk_name)
        except ResourceNotFoundError:
            return {
                "status": "error",
                "message": f"Disk {disk_name} not found in resource group {resource_group}"
            }
            
        # Check if new size is smaller than current size
        current_size = disk.disk_size_gb
        if new_disk_size_gb <= current_size:
            return {
                "status": "error",
                "message": f"New size ({new_disk_size_gb} GB) must be larger than current size ({current_size} GB)"
            }
        
        # Update the disk size
        logger.info(f"Resizing disk {disk_name} from {current_size} GB to {new_disk_size_gb} GB")
        disk.disk_size_gb = new_disk_size_gb
        
        # Apply the update
        disk_update = compute_client.disks.begin_create_or_update(
            resource_group,
            disk_name,
            disk
        )
        updated_disk = disk_update.result()
        
        return {
            "status": "success",
            "message": f"Successfully resized disk {disk_name}",
            "disk_details": {
                "name": disk_name,
                "id": updated_disk.id,
                "previous_size_gb": current_size,
                "new_size_gb": updated_disk.disk_size_gb,
                "type": updated_disk.sku.name if updated_disk.sku else "Unknown"
            },
            "note": "The OS may need to rescan the disk and extend the filesystem to use the new space"
        }
    except ResourceNotFoundError as e:
        return {
            "status": "error",
            "message": f"Resource not found: {str(e)}"
        }
    except HttpResponseError as e:
        return {
            "status": "error",
            "message": f"Failed to resize disk: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Unexpected error resizing disk: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }

async def remove_disk(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    component: Optional[str] = None,
    disk_name: Optional[str] = None,
    lun: Optional[int] = None,
    delete_disk: bool = False,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Remove/detach a disk from an Azure VM and optionally delete it
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        disk_name (str, optional): Name of the disk to remove. Either disk_name or lun must be provided.
        lun (int, optional): LUN of the disk to remove. Either disk_name or lun must be provided.
        delete_disk (bool): Whether to delete the disk after detaching. Defaults to False.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: Operation result
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_MANAGE", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_MANAGE permission required"
                    }
        
        # Validate required parameters
        if not disk_name and lun is None:
            return {
                "status": "error",
                "message": "Either disk_name or lun must be provided"
            }
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Get VM to access its disks
        try:
            vm = compute_client.virtual_machines.get(resource_group, vm_name)
        except ResourceNotFoundError:
            return {
                "status": "error",
                "message": f"VM {vm_name} not found in resource group {resource_group}"
            }
        
        # Get existing data disks
        data_disks = vm.storage_profile.data_disks or []
        
        # Find the disk to remove
        disk_to_remove = None
        remaining_disks = []
        
        for disk in data_disks:
            if (disk_name and disk.name == disk_name) or (lun is not None and disk.lun == lun):
                disk_to_remove = disk
            else:
                remaining_disks.append(disk)
                
        if not disk_to_remove:
            return {
                "status": "error",
                "message": f"Disk {'with name ' + disk_name if disk_name else 'with LUN ' + str(lun)} not found on VM {vm_name}"
            }
        
        # Store disk details for later use
        disk_details = {
            "name": disk_to_remove.name,
            "lun": disk_to_remove.lun,
            "id": disk_to_remove.managed_disk.id if disk_to_remove.managed_disk else None
        }
        
        # Update VM to remove the disk
        logger.info(f"Detaching disk {disk_details['name']} from VM {vm_name}")
        vm_update = compute_client.virtual_machines.begin_update(
            resource_group,
            vm_name,
            {
                'storage_profile': {
                    'data_disks': remaining_disks
                }
            }
        )
        vm_update.result()
        
        # Delete the disk if requested
        if delete_disk and disk_details["id"]:
            logger.info(f"Deleting disk {disk_details['name']}")
            # Extract disk name from ID if we only had LUN before
            if not disk_name:
                disk_name = disk_details["name"]
                
            try:
                disk_delete = compute_client.disks.begin_delete(
                    resource_group,
                    disk_name
                )
                disk_delete.result()
                disk_details["deleted"] = True
            except Exception as e:
                logger.error(f"Failed to delete disk {disk_name}: {str(e)}")
                disk_details["deleted"] = False
                disk_details["delete_error"] = str(e)
        
        return {
            "status": "success",
            "message": f"Successfully detached disk {disk_details['name']} from VM {vm_name}" + 
                      (f" and deleted it" if delete_disk and disk_details.get("deleted", False) else ""),
            "disk_details": disk_details
        }
    except ResourceNotFoundError as e:
        return {
            "status": "error",
            "message": f"Resource not found: {str(e)}"
        }
    except HttpResponseError as e:
        return {
            "status": "error",
            "message": f"Failed to remove disk: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Unexpected error removing disk: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }

async def list_disks(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    component: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    List all disks attached to an Azure VM
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: Operation result with list of disks
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_VIEW", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_VIEW permission required"
                    }
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Get VM to access its disks
        try:
            vm = compute_client.virtual_machines.get(resource_group, vm_name)
        except ResourceNotFoundError:
            return {
                "status": "error",
                "message": f"VM {vm_name} not found in resource group {resource_group}"
            }
        
        # Get OS disk details
        os_disk = vm.storage_profile.os_disk
        os_disk_details = {
            "name": os_disk.name,
            "type": "OS Disk",
            "disk_size_gb": None,  # Will be populated below
            "caching": os_disk.caching,
            "storage_account_type": os_disk.managed_disk.storage_account_type if os_disk.managed_disk else None,
            "is_os_disk": True
        }
        
        # Get data disks
        data_disks = vm.storage_profile.data_disks or []
        data_disk_details = []
        
        for disk in data_disks:
            data_disk_details.append({
                "name": disk.name,
                "lun": disk.lun,
                "disk_size_gb": None,  # Will be populated below
                "caching": disk.caching,
                "storage_account_type": disk.managed_disk.storage_account_type if disk.managed_disk else None,
                "is_os_disk": False
            })
        
        # Get all disks in the resource group to get sizes and other details
        all_disks = list(compute_client.disks.list_by_resource_group(resource_group))
        disk_map = {disk.name: disk for disk in all_disks}
        
        # Update OS disk with full details
        if os_disk.name in disk_map:
            full_disk = disk_map[os_disk.name]
            os_disk_details["disk_size_gb"] = full_disk.disk_size_gb
            os_disk_details["id"] = full_disk.id
            os_disk_details["location"] = full_disk.location
            os_disk_details["provisioning_state"] = full_disk.provisioning_state
            os_disk_details["disk_state"] = full_disk.disk_state
            os_disk_details["time_created"] = full_disk.time_created.isoformat() if full_disk.time_created else None
            
        # Update data disks with full details
        for disk_detail in data_disk_details:
            if disk_detail["name"] in disk_map:
                full_disk = disk_map[disk_detail["name"]]
                disk_detail["disk_size_gb"] = full_disk.disk_size_gb
                disk_detail["id"] = full_disk.id
                disk_detail["location"] = full_disk.location
                disk_detail["provisioning_state"] = full_disk.provisioning_state
                disk_detail["disk_state"] = full_disk.disk_state
                disk_detail["time_created"] = full_disk.time_created.isoformat() if full_disk.time_created else None
        
        # Combine all disks
        all_disk_details = [os_disk_details] + data_disk_details
        
        # Calculate total disk size
        total_size_gb = sum(disk.get("disk_size_gb", 0) or 0 for disk in all_disk_details)
        
        return {
            "status": "success",
            "message": f"Successfully retrieved disk information for VM {vm_name}",
            "vm_name": vm_name,
            "resource_group": resource_group,
            "os_disk": os_disk_details,
            "data_disks": data_disk_details,
            "total_disks": len(all_disk_details),
            "total_size_gb": total_size_gb
        }
    except ResourceNotFoundError as e:
        return {
            "status": "error",
            "message": f"Resource not found: {str(e)}"
        }
    except HttpResponseError as e:
        return {
            "status": "error",
            "message": f"Failed to list disks: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Unexpected error listing disks: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }

async def prepare_disk(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    component: Optional[str] = None,
    device_name: Optional[str] = None,
    lun: Optional[int] = None,
    mount_point: str = None,
    filesystem: str = "ext4",
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Format and mount a new disk on a Linux VM
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        device_name (str, optional): Device name (e.g., /dev/sdc). Either device_name or lun must be provided.
        lun (int, optional): LUN of the disk. Either device_name or lun must be provided.
        mount_point (str): Directory where the disk should be mounted. Required.
        filesystem (str): Filesystem type to create. Defaults to "ext4".
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: Operation result
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("VM_EXECUTE", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: VM_EXECUTE permission required"
                    }
        
        # Validate required parameters
        if not device_name and lun is None:
            return {
                "status": "error",
                "message": "Either device_name or lun must be provided"
            }
            
        if not mount_point:
            return {
                "status": "error",
                "message": "Mount point is required"
            }
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
        
        # If LUN is provided but not device_name, determine device_name
        if lun is not None and not device_name:
            # Use list_disks to get disk information
            disk_info = await list_disks(
                sid=sid,
                vm_name=vm_name,
                resource_group=resource_group,
                component=component,
                auth_context=auth_context
            )
            
            if disk_info["status"] != "success":
                return {
                    "status": "error",
                    "message": f"Failed to get disk information: {disk_info['message']}"
                }
                
            # Find the disk with the specified LUN
            target_disk = None
            for disk in disk_info["data_disks"]:
                if disk["lun"] == lun:
                    target_disk = disk
                    break
                    
            if not target_disk:
                return {
                    "status": "error",
                    "message": f"No disk found with LUN {lun}"
                }
            
            # Determine device name based on LUN
            # In Azure Linux VMs, the device naming follows a pattern:
            # - SCSI disks: /dev/sd[a-z] (LUN 0 = /dev/sdc, LUN 1 = /dev/sdd, etc.)
            # - NVMe disks: /dev/nvme[0-9]n1 
            # We'll check both possibilities
            
            # For SCSI disks (most common)
            device_name = f"/dev/sd{chr(ord('c') + lun)}"
        
        # Establish SSH connection to the VM
        ssh_result = await get_ssh_connection(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            component=component,
            auth_context=auth_context
        )
        
        if ssh_result["status"] != "success":
            return {
                "status": "error",
                "message": f"Failed to establish SSH connection: {ssh_result['message']}"
            }
            
        ssh_client = ssh_result["ssh_client"]
        
        # Check if the device exists
        _, stdout, stderr = await ssh_client.exec_command(f"ls -la {device_name}")
        if stderr.read().decode():
            # Try NVMe naming if SCSI naming fails
            if lun is not None:
                device_name = f"/dev/nvme{lun}n1"
                _, stdout, stderr = await ssh_client.exec_command(f"ls -la {device_name}")
                if stderr.read().decode():
                    return {
                        "status": "error",
                        "message": f"Device {device_name} not found. Please check if the disk is properly attached."
                    }
        
        # Check if the device is already mounted
        _, stdout, stderr = await ssh_client.exec_command("mount | grep -w " + device_name)
        mount_output = stdout.read().decode().strip()
        
        if mount_output:
            return {
                "status": "error",
                "message": f"Device {device_name} is already mounted at {mount_output.split()[2]}"
            }
        
        # Check if the mount point exists, create if not
        _, stdout, stderr = await ssh_client.exec_command(f"sudo mkdir -p {mount_point}")
        if stderr.read().decode():
            return {
                "status": "error",
                "message": f"Failed to create mount point {mount_point}"
            }
        
        # Create a partition table on the disk
        logger.info(f"Creating partition table on {device_name}")
        partition_cmd = f"echo -e 'o\\nn\\np\\n1\\n\\n\\nw' | sudo fdisk {device_name}"
        _, stdout, stderr = await ssh_client.exec_command(partition_cmd)
        
        # Wait for the partition to be recognized
        await ssh_client.exec_command("sleep 2")
        
        # Get the partition name
        partition_name = f"{device_name}1"
        
        # Format the partition
        logger.info(f"Formatting {partition_name} with {filesystem}")
        format_cmd = f"sudo mkfs.{filesystem} {partition_name}"
        _, stdout, stderr = await ssh_client.exec_command(format_cmd)
        format_error = stderr.read().decode()
        
        if "already contains a" in format_error:
            logger.warning(f"Partition {partition_name} already contains a filesystem")
        elif format_error and "done" not in format_error.lower():
            return {
                "status": "error",
                "message": f"Failed to format partition: {format_error}"
            }
        
        # Mount the partition
        logger.info(f"Mounting {partition_name} to {mount_point}")
        mount_cmd = f"sudo mount {partition_name} {mount_point}"
        _, stdout, stderr = await ssh_client.exec_command(mount_cmd)
        mount_error = stderr.read().decode()
        
        if mount_error:
            return {
                "status": "error",
                "message": f"Failed to mount partition: {mount_error}"
            }
        
        # Set appropriate permissions
        _, stdout, stderr = await ssh_client.exec_command(f"sudo chmod 755 {mount_point}")
        
        # Add to fstab for persistence across reboots
        logger.info(f"Adding entry to /etc/fstab for {partition_name}")
        uuid_cmd = f"sudo blkid -s UUID -o value {partition_name}"
        _, stdout, stderr = await ssh_client.exec_command(uuid_cmd)
        uuid = stdout.read().decode().strip()
        
        if not uuid:
            return {
                "status": "warning",
                "message": f"Disk formatted and mounted successfully, but failed to get UUID for fstab entry"
            }
        
        fstab_entry = f"UUID={uuid} {mount_point} {filesystem} defaults 0 2"
        fstab_cmd = f"echo '{fstab_entry}' | sudo tee -a /etc/fstab"
        _, stdout, stderr = await ssh_client.exec_command(fstab_cmd)
        
        # Check disk space
        _, stdout, stderr = await ssh_client.exec_command(f"df -h {mount_point}")
        disk_space = stdout.read().decode().strip()
        
        # Close SSH connection
        ssh_client.close()
        
        return {
            "status": "success",
            "message": f"Successfully prepared disk {device_name} and mounted at {mount_point}",
            "device_name": device_name,
            "partition_name": partition_name,
            "mount_point": mount_point,
            "filesystem": filesystem,
            "uuid": uuid,
            "disk_space": disk_space
        }
    except Exception as e:
        logger.error(f"Unexpected error preparing disk: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }

async def extend_filesystem(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    component: Optional[str] = None,
    device_name: Optional[str] = None,
    lun: Optional[int] = None,
    mount_point: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Extend a filesystem after resizing the underlying disk
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        device_name (str, optional): Device name (e.g., /dev/sdc). Either device_name, lun, or mount_point must be provided.
        lun (int, optional): LUN of the disk. Either device_name, lun, or mount_point must be provided.
        mount_point (str, optional): Mount point of the filesystem to extend. Either device_name, lun, or mount_point must be provided.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: Operation result
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("VM_EXECUTE", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: VM_EXECUTE permission required"
                    }
        
        # Validate required parameters
        if not device_name and lun is None and not mount_point:
            return {
                "status": "error",
                "message": "Either device_name, lun, or mount_point must be provided"
            }
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
        
        # Establish SSH connection to the VM
        ssh_result = await get_ssh_connection(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            component=component,
            auth_context=auth_context
        )
        
        if ssh_result["status"] != "success":
            return {
                "status": "error",
                "message": f"Failed to establish SSH connection: {ssh_result['message']}"
            }
            
        ssh_client = ssh_result["ssh_client"]
        
        # If LUN is provided but not device_name, determine device_name
        if lun is not None and not device_name:
            # For SCSI disks (most common)
            device_name = f"/dev/sd{chr(ord('c') + lun)}"
            
            # Check if the device exists
            _, stdout, stderr = await ssh_client.exec_command(f"ls -la {device_name}")
            if stderr.read().decode():
                # Try NVMe naming if SCSI naming fails
                device_name = f"/dev/nvme{lun}n1"
                _, stdout, stderr = await ssh_client.exec_command(f"ls -la {device_name}")
                if stderr.read().decode():
                    return {
                        "status": "error",
                        "message": f"Could not determine device name for LUN {lun}"
                    }
        
        # If mount_point is provided but not device_name, determine device_name
        if mount_point and not device_name:
            _, stdout, stderr = await ssh_client.exec_command(f"findmnt -n -o SOURCE {mount_point}")
            device_output = stdout.read().decode().strip()
            
            if not device_output:
                return {
                    "status": "error",
                    "message": f"No device found for mount point {mount_point}"
                }
                
            # The device might be a partition (e.g., /dev/sdc1)
            # Extract the base device (e.g., /dev/sdc)
            device_name = ''.join(device_output.rstrip('0123456789'))
        
        # Get the partition name
        # First check if we have a partition or a raw device
        _, stdout, stderr = await ssh_client.exec_command(f"lsblk -no NAME {device_name} | grep -v $(basename {device_name})")
        partition_output = stdout.read().decode().strip()
        
        if partition_output:
            # We have a partition
            partition_name = f"{device_name}1"
        else:
            # We're using the raw device
            partition_name = device_name
        
        # If we don't have a mount point yet, find it
        if not mount_point:
            _, stdout, stderr = await ssh_client.exec_command(f"findmnt -n -o TARGET {partition_name}")
            mount_output = stdout.read().decode().strip()
            
            if not mount_output:
                return {
                    "status": "error",
                    "message": f"Device {partition_name} is not mounted"
                }
                
            mount_point = mount_output
        
        # Get filesystem type
        _, stdout, stderr = await ssh_client.exec_command(f"findmnt -n -o FSTYPE {mount_point}")
        filesystem = stdout.read().decode().strip()
        
        if not filesystem:
            return {
                "status": "error",
                "message": f"Could not determine filesystem type for {mount_point}"
            }
        
        # Get disk info before resize
        _, stdout, stderr = await ssh_client.exec_command(f"df -h {mount_point}")
        before_resize = stdout.read().decode().strip()
        
        # Rescan the SCSI bus to recognize the new disk size
        logger.info("Rescanning SCSI bus to recognize new disk size")
        _, stdout, stderr = await ssh_client.exec_command("sudo bash -c 'echo 1 > /sys/class/block/$(basename $(readlink -f " + device_name + "))/device/rescan'")
        
        # If we have a partition, we need to update the partition table
        if partition_output:
            logger.info(f"Updating partition table for {device_name}")
            
            # Check if growpart is available
            _, stdout, stderr = await ssh_client.exec_command("which growpart")
            if stderr.read().decode():
                # Install growpart if not available
                logger.info("Installing growpart")
                _, stdout, stderr = await ssh_client.exec_command("sudo apt-get update && sudo apt-get install -y cloud-guest-utils")
                if stderr.read().decode():
                    return {
                        "status": "error",
                        "message": "Failed to install required tools (growpart)"
                    }
            
            # Use growpart to extend the partition
            _, stdout, stderr = await ssh_client.exec_command(f"sudo growpart {device_name} 1")
            growpart_error = stderr.read().decode()
            
            if growpart_error and "NOCHANGE" not in growpart_error:
                return {
                    "status": "error",
                    "message": f"Failed to extend partition: {growpart_error}"
                }
        
        # Extend the filesystem
        logger.info(f"Extending {filesystem} filesystem on {partition_name}")
        
        resize_command = ""
        if filesystem == "ext4" or filesystem == "ext3" or filesystem == "ext2":
            resize_command = f"sudo resize2fs {partition_name}"
        elif filesystem == "xfs":
            resize_command = f"sudo xfs_growfs {mount_point}"
        else:
            return {
                "status": "error",
                "message": f"Unsupported filesystem type: {filesystem}"
            }
        
        _, stdout, stderr = await ssh_client.exec_command(resize_command)
        resize_error = stderr.read().decode()
        
        if resize_error and "nothing to do" not in resize_error.lower():
            return {
                "status": "error",
                "message": f"Failed to extend filesystem: {resize_error}"
            }
        
        # Get disk info after resize
        _, stdout, stderr = await ssh_client.exec_command(f"df -h {mount_point}")
        after_resize = stdout.read().decode().strip()
        
        # Close SSH connection
        ssh_client.close()
        
        return {
            "status": "success",
            "message": f"Successfully extended filesystem on {partition_name} mounted at {mount_point}",
            "device_name": device_name,
            "partition_name": partition_name,
            "mount_point": mount_point,
            "filesystem": filesystem,
            "before_resize": before_resize,
            "after_resize": after_resize
        }
    except Exception as e:
        logger.error(f"Unexpected error extending filesystem: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }

async def cleanup_disk(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    component: Optional[str] = None,
    device_name: Optional[str] = None,
    lun: Optional[int] = None,
    mount_point: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Unmount and clean up a disk before removal
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        device_name (str, optional): Device name (e.g., /dev/sdc). Either device_name, lun, or mount_point must be provided.
        lun (int, optional): LUN of the disk. Either device_name, lun, or mount_point must be provided.
        mount_point (str, optional): Mount point of the filesystem to clean up. Either device_name, lun, or mount_point must be provided.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: Operation result
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("VM_EXECUTE", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: VM_EXECUTE permission required"
                    }
        
        # Validate required parameters
        if not device_name and lun is None and not mount_point:
            return {
                "status": "error",
                "message": "Either device_name, lun, or mount_point must be provided"
            }
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
        
        # Establish SSH connection to the VM
        ssh_result = await get_ssh_connection(
            sid=sid,
            vm_name=vm_name,
            resource_group=resource_group,
            component=component,
            auth_context=auth_context
        )
        
        if ssh_result["status"] != "success":
            return {
                "status": "error",
                "message": f"Failed to establish SSH connection: {ssh_result['message']}"
            }
            
        ssh_client = ssh_result["ssh_client"]
        
        # If LUN is provided but not device_name, determine device_name
        if lun is not None and not device_name:
            # For SCSI disks (most common)
            device_name = f"/dev/sd{chr(ord('c') + lun)}"
            
            # Check if the device exists
            _, stdout, stderr = await ssh_client.exec_command(f"ls -la {device_name}")
            if stderr.read().decode():
                # Try NVMe naming if SCSI naming fails
                device_name = f"/dev/nvme{lun}n1"
                _, stdout, stderr = await ssh_client.exec_command(f"ls -la {device_name}")
                if stderr.read().decode():
                    return {
                        "status": "error",
                        "message": f"Could not determine device name for LUN {lun}"
                    }
        
        # Get the partition name
        # First check if we have a partition or a raw device
        if device_name:
            _, stdout, stderr = await ssh_client.exec_command(f"lsblk -no NAME {device_name} | grep -v $(basename {device_name})")
            partition_output = stdout.read().decode().strip()
            
            if partition_output:
                # We have a partition
                partition_name = f"{device_name}1"
            else:
                # We're using the raw device
                partition_name = device_name
        else:
            partition_name = None
        
        # If mount_point is provided but not device_name, determine device_name and partition_name
        if mount_point and not device_name:
            _, stdout, stderr = await ssh_client.exec_command(f"findmnt -n -o SOURCE {mount_point}")
            partition_name = stdout.read().decode().strip()
            
            if not partition_name:
                return {
                    "status": "error",
                    "message": f"No device found for mount point {mount_point}"
                }
                
            # Extract the base device from the partition (e.g., /dev/sdc from /dev/sdc1)
            device_name = ''.join(partition_name.rstrip('0123456789'))
        
        # If we don't have a mount point yet, find it
        if not mount_point and partition_name:
            _, stdout, stderr = await ssh_client.exec_command(f"findmnt -n -o TARGET {partition_name}")
            mount_output = stdout.read().decode().strip()
            
            if mount_output:
                mount_point = mount_output
        
        # If we still don't have a mount point, the disk might not be mounted
        if not mount_point:
            return {
                "status": "warning",
                "message": f"Device {partition_name or device_name} is not mounted, nothing to clean up",
                "device_name": device_name,
                "partition_name": partition_name
            }
        
        # Get UUID of the partition for fstab cleanup
        _, stdout, stderr = await ssh_client.exec_command(f"sudo blkid -s UUID -o value {partition_name}")
        uuid = stdout.read().decode().strip()
        
        # Unmount the filesystem
        logger.info(f"Unmounting {mount_point}")
        _, stdout, stderr = await ssh_client.exec_command(f"sudo umount {mount_point}")
        umount_error = stderr.read().decode()
        
        if umount_error and "not mounted" not in umount_error and "not found" not in umount_error:
            # Check if the filesystem is busy
            if "target is busy" in umount_error:
                # Try to find processes using the mount point
                _, stdout, stderr = await ssh_client.exec_command(f"sudo lsof {mount_point}")
                processes = stdout.read().decode()
                
                return {
                    "status": "error",
                    "message": f"Failed to unmount {mount_point}: Device is busy. Processes using it: {processes}",
                    "recommendation": "Stop the processes using the mount point and try again"
                }
            else:
                return {
                    "status": "error",
                    "message": f"Failed to unmount {mount_point}: {umount_error}"
                }
        
        # Remove from fstab
        if uuid:
            logger.info(f"Removing UUID={uuid} from /etc/fstab")
            # Create a backup of fstab
            _, stdout, stderr = await ssh_client.exec_command("sudo cp /etc/fstab /etc/fstab.bak")
            
            # Remove the entry from fstab
            _, stdout, stderr = await ssh_client.exec_command(f"sudo sed -i '/UUID={uuid}/d' /etc/fstab")
            if stderr.read().decode():
                return {
                    "status": "warning",
                    "message": f"Disk unmounted successfully, but failed to update /etc/fstab",
                    "device_name": device_name,
                    "partition_name": partition_name,
                    "mount_point": mount_point,
                    "uuid": uuid
                }
        
        # Close SSH connection
        ssh_client.close()
        
        return {
            "status": "success",
            "message": f"Successfully unmounted and cleaned up {mount_point}",
            "device_name": device_name,
            "partition_name": partition_name,
            "mount_point": mount_point,
            "uuid": uuid
        }
    except Exception as e:
        logger.error(f"Unexpected error cleaning up disk: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error: {str(e)}"
        }
