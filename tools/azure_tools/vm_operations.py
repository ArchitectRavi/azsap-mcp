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
