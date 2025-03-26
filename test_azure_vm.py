#!/usr/bin/env python3
"""
Test script for Azure VM operations

This script tests the Azure VM operations module by retrieving VM status
and listing available VMs.
"""
import asyncio
import json
import logging
from tools.azure_tools.vm_operations import get_vm_status, list_vms
from tools.azure_tools.auth import test_azure_auth

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    """
    Main test function
    """
    # Test Azure authentication
    logger.info("Testing Azure authentication...")
    auth_result = test_azure_auth()
    logger.info(f"Authentication test result: {json.dumps(auth_result, indent=2)}")
    
    if auth_result["status"] == "error":
        logger.error("Authentication failed, cannot continue with tests")
        return
    
    # List VMs
    logger.info("Listing VMs...")
    vms_result = await list_vms()
    logger.info(f"VMs: {json.dumps(vms_result, indent=2)}")
    
    # If VMs were found, get status of the first one
    if vms_result["status"] == "success" and vms_result["vms"]:
        first_vm = vms_result["vms"][0]
        vm_name = first_vm["name"]
        resource_group = first_vm["resource_group"]
        
        logger.info(f"Getting status of VM {vm_name}...")
        status_result = await get_vm_status(vm_name=vm_name, resource_group=resource_group)
        logger.info(f"VM status: {json.dumps(status_result, indent=2)}")

if __name__ == "__main__":
    asyncio.run(main())
