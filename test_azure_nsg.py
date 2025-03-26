#!/usr/bin/env python3
"""
Test script for Azure NSG operations

This script tests the Azure NSG operations module by listing NSGs
and retrieving NSG rules.
"""
import asyncio
import json
import logging
from tools.azure_tools.nsg_operations import list_nsgs, get_nsg_rules
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
    
    # List NSGs
    logger.info("Listing NSGs...")
    nsgs_result = await list_nsgs()
    logger.info(f"NSGs: {json.dumps(nsgs_result, indent=2)}")
    
    # If NSGs were found, get rules of the first one
    if nsgs_result["status"] == "success" and nsgs_result["nsgs"]:
        first_nsg = nsgs_result["nsgs"][0]
        nsg_name = first_nsg["name"]
        resource_group = first_nsg["resource_group"]
        
        logger.info(f"Getting rules of NSG {nsg_name}...")
        rules_result = await get_nsg_rules(nsg_name=nsg_name, resource_group=resource_group)
        logger.info(f"NSG rules: {json.dumps(rules_result, indent=2)}")

if __name__ == "__main__":
    asyncio.run(main())
