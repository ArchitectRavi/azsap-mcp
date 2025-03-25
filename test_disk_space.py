#!/usr/bin/env python3
"""
Test script for disk space check functionality
"""
import asyncio
import logging
import json
from tools.disk_check import check_disk_space, check_hana_volumes

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_disk_space():
    """Test disk space check functionality"""
    logger.info("Testing check_disk_space for D54...")
    result = await check_disk_space(sid="D54")
    logger.info(f"check_disk_space result: {json.dumps(result, indent=2)}")
    
    logger.info("\nTesting check_hana_volumes for D54...")
    result = await check_hana_volumes(sid="D54")
    logger.info(f"check_hana_volumes result: {json.dumps(result, indent=2)}")

if __name__ == "__main__":
    asyncio.run(test_disk_space())
