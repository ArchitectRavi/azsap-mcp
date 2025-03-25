#!/usr/bin/env python3
"""
Test script for HANA version check functionality
"""
import asyncio
import logging
import json
from tools.hana_status import get_hana_version

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_hana_version():
    """Test HANA version check functionality"""
    logger.info("Testing get_hana_version for D54...")
    result = await get_hana_version(sid="D54")
    logger.info(f"get_hana_version result: {json.dumps(result, indent=2)}")

if __name__ == "__main__":
    asyncio.run(test_hana_version())
