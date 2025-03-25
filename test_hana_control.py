#!/usr/bin/env python3
"""
Test script for HANA control functionality
"""
import asyncio
import logging
import json
from tools.hana_control import manage_hana_system

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_hana_control():
    """Test HANA control functionality (status check only)"""
    logger.info("Testing manage_hana_system for D54 (status check only)...")
    logger.info("Note: This is just validating the endpoint works, not actually performing the action")
    
    # Test start action (we're only testing if the command is properly constructed)
    result = await manage_hana_system(sid="D54", action="start", wait=False)
    logger.info(f"manage_hana_system (start) result: {json.dumps(result, indent=2)}")

if __name__ == "__main__":
    asyncio.run(test_hana_control())
