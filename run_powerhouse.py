#!/usr/bin/env python3
"""
Runner script for the Powerhouse OrderDetail Processor
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from powerhouse_orderdetail_processor import main

if __name__ == "__main__":
    print("🚀 Starting Powerhouse OrderDetail Processor...")
    print("Optimized for 128GB RAM + RTX 5090")
    print("="*60)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️  Processing interrupted by user")
    except Exception as e:
        print(f"\n❌ Processing failed: {e}")
        sys.exit(1)
