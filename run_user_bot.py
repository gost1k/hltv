#!/usr/bin/env python
"""
Run bot for users
"""
import sys
import os

# Configure encoding for correct handling of Russian language in Windows console
if sys.platform == "win32":
    # Set UTF-8 for console output
    # Fix the UnicodeEncodeError issue on Windows
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer)
    
    # Create directory for logs
    os.makedirs("logs", exist_ok=True)

from src.bots.start_user_bot import main

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nBot was stopped by user.")
    except Exception as e:
        print(f"\nAn error occurred when starting the bot: {e}")
        sys.exit(1) 