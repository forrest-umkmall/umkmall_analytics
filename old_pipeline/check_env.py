#!/usr/bin/env python
"""
Quick script to check if your .env file is set up correctly.
Run: uv run python check_env.py
"""

import os
from dotenv import load_dotenv

def check_env():
    print("="*60)
    print("Environment Variables Check")
    print("="*60)

    # Load .env file
    print("\n1. Loading .env file...")
    env_file = ".env"
    if os.path.exists(env_file):
        print(f"   ✓ Found {env_file}")
        load_dotenv()
    else:
        print(f"   ✗ {env_file} not found")
        print(f"   → Run: cp .env.example .env")
        return

    # Check required variables
    print("\n2. Checking required variables...")

    client_email = os.getenv('GOOGLE_CLIENT_EMAIL')
    private_key = os.getenv('GOOGLE_PRIVATE_KEY')

    if client_email:
        print(f"   ✓ GOOGLE_CLIENT_EMAIL: {client_email[:30]}...")
    else:
        print("   ✗ GOOGLE_CLIENT_EMAIL not set")

    if private_key:
        # Check if it looks like a valid private key
        if "BEGIN PRIVATE KEY" in private_key:
            print(f"   ✓ GOOGLE_PRIVATE_KEY: Found (length: {len(private_key)} chars)")

            # Check if newlines were properly converted
            if "\n" in private_key and "\\n" not in private_key:
                print("   ✓ Newlines properly formatted")
            else:
                print("   ⚠ Warning: Check newline formatting")
        else:
            print("   ✗ GOOGLE_PRIVATE_KEY doesn't look like a valid key")
    else:
        print("   ✗ GOOGLE_PRIVATE_KEY not set")

    # Test connection
    print("\n3. Testing Google Sheets connection...")
    if client_email and private_key:
        try:
            from src.GSheets import GSheetsClient
            client = GSheetsClient()
            print("   ✓ Successfully created GSheetsClient")
            print("\n" + "="*60)
            print("✓ Configuration looks good!")
            print("="*60)
            print("\nTo test with a real spreadsheet, use:")
            print("  from src.GSheets import GSheetsClient")
            print("  client = GSheetsClient()")
            print("  client.get_all_sheets_metadata('your-spreadsheet-id')")
        except Exception as e:
            print(f"   ✗ Error creating client: {e}")
    else:
        print("   ⊗ Skipped (missing required variables)")
        print("\n" + "="*60)
        print("✗ Configuration incomplete")
        print("="*60)
        print("\nPlease update your .env file with:")
        print("  GOOGLE_CLIENT_EMAIL=your-service-account@project.iam.gserviceaccount.com")
        print('  GOOGLE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\\nYour key\\n-----END PRIVATE KEY-----"')

if __name__ == "__main__":
    check_env()
