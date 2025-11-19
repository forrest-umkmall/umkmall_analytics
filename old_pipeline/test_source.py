#!/usr/bin/env python3
"""
Test script for individual data sources.
Allows you to select and test specific sources to debug extraction issues.
"""

import os
from dotenv import load_dotenv
import argparse
from pathlib import Path
import pandas as pd
from tabulate import tabulate

# Add src to path

from src.config.sources import get_all_data_sources, get_data_source
from src.transforms.staging import stage_source, stage_all_sources
from src.config.layers import INTERMEDIATE_LAYERS, OUTPUT_LAYERS
from src.transforms.intermediate import process_intermediate_layer

load_dotenv()


def list_sources():
    """List all available data sources."""
    sources = get_all_data_sources()
    print("\n📋 Available Data Sources:\n")

    data = []
    for idx, source in enumerate(sources, 1):
        data.append([
            idx,
            source.name,
            source.source_type,
            source.description or "N/A"
        ])

    print(tabulate(data, headers=["#", "Name", "Type", "Description"], tablefmt="grid"))
    print()


def list_layers():
    """List all available intermediate and output layers."""
    print("\n📋 Available Layers:\n")

    # Intermediate layers
    print("INTERMEDIATE LAYERS:")
    data = []
    for idx, layer in enumerate(INTERMEDIATE_LAYERS, 1):
        layer_type = type(layer).__name__
        sources = ', '.join(layer.sources)
        data.append([
            idx,
            layer.name,
            layer_type,
            sources
        ])

    if data:
        print(tabulate(data, headers=["#", "Name", "Type", "Sources"], tablefmt="grid"))
    else:
        print("  (none)")

    # Output layers
    print("\nOUTPUT LAYERS:")
    data = []
    for idx, layer in enumerate(OUTPUT_LAYERS, 1):
        data.append([
            idx,
            layer.name,
            layer.source_layer,
            layer.sheet_name
        ])

    if data:
        print(tabulate(data, headers=["#", "Name", "Source Layer", "Sheet Name"], tablefmt="grid"))
    else:
        print("  (none)")

    print()


def test_source(source_name: str, limit: int = None, show_columns: bool = True, columns: list = None):
    """Test extraction from a specific data source."""
    source_config = get_data_source(source_name)

    if not source_config:
        print(f"❌ Error: Source '{source_name}' not found.")
        print("\nUse --list to see available sources.")
        return

    print(f"\n🔍 Testing source: {source_config.name}")
    print(f"   Type: {source_config.source_type}")
    print(f"   Description: {source_config.description or 'N/A'}")

    if source_config.source_type == 'postgres':
        print(f"\n   Query:\n{source_config.query}")

    print("\n⏳ Extracting data...\n")

    try:
        df = stage_source(source_config)

        if df is None or df.empty:
            print("⚠️  No data extracted (empty DataFrame)")
            return

        print(f"✅ Successfully extracted {len(df)} rows")

        # Show column information
        if show_columns:
            print(f"\n📊 Columns ({len(df.columns)}):")
            for col in df.columns:
                dtype = str(df[col].dtype)
                non_null = df[col].notna().sum()
                null_count = df[col].isna().sum()
                print(f"   • {col:30s} (dtype: {dtype:12s}, non-null: {non_null:6d}, null: {null_count:6d})")

        # Show sample data
        display_limit = limit or 10

        # If specific columns requested, show only those
        if columns:
            available_cols = [c for c in columns if c in df.columns]
            missing_cols = [c for c in columns if c not in df.columns]

            if missing_cols:
                print(f"\n⚠️  Warning: Columns not found: {', '.join(missing_cols)}")

            if available_cols:
                print(f"\n📄 Sample Data - Selected Columns (first {display_limit} rows):\n")
                sample_df = df[available_cols].head(display_limit)
                print(tabulate(sample_df, headers='keys', tablefmt='grid', showindex=True))
        else:
            # Show all columns - use simple format for wide tables
            if len(df.columns) > 10:
                print(f"\n📄 Sample Data (first {display_limit} rows) - Showing column-by-column due to width:\n")
                sample_df = df.head(display_limit)
                for col in df.columns:
                    print(f"\n{col}:")
                    print(sample_df[col].to_string(index=True))
            else:
                print(f"\n📄 Sample Data (first {display_limit} rows):\n")
                sample_df = df.head(display_limit)
                print(tabulate(sample_df, headers='keys', tablefmt='grid', showindex=True))

        # Check for specific columns mentioned in the task
        if 'paid_at' in df.columns:
            print(f"\n✅ 'paid_at' column found!")
            print(f"   Non-null values: {df['paid_at'].notna().sum()}")
            print(f"   Sample values: {df['paid_at'].head(3).tolist()}")
        else:
            print(f"\n⚠️  'paid_at' column NOT found in extracted data")
            print(f"   Available columns: {', '.join(df.columns)}")

    except Exception as e:
        print(f"❌ Error extracting data: {str(e)}")
        import traceback
        traceback.print_exc()


def test_multiple_sources(source_names: list, limit: int = None):
    """Test multiple data sources."""
    for idx, source_name in enumerate(source_names, 1):
        print(f"\n{'='*80}")
        print(f"Testing source {idx}/{len(source_names)}")
        print('='*80)
        test_source(source_name, limit=limit, show_columns=True)

    print(f"\n{'='*80}")
    print(f"✅ Completed testing {len(source_names)} sources")
    print('='*80)


def test_layer(layer_name: str, limit: int = None, show_columns: bool = True, columns: list = None):
    """Test processing of a specific intermediate layer."""
    # Find the layer
    layer = None
    for l in INTERMEDIATE_LAYERS:
        if l.name == layer_name:
            layer = l
            break

    if not layer:
        print(f"❌ Error: Layer '{layer_name}' not found in INTERMEDIATE_LAYERS.")
        print("\nUse --list-layers to see available layers.")
        return

    print(f"\n🔍 Testing layer: {layer.name}")
    print(f"   Type: {type(layer).__name__}")
    print(f"   Sources: {', '.join(layer.sources)}")

    print("\n⏳ Processing layer...\n")

    try:
        # First, stage all the required sources
        print("📦 Staging required sources...")
        source_configs = get_all_data_sources()
        staged_data = stage_all_sources(source_configs)

        # Process any intermediate layers that this layer depends on
        all_data = staged_data.copy()
        for intermediate_layer in INTERMEDIATE_LAYERS:
            # Process layers up to (but not including) our target layer
            if intermediate_layer.name == layer_name:
                break

            # Check if this layer is a dependency
            if any(src in layer.sources for src in [intermediate_layer.name]):
                print(f"  Processing dependency: {intermediate_layer.name}")
                all_data[intermediate_layer.name] = process_intermediate_layer(intermediate_layer, all_data)

        # Now process the target layer
        print(f"\n📊 Processing target layer: {layer_name}\n")
        df = process_intermediate_layer(layer, all_data)

        if df is None or df.empty:
            print("⚠️  No data produced (empty DataFrame)")
            return

        print(f"\n✅ Successfully processed layer: {len(df)} rows")

        # Show column information
        if show_columns:
            print(f"\n📊 Columns ({len(df.columns)}):")
            for col in df.columns:
                dtype = str(df[col].dtype)
                non_null = df[col].notna().sum()
                null_count = df[col].isna().sum()
                print(f"   • {col:30s} (dtype: {dtype:12s}, non-null: {non_null:6d}, null: {null_count:6d})")

        # Show sample data
        display_limit = limit or 10

        # If specific columns requested, show only those
        if columns:
            available_cols = [c for c in columns if c in df.columns]
            missing_cols = [c for c in columns if c not in df.columns]

            if missing_cols:
                print(f"\n⚠️  Warning: Columns not found: {', '.join(missing_cols)}")

            if available_cols:
                print(f"\n📄 Sample Data - Selected Columns (first {display_limit} rows):\n")
                sample_df = df[available_cols].head(display_limit)
                print(tabulate(sample_df, headers='keys', tablefmt='grid', showindex=True))
        else:
            # Show all columns - use simple format for wide tables
            if len(df.columns) > 10:
                print(f"\n📄 Sample Data (first {display_limit} rows) - Column-by-column view:\n")
                sample_df = df.head(display_limit)
                for col in df.columns:
                    print(f"\n{col}:")
                    print(sample_df[col].to_string(index=True))
            else:
                print(f"\n📄 Sample Data (first {display_limit} rows):\n")
                sample_df = df.head(display_limit)
                print(tabulate(sample_df, headers='keys', tablefmt='grid', showindex=True))

        # Check for specific columns
        if 'paid_at' in df.columns:
            print(f"\n✅ 'paid_at' column found!")
            print(f"   Non-null values: {df['paid_at'].notna().sum()}")
            print(f"   Sample values: {df['paid_at'].head(3).tolist()}")
        else:
            print(f"\n⚠️  'paid_at' column NOT found in layer output")

        # Check for any paid_at variations
        paid_at_cols = [col for col in df.columns if 'paid_at' in col.lower()]
        if paid_at_cols:
            print(f"\n📌 Found paid_at variations: {', '.join(paid_at_cols)}")
            for col in paid_at_cols:
                print(f"   • {col}: {df[col].notna().sum()} non-null values")
                print(f"     Sample: {df[col].head(3).tolist()}")

    except Exception as e:
        print(f"❌ Error processing layer: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(
        description='Test individual data sources and layers from the pipeline configuration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # List all available sources and layers
  python test_source.py --list
  python test_source.py --list-layers

  # Test a specific source
  python test_source.py --source purchase_form_data

  # Test a specific layer (auto-formats wide tables as column-by-column)
  python test_source.py --layer enriched_contacts

  # Show only specific columns (great for wide tables!)
  python test_source.py --layer enriched_contacts --columns "email,phone_number,paid_at"
  python test_source.py --source purchase_form_data -c "customer_email,customer_phone,paid_at"

  # Test with limited rows
  python test_source.py --source purchase_form_data --limit 5
  python test_source.py --layer enriched_contacts --limit 5

  # Combine options
  python test_source.py --layer enriched_contacts --columns "email,paid_at_purchase_form_data" --limit 3

  # Test multiple sources
  python test_source.py --source leads_ads_community --source website_form_responses

  # Test all sources
  python test_source.py --all
        '''
    )

    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List all available data sources'
    )

    parser.add_argument(
        '--list-layers',
        action='store_true',
        help='List all available intermediate and output layers'
    )

    parser.add_argument(
        '--source', '-s',
        action='append',
        dest='sources',
        help='Data source name to test (can be specified multiple times)'
    )

    parser.add_argument(
        '--layer',
        type=str,
        help='Intermediate layer name to test'
    )

    parser.add_argument(
        '--all', '-a',
        action='store_true',
        help='Test all data sources'
    )

    parser.add_argument(
        '--limit', '-n',
        type=int,
        default=10,
        help='Limit number of rows to display (default: 10)'
    )

    parser.add_argument(
        '--columns', '-c',
        type=str,
        help='Comma-separated list of columns to display (e.g., "email,phone_number,paid_at")'
    )

    args = parser.parse_args()

    # Parse columns if provided
    columns = None
    if args.columns:
        columns = [c.strip() for c in args.columns.split(',')]

    # List sources
    if args.list:
        list_sources()
        return

    # List layers
    if args.list_layers:
        list_layers()
        return

    # Test layer
    if args.layer:
        test_layer(args.layer, limit=args.limit, columns=columns)
        return

    # Test all sources
    if args.all:
        all_sources = [s.name for s in get_all_data_sources()]
        test_multiple_sources(all_sources, limit=args.limit)
        return

    # Test specific sources
    if args.sources:
        if len(args.sources) == 1:
            test_source(args.sources[0], limit=args.limit, columns=columns)
        else:
            test_multiple_sources(args.sources, limit=args.limit)
        return

    # No arguments - show help
    parser.print_help()
    print("\n")
    list_sources()
    print()
    list_layers()


if __name__ == '__main__':
    main()
