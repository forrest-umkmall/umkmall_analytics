"""
Main pipeline orchestration using the layered architecture.

This demonstrates the new approach:
1. Staging: Load and normalize each source
2. Intermediate: Progressive union and merge layers
3. Output: Write to one or more Google Sheets
"""

import os
from dotenv import load_dotenv

from src.config.sources import get_all_data_sources
from src.config.layers import INTERMEDIATE_LAYERS, OUTPUT_LAYERS
from src.transforms.staging import stage_all_sources
from src.transforms.intermediate import process_intermediate_layer
from src.transforms.marts import write_output
from src.transforms.metadata import extract_field_metadata


def main():
    """Run the complete data pipeline."""
    load_dotenv()

    print("\n" + "="*60)
    print("DATA PIPELINE - LAYERED ARCHITECTURE")
    print("="*60)

    # =========================================================================
    # STAGE 1: STAGING - Load and normalize each source
    # =========================================================================
    print("\n### STAGE 1: STAGING ###\n")

    source_configs = get_all_data_sources()
    staged_data = stage_all_sources(source_configs)

    print(f"\n✓ Staged {len(staged_data)} sources")
    for name, df in staged_data.items():
        print(f"  - {name}: {len(df)} rows")

    # =========================================================================
    # STAGE 1.5: METADATA EXTRACTION - Extract field metadata from staged sources
    # =========================================================================
    print("\n### METADATA EXTRACTION ###\n")

    field_metadata = extract_field_metadata(staged_data)

    # =========================================================================
    # STAGE 2: INTERMEDIATE - Progressive union and merge layers
    # =========================================================================
    print("\n### STAGE 2: INTERMEDIATE LAYERS ###\n")

    # This dictionary holds both staged and intermediate data
    # Intermediate layers can reference staged sources or previous intermediate layers
    all_data = staged_data.copy()

    # Add field metadata as a special layer
    all_data['_field_metadata'] = field_metadata

    for layer in INTERMEDIATE_LAYERS:
        result_df = process_intermediate_layer(layer, all_data)
        all_data[layer.name] = result_df

    print(f"\n✓ Processed {len(INTERMEDIATE_LAYERS)} intermediate layers")

    # =========================================================================
    # STAGE 3: OUTPUT - Write to Google Sheets
    # =========================================================================
    print("\n### STAGE 3: OUTPUTS ###\n")

    for output in OUTPUT_LAYERS:
        write_output(output, all_data)

    print(f"\n✓ Written {len(OUTPUT_LAYERS)} outputs")

    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
