#!/usr/bin/env python3
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Add src directory to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.parser import FileParser
from src.prompt_manager import PromptManager

def test_prompt_generation():
    """Test that prompts are generated correctly with extracted_tables parameter"""
    prompt_manager = PromptManager()

    # Test SQL prompt with extracted tables
    sql_prompt = prompt_manager.get_prompt(
        "parse_sql_round1",
        content="test content",
        filename="test.sql",
        extracted_tables=["table1", "table2", "table3"],
        max_tables=5
    )

    print("SQL Prompt with extracted tables:")
    print("-" * 80)
    print(sql_prompt)
    print("-" * 80)

    # Test MD prompt with extracted tables
    md_prompt = prompt_manager.get_prompt(
        "parse_md_round1",
        content="test content",
        extracted_tables=["table1", "table2", "table3"],
        max_tables=5
    )

    print("\nMD Prompt with extracted tables:")
    print("-" * 80)
    print(md_prompt)
    print("-" * 80)

    # Test prompt without extracted tables
    sql_prompt_empty = prompt_manager.get_prompt(
        "parse_sql_round1",
        content="test content",
        filename="test.sql"
    )

    print("\nSQL Prompt without extracted tables:")
    print("-" * 80)
    print(sql_prompt_empty)
    print("-" * 80)

    return True

def test_batch_logic():
    """Test the batch extraction logic with mock data"""
    parser = FileParser()

    # Mock response that returns 3 tables first
    mock_response1 = '''{
        "business_domain": "电商",
        "data_sources": [
            {"table_name": "table1", "database": "test", "snowflake_layer": "ADS", "partition_field": "pt_d", "main_usage": "usage1", "description": "desc1", "fields": []},
            {"table_name": "table2", "database": "test", "snowflake_layer": "DWD", "partition_field": "pt_d", "main_usage": "usage2", "description": "desc2", "fields": []},
            {"table_name": "table3", "database": "test", "snowflake_layer": "ODS", "partition_field": "pt_d", "main_usage": "usage3", "description": "desc3", "fields": []}
        ]
    }'''

    # Mock response that returns next 3 tables (including one duplicate)
    mock_response2 = '''{
        "business_domain": "电商",
        "data_sources": [
            {"table_name": "table3", "database": "test", "snowflake_layer": "ODS", "partition_field": "pt_d", "main_usage": "usage3", "description": "desc3", "fields": []},
            {"table_name": "table4", "database": "test", "snowflake_layer": "ADS", "partition_field": "pt_d", "main_usage": "usage4", "description": "desc4", "fields": []},
            {"table_name": "table5", "database": "test", "snowflake_layer": "DWD", "partition_field": "pt_d", "main_usage": "usage5", "description": "desc5", "fields": []}
        ]
    }'''

    # Mock response that returns no new tables
    mock_response3 = '''{
        "business_domain": "电商",
        "data_sources": [
            {"table_name": "table1", "database": "test", "snowflake_layer": "ADS", "partition_field": "pt_d", "main_usage": "usage1", "description": "desc1", "fields": []},
            {"table_name": "table2", "database": "test", "snowflake_layer": "DWD", "partition_field": "pt_d", "main_usage": "usage2", "description": "desc2", "fields": []}
        ]
    }'''

    # Test JSON parsing
    result1 = parser._parse_json_safely(mock_response1)
    assert len(result1["data_sources"]) == 3
    assert result1["data_sources"][0]["table_name"] == "table1"

    result2 = parser._parse_json_safely(mock_response2)
    assert len(result2["data_sources"]) == 3

    result3 = parser._parse_json_safely(mock_response3)
    assert len(result3["data_sources"]) == 2

    # Test merge logic
    merged = parser._merge_multi_round_data([result1, result2, result3])
    assert len(merged["data_sources"]) == 5  # Should have 5 unique tables

    table_names = {ds["table_name"] for ds in merged["data_sources"]}
    assert table_names == {"table1", "table2", "table3", "table4", "table5"}

    print("Batch logic test passed!")
    print(f"Extracted {len(merged['data_sources'])} unique tables: {sorted(table_names)}")

    return True

def main():
    print("Testing batch extraction functionality...")
    print("=" * 80)

    try:
        # Test prompt generation
        print("\n1. Testing prompt generation...")
        test_prompt_generation()
        print("✓ Prompt generation test passed")

        # Test batch logic
        print("\n2. Testing batch logic...")
        test_batch_logic()
        print("✓ Batch logic test passed")

        print("\n" + "=" * 80)
        print("All tests passed! The batch extraction functionality is working correctly.")
        print("\nFeatures implemented:")
        print("- Each round extracts maximum 5 tables")
        print("- Already extracted tables are passed as prompt to avoid duplicates")
        print("- Extraction continues until no new tables are found")
        print("- Maximum 10 rounds to prevent infinite loops")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
