#!/usr/bin/env python3
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Add src directory to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.parser import FileParser as Parser
from src.data_source_manager import DataSourceManager
from src.prompt_manager import PromptManager
from src.llm_client import LLMClient

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <file_path>")
        sys.exit(1)

    file_path = Path(sys.argv[1])
    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        sys.exit(1)

    print(f"Processing file: {file_path}")
    print(f"Using multi-round extraction: {os.getenv('USE_MULTI_ROUND_EXTRACTION', 'true')}")
    print("-" * 80)

    # Initialize components
    parser = Parser()
    data_source_manager = DataSourceManager()

    try:
        # Parse the file
        if file_path.suffix == ".sql":
            content = file_path.read_text(encoding="utf-8")
            result = parser.parse_sql_multi_round(content, file_path.name)
        elif file_path.suffix == ".md":
            content = file_path.read_text(encoding="utf-8")
            result = parser.parse_md_multi_round(content)
        else:
            print(f"Unsupported file type: {file_path.suffix}")
            sys.exit(1)

        print("Parsing completed successfully!")
        print(f"Extracted {len(result.get('data_sources', []))} data sources")

        # Process each data source
        business_domain = result.get("business_domain", "unknown")
        for ds in result.get("data_sources", []):
            print(f"\nProcessing table: {ds['table_name']}")
            file_path, action, updates = data_source_manager.create_or_update_data_source(
                ds["table_name"], business_domain, ds
            )
            print(f"Result: {action} - {file_path}")
            if updates:
                print(f"Updated fields: {', '.join(updates)}")

        print("\n" + "-" * 80)
        print("Processing completed successfully!")
        print("Please check the generated data source files for completeness.")

    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
