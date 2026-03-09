import sys
import argparse
from dotenv import load_dotenv
load_dotenv()

from src.main import DataSourceParser
from src.quality_checker import QualityChecker

def main():
    parser = argparse.ArgumentParser(description="SQL Knowledge Retrieval Parser")
    parser.add_argument("command", nargs="?", default="parse",
                        choices=["parse", "check", "merge"],
                        help="Command to execute: parse (default), check, merge")
    parser.add_argument("--full", "--force", action="store_true",
                        help="Force full reprocessing of all files, ignore previous progress")

    args = parser.parse_args()

    if args.command == "check":
        # 仅运行质量检查，不合并
        checker = QualityChecker()
        checker.run()
    elif args.command == "merge":
        # 运行质量检查并自动合并重复数据源
        checker = QualityChecker()
        report_file, merge_results = checker.run(auto_merge_duplicates=True)
        if merge_results:
            print("\nMerge results:")
            for table_name, merged_files in merge_results.items():
                print(f"- {table_name}: merged {len(merged_files)} duplicate files")
    else:  # parse command (default)
        # 运行完整解析流程
        parser = DataSourceParser()
        parser.run(force_full=args.full)

        # 运行质量检查并自动合并重复数据源
        checker = QualityChecker()
        report_file, merge_results = checker.run(auto_merge_duplicates=True)
        if merge_results:
            print("\nAuto merge results:")
            for table_name, merged_files in merge_results.items():
                print(f"- {table_name}: merged {len(merged_files)} duplicate files")

if __name__ == "__main__":
    main()
