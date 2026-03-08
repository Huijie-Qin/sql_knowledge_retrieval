import sys
from dotenv import load_dotenv
load_dotenv()

from src.main import DataSourceParser
from src.quality_checker import QualityChecker

def main():
    if len(sys.argv) > 1:
        if sys.argv[1] == "check":
            # 仅运行质量检查，不合并
            checker = QualityChecker()
            checker.run()
        elif sys.argv[1] == "merge":
            # 运行质量检查并自动合并重复数据源
            checker = QualityChecker()
            report_file, merge_results = checker.run(auto_merge_duplicates=True)
            if merge_results:
                print("\nMerge results:")
                for table_name, merged_files in merge_results.items():
                    print(f"- {table_name}: merged {len(merged_files)} duplicate files")
    else:
        # 运行完整解析流程
        parser = DataSourceParser()
        parser.run()

        # 运行质量检查并自动合并重复数据源
        checker = QualityChecker()
        report_file, merge_results = checker.run(auto_merge_duplicates=True)
        if merge_results:
            print("\nAuto merge results:")
            for table_name, merged_files in merge_results.items():
                print(f"- {table_name}: merged {len(merged_files)} duplicate files")

if __name__ == "__main__":
    main()
