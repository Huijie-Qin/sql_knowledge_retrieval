import sys
from dotenv import load_dotenv
load_dotenv()

from src.main import DataSourceParser
from src.quality_checker import QualityChecker

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "check":
        # 仅运行质量检查
        checker = QualityChecker()
        checker.run()
    else:
        # 运行完整解析流程
        parser = DataSourceParser()
        parser.run()

        # 运行质量检查
        checker = QualityChecker()
        checker.run()

if __name__ == "__main__":
    main()
