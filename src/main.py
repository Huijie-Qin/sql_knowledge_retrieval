from pathlib import Path
from typing import List
from config.settings import settings
from src.parser import FileParser
from src.data_source_manager import DataSourceManager
from src.progress_manager import ProgressManager

class DataSourceParser:
    def __init__(self):
        self.parser = FileParser()
        self.data_source_manager = DataSourceManager()
        self.progress_manager = ProgressManager()

    def scan_source_files(self) -> List[Path]:
        """扫描所有待解析的源文件"""
        source_dir = settings.source_dir
        files = []
        for ext in ["*.md", "*.sql"]:
            files.extend(list(source_dir.rglob(ext)))
        return files

    def process_file(self, file_path: Path):
        """处理单个文件"""
        print(f"Processing file: {file_path}")

        # 读取文件内容
        content = file_path.read_text(encoding="utf-8")

        # 解析文件
        if file_path.suffix == ".md":
            parse_result = self.parser.parse_md(content)
        elif file_path.suffix == ".sql":
            parse_result = self.parser.parse_sql(content, file_path.name)
        else:
            print(f"Unsupported file type: {file_path.suffix}")
            return

        business_domain = parse_result.get("business_domain", "其他")
        data_sources = parse_result.get("data_sources", [])

        # 处理每个数据源
        for ds in data_sources:
            ds["business_domain"] = business_domain
            table_name = ds["table_name"]

            # 智能判断：全局存在则更新，否则新建（自动处理跨业务域重复问题）
            ds_file_path, operation_type, update_points = self.data_source_manager.create_or_update_data_source(
                table_name, business_domain, ds
            )

            if operation_type == "更新数据源":
                print(f"Updated existing data source: {table_name}, updates: {update_points}")
            else:
                print(f"Created new data source: {table_name} in {business_domain} domain")

            # 更新进度：使用文件实际所在的业务域（可能和当前解析的不同）
            actual_business_domain = ds_file_path.parent.name
            self.progress_manager.add_data_source_index(table_name, actual_business_domain, ds_file_path)
            self.progress_manager.add_parse_record(table_name, operation_type, update_points)

        # 标记文件为已处理
        self.progress_manager.mark_file_processed(file_path)
        print(f"Completed processing: {file_path}")

    def run(self):
        """运行完整解析流程"""
        print("Starting data source parsing...")

        # 扫描所有源文件
        all_files = self.scan_source_files()
        print(f"Found {len(all_files)} source files")

        # 添加到待解析列表
        self.progress_manager.add_pending_files(all_files)

        # 获取待处理文件
        pending_files = self.progress_manager.get_pending_files()
        print(f"Pending files: {len(pending_files)}")

        # 逐个处理
        for file in pending_files:
            if file.exists():
                self.process_file(file)
            else:
                print(f"File not found: {file}, skipping")

        print("Parsing completed!")

if __name__ == "__main__":
    parser = DataSourceParser()
    parser.run()
