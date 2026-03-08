from pathlib import Path
from typing import List, Dict, Tuple, Optional
from config.settings import settings
from src.data_source_manager import DataSourceManager
from src.progress_manager import ProgressManager

class QualityChecker:
    def __init__(self):
        self.output_dir = settings.output_dir
        self.report_file = self.output_dir / "检查报告.md"
        self.data_source_manager = DataSourceManager()
        self.progress_manager = ProgressManager()

    def scan_all_data_sources(self) -> Dict[str, List[Path]]:
        """扫描所有数据源文件，按表名分组"""
        data_sources = {}
        for md_file in self.output_dir.rglob("*.md"):
            if md_file.name in ["解析进度.md", "检查报告.md"]:
                continue
            # 读取第一行获取表名
            try:
                content = md_file.read_text(encoding="utf-8")
                lines = content.splitlines()
                if lines:
                    first_line = lines[0].strip()
                    if first_line.startswith("# "):
                        table_name = first_line[2:].strip()
                        if table_name not in data_sources:
                            data_sources[table_name] = []
                        data_sources[table_name].append(md_file)
            except Exception as e:
                print(f"Warning: Error reading file {md_file}: {e}")
        return data_sources

    def detect_duplicates(self) -> List[Tuple[str, List[Path]]]:
        """检测重复的数据源"""
        data_sources = self.scan_all_data_sources()
        duplicates = []
        for table_name, files in data_sources.items():
            if len(files) > 1:
                duplicates.append((table_name, files))
        return duplicates

    def detect_missing(self, used_tables: List[str]) -> List[str]:
        """检测遗漏的数据源"""
        data_sources = self.scan_all_data_sources()
        existing_tables = set(data_sources.keys())
        missing = [table for table in used_tables if table not in existing_tables]
        return missing

    def generate_report(self, duplicates: List[Tuple[str, List[Path]]], missing: List[str], merge_results: Dict[str, List[str]] = None) -> Path:
        """生成检查报告"""
        content = "# 数据源检查报告\n\n"

        content += "## 1.重复数据源检测\n"
        if duplicates:
            content += f"发现 {len(duplicates)} 个重复数据源：\n\n"
            for table_name, files in duplicates:
                content += f"### {table_name}\n"
                for f in files:
                    content += f"- {f.relative_to(settings.output_dir)}\n"
                content += "\n"
        else:
            content += "未发现重复数据源\n\n"

        if merge_results is not None and merge_results:
            content += "## 2.重复数据源合并结果\n"
            content += f"已成功合并 {len(merge_results)} 个重复数据源：\n\n"
            for table_name, merged_files in merge_results.items():
                content += f"### {table_name}\n"
                content += f"已合并以下重复文件：\n"
                for f_path in merged_files:
                    f = Path(f_path)
                    content += f"- {f.relative_to(settings.output_dir) if f.is_absolute() else f_path}\n"
                content += "\n"

        content += "## 3.遗漏数据源检测\n"
        if missing:
            content += f"发现 {len(missing)} 个遗漏的数据源：\n\n"
            for table in missing:
                content += f"- {table}\n"
            content += "\n"
        else:
            content += "未发现遗漏数据源\n\n"

        content += "## 2.遗漏数据源检测\n"
        if missing:
            content += f"发现 {len(missing)} 个遗漏的数据源：\n\n"
            for table in missing:
                content += f"- {table}\n"
            content += "\n"
        else:
            content += "未发现遗漏数据源\n\n"

        content += "## 4.完整性评估\n"
        data_sources = self.scan_all_data_sources()
        total_tables = len(data_sources)
        content += f"- 总数据源数量：{total_tables}\n"
        content += f"- 重复数据源数量：{len(duplicates)}\n"
        content += f"- 遗漏数据源数量：{len(missing)}\n"
        total_expected = max(total_tables + len(missing), 1)
        completeness = 100 - (len(missing) / total_expected * 100)
        content += f"- 完整性：{completeness:.1f}%\n"

        self.report_file.write_text(content, encoding="utf-8")
        return self.report_file

    def merge_duplicates(self, duplicates: Optional[List[Tuple[str, List[Path]]]] = None) -> Dict[str, List[str]]:
        """
        合并重复数据源，合并逻辑与"更新数据源"操作保持一致
        返回合并结果：{表名: [被合并的文件路径]}
        """
        if duplicates is None:
            duplicates = self.detect_duplicates()

        merge_results = {}

        for table_name, files in duplicates:
            if len(files) <= 1:
                continue

            print(f"Merging duplicate data source: {table_name}, found {len(files)} copies")

            # 选择主文件：优先选择文件内容最多的作为主文件
            main_file = max(files, key=lambda f: f.stat().st_size)
            other_files = [f for f in files if f != main_file]

            merged_files = []
            # 读取主文件内容
            main_content = main_file.read_text(encoding="utf-8")

            # 从主文件路径提取业务域
            business_domain = main_file.parent.name

            for other_file in other_files:
                try:
                    print(f"Merging {other_file} into {main_file}")

                    # 读取待合并文件内容
                    other_content = other_file.read_text(encoding="utf-8")

                    # 直接传入两个markdown内容进行合并，复用现有merge逻辑
                    merged_content, update_points = self.data_source_manager.merge_data_source(
                        old_content=main_content,
                        new_content=other_content
                    )

                    # 更新主文件
                    main_file.write_text(merged_content, encoding="utf-8")
                    main_content = merged_content

                    # 记录合并操作到解析记录
                    self.progress_manager.add_parse_record(
                        table_name,
                        "更新数据源",
                        update_points + ["合并重复数据源"]
                    )

                    # 删除重复文件
                    other_file.unlink()
                    merged_files.append(str(other_file))

                    print(f"Successfully merged {other_file}, updates: {update_points}")

                except Exception as e:
                    print(f"Error merging {other_file}: {e}")
                    continue

            merge_results[table_name] = merged_files

        return merge_results

    def run(self, used_tables: List[str] = None, auto_merge_duplicates: bool = False) -> Tuple[Path, Dict[str, List[str]]]:
        """
        运行质量检查
        :param used_tables: 案例中使用的表名列表，用于检测遗漏
        :param auto_merge_duplicates: 是否自动合并重复数据源
        :return: (报告文件路径, 合并结果)
        """
        print("Running quality check...")
        duplicates = self.detect_duplicates()
        missing = self.detect_missing(used_tables or [])

        merge_results = {}
        if auto_merge_duplicates and duplicates:
            print(f"Found {len(duplicates)} duplicate data sources, auto merging...")
            merge_results = self.merge_duplicates(duplicates)
            # 合并后重新扫描数据源更新报告
            duplicates = self.detect_duplicates()

        report_file = self.generate_report(duplicates, missing, merge_results)
        print(f"Quality check completed. Report saved to: {report_file}")

        if merge_results:
            print(f"Merge completed: {len(merge_results)} tables merged")

        return report_file, merge_results
