from pathlib import Path
from typing import List, Dict, Tuple
from config.settings import settings

class QualityChecker:
    def __init__(self):
        self.output_dir = settings.output_dir
        self.report_file = self.output_dir / "检查报告.md"

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

    def generate_report(self, duplicates: List[Tuple[str, List[Path]]], missing: List[str]) -> Path:
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

        content += "## 2.遗漏数据源检测\n"
        if missing:
            content += f"发现 {len(missing)} 个遗漏的数据源：\n\n"
            for table in missing:
                content += f"- {table}\n"
            content += "\n"
        else:
            content += "未发现遗漏数据源\n\n"

        content += "## 3.完整性评估\n"
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

    def run(self, used_tables: List[str] = None):
        """运行质量检查"""
        print("Running quality check...")
        duplicates = self.detect_duplicates()
        missing = self.detect_missing(used_tables or [])
        report_file = self.generate_report(duplicates, missing)
        print(f"Quality check completed. Report saved to: {report_file}")
        return report_file
