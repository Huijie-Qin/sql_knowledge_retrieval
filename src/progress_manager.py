from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
from config.settings import settings

class ProgressManager:
    def __init__(self):
        self.progress_file = settings.output_dir / "解析进度.md"
        self._initialize()

    def _initialize(self):
        """初始化进度文件"""
        if not self.progress_file.exists():
            settings.output_dir.mkdir(parents=True, exist_ok=True)
            content = """# 数据源解析进度

## 待解析文件

### 案例文件（.sql 或者 .md）

## 已解析文件

## 数据源索引
|数据源表名|业务域|文件路径|
|-----------|-----------|-----------|

## 解析记录
|数据源名称|解析时间|操作类型| 更新内容|
|-----------|-----------|-----------|-----------|
"""
            self.progress_file.write_text(content, encoding="utf-8")

    def add_pending_files(self, files: List[Path], skip_processed: bool = True):
        """
        批量添加待解析文件
        :param skip_processed: 是否跳过已经处理过的文件（默认True，增量模式）
        """
        content = self.progress_file.read_text(encoding="utf-8")
        pending_section = "### 案例文件（.sql 或者 .md）"
        lines = content.splitlines()

        # 找到待解析文件部分
        start_idx = None
        end_idx = None
        for i, line in enumerate(lines):
            if pending_section in line:
                start_idx = i + 1
            elif start_idx is not None and line.startswith("## "):
                end_idx = i
                break

        if start_idx is None:
            return

        # 获取已处理的文件列表
        processed_files = set(self.get_processed_files()) if skip_processed else set()

        # 添加新文件
        existing_pending = set()
        for line in lines[start_idx:end_idx]:
            if line.startswith("- [ ] "):
                existing_pending.add(line[6:].strip())

        new_lines = []
        for file in files:
            file_path = file.as_posix()
            # 增量模式下跳过已处理的，同时避免重复添加到待处理列表
            if file_path not in existing_pending and (not skip_processed or file_path not in processed_files):
                new_lines.append(f"- [ ] {file_path}")

        if new_lines:
            lines[start_idx:start_idx] = new_lines
            self.progress_file.write_text("\n".join(lines), encoding="utf-8")

    def add_pending_file(self, file: Path):
        """添加单个待解析文件"""
        self.add_pending_files([file])

    def mark_file_processed(self, file: Path):
        """标记文件为已处理"""
        content = self.progress_file.read_text(encoding="utf-8")
        file_path = file.as_posix()

        # 从待解析中移除，添加到已解析
        lines = content.splitlines()
        new_lines = []
        processed = False

        for line in lines:
            if line == f"- [ ] {file_path}":
                processed = True
                continue
            if line == "## 已解析文件" and processed:
                new_lines.append(line)
                new_lines.append(f"- [x] {file_path}")
                processed = False
                continue
            new_lines.append(line)

        self.progress_file.write_text("\n".join(new_lines), encoding="utf-8")

    def add_data_source_index(self, table_name: str, business_domain: str, file_path: Path):
        """添加数据源索引"""
        content = self.progress_file.read_text(encoding="utf-8")
        lines = content.splitlines()

        # 找到数据源索引表格
        start_idx = None
        end_idx = None
        for i, line in enumerate(lines):
            if line == "## 数据源索引":
                start_idx = i + 3  # 跳过表头
            elif start_idx is not None and line.startswith("## "):
                end_idx = i
                break

        if start_idx is None:
            return

        # 检查是否已存在
        exists = False
        for line in lines[start_idx:end_idx]:
            if line.startswith(f"|{table_name}|"):
                exists = True
                break

        if not exists:
            relative_path = file_path.relative_to(settings.output_dir).as_posix()
            new_line = f"|{table_name}| {business_domain} |{relative_path}|"
            lines.insert(start_idx, new_line)
            self.progress_file.write_text("\n".join(lines), encoding="utf-8")

    def add_parse_record(self, table_name: str, operation_type: str, update_points: List[str] = None):
        """添加解析记录"""
        content = self.progress_file.read_text(encoding="utf-8")
        lines = content.splitlines()

        # 找到解析记录表格
        start_idx = None
        for i, line in enumerate(lines):
            if line == "## 解析记录":
                start_idx = i + 3  # 跳过表头
                break

        if start_idx is None:
            return

        now = datetime.now().strftime("%Y%m%d-%H%M%S")
        update_content = "；".join(update_points) if update_points else ""
        new_line = f"|{table_name}| {now}|{operation_type}| {update_content}|"
        lines.insert(start_idx, new_line)

        self.progress_file.write_text("\n".join(lines), encoding="utf-8")

    def get_processed_files(self) -> List[str]:
        """获取已解析的文件路径列表"""
        content = self.progress_file.read_text(encoding="utf-8")
        lines = content.splitlines()

        start_idx = None
        end_idx = None
        for i, line in enumerate(lines):
            if line == "## 已解析文件":
                start_idx = i + 1
            elif start_idx is not None and line.startswith("## "):
                end_idx = i
                break

        processed_files = []
        if start_idx is not None:
            for line in lines[start_idx:end_idx]:
                if line.startswith("- [x] "):
                    file_path = line[6:].strip()
                    processed_files.append(file_path)

        return processed_files

    def get_pending_files(self) -> List[Path]:
        """获取待解析文件列表"""
        content = self.progress_file.read_text(encoding="utf-8")
        lines = content.splitlines()

        pending_section = "### 案例文件（.sql 或者 .md）"
        start_idx = None
        end_idx = None
        for i, line in enumerate(lines):
            if pending_section in line:
                start_idx = i + 1
            elif start_idx is not None and line.startswith("## "):
                end_idx = i
                break

        pending_files = []
        if start_idx is not None:
            for line in lines[start_idx:end_idx]:
                if line.startswith("- [ ] "):
                    file_path = Path(line[6:].strip())
                    pending_files.append(file_path)

        return pending_files

    def reset_progress(self):
        """重置解析进度，将所有已解析文件移回待解析列表，实现全量重新处理"""
        content = self.progress_file.read_text(encoding="utf-8")
        lines = content.splitlines()

        # 1. 收集所有已处理的文件
        processed_files = self.get_processed_files()
        if not processed_files:
            return

        # 2. 从已解析列表中移除
        new_lines = []
        in_processed_section = False
        for line in lines:
            if line == "## 已解析文件":
                in_processed_section = True
                new_lines.append(line)
                continue
            if in_processed_section and line.startswith("## "):
                in_processed_section = False
            if in_processed_section and line.startswith("- [x] "):
                continue  # 跳过已处理的文件行
            new_lines.append(line)

        # 3. 添加到待解析列表的头部
        pending_section = "### 案例文件（.sql 或者 .md）"
        for i, line in enumerate(new_lines):
            if pending_section in line:
                insert_idx = i + 1
                # 插入待处理文件
                for file_path in processed_files:
                    new_lines.insert(insert_idx, f"- [ ] {file_path}")
                break

        # 4. 保存更新后的进度文件
        self.progress_file.write_text("\n".join(new_lines), encoding="utf-8")
