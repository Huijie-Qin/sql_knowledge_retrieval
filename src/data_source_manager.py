from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional
from src.llm_client import LLMClient
from src.prompt_manager import PromptManager
from config.settings import settings
import json

class DataSourceManager:
    def __init__(self):
        self.output_dir = Path(settings.output_dir)
        self.llm_client = LLMClient()
        self.prompt_manager = PromptManager()

    def _get_data_source_path(self, table_name: str, business_domain: str) -> Path:
        domain_dir = self.output_dir / business_domain
        domain_dir.mkdir(parents=True, exist_ok=True)
        return domain_dir / f"{table_name}.md"

    def find_data_source(self, table_name: str) -> Optional[Path]:
        """
        全局查找指定表名的数据源文件，搜索所有业务域目录
        返回找到的文件路径，如果不存在返回None
        如果找到多个重复的，返回内容最多的那个
        """
        matched_files = []
        for md_file in self.output_dir.rglob("*.md"):
            if md_file.name in ["解析进度.md", "检查报告.md"]:
                continue
            try:
                first_line = md_file.read_text(encoding="utf-8").splitlines()[0].strip()
                if first_line.startswith("# ") and first_line[2:].strip() == table_name:
                    matched_files.append(md_file)
            except Exception:
                continue

        if not matched_files:
            return None

        # 如果有多个匹配，返回文件最大的那个作为主文件
        if len(matched_files) > 1:
            return max(matched_files, key=lambda f: f.stat().st_size)

        return matched_files[0]

    def exists(self, table_name: str, business_domain: str = None) -> bool:
        """
        检查数据源是否存在
        如果指定business_domain，仅检查该业务域下
        否则全局检查所有业务域
        """
        if business_domain is not None:
            return self._get_data_source_path(table_name, business_domain).exists()
        return self.find_data_source(table_name) is not None

    def create_data_source(self, data: Dict[str, Any], business_domain: str) -> Path:
        """创建新的数据源文件"""
        file_path = self._get_data_source_path(data["table_name"], business_domain)

        content = self._generate_markdown(data)
        file_path.write_text(content, encoding="utf-8")
        return file_path

    def merge_data_source(self, old_content: str, new_data: Dict[str, Any] = None, new_content: str = None) -> Tuple[str, List[str]]:
        """
        合并新旧数据源信息，返回合并后的内容和更新点
        可以传入结构化的new_data，或者直接传入已经生成好的new_content
        """
        if new_content is None:
            if new_data is None:
                raise ValueError("Either new_data or new_content must be provided")
            # 生成新数据源的markdown
            new_content = self._generate_markdown(new_data)

        # 调用LLM进行智能合并
        prompt = self.prompt_manager.get_prompt(
            "merge_data_source",
            old_content=old_content,
            new_content=new_content
        )
        merged_content = self.llm_client.chat(prompt)

        # 识别更新点
        update_points = self._detect_update_points(old_content, merged_content)
        return merged_content, update_points

    def update_data_source(self, table_name: str, business_domain: str, new_data: Dict[str, Any]) -> Tuple[Path, List[str]]:
        """
        更新现有数据源文件
        优先全局查找已存在的文件，不存在则在指定business_domain下创建
        """
        # 先全局查找是否已存在该表
        existing_file = self.find_data_source(table_name)
        if existing_file:
            file_path = existing_file
        else:
            file_path = self._get_data_source_path(table_name, business_domain)

        old_content = file_path.read_text(encoding="utf-8")
        merged_content, update_points = self.merge_data_source(old_content, new_data)
        file_path.write_text(merged_content, encoding="utf-8")
        return file_path, update_points

    def create_or_update_data_source(self, table_name: str, business_domain: str, new_data: Dict[str, Any]) -> Tuple[Path, str, List[str]]:
        """
        智能创建或更新数据源
        如果全局已存在则更新，否则创建
        返回 (文件路径, 操作类型, 更新点列表)
        """
        existing_file = self.find_data_source(table_name)
        if existing_file:
            # 已存在，执行更新
            file_path, update_points = self.update_data_source(table_name, business_domain, new_data)
            return file_path, "更新数据源", update_points
        else:
            # 不存在，新建
            file_path = self.create_data_source(new_data, business_domain)
            return file_path, "新建数据源", []

    def _generate_markdown(self, data: Dict[str, Any]) -> str:
        """生成数据源文件的Markdown内容"""
        md = f"# {data['table_name']}\n\n"
        md += "## 数据源基本信息\n"
        md += f"\t- 表名：{data.get('table_name', '')}\n"
        md += f"\t- 业务域：{data.get('business_domain', '')}\n"
        md += f"\t- 数据库：{data.get('database', data.get('table_name', '').split('.')[0] if '.' in data.get('table_name', '') else '')}\n"
        md += f"\t- 雪花层：{data.get('snowflake_layer', '')}\n"
        md += f"\t- 分区字段：{data.get('partition_field', '')}\n"
        md += f"\t- 主要用途：{data.get('main_usage', '')}\n"
        md += f"\t- 数据源描述：{data.get('description', '')}\n\n"

        # 业务场景
        scenarios = data.get("typical_application_scenarios", [])
        if scenarios:
            md += "## 业务场景\n"
            for scenario in scenarios:
                md += f"{scenario}\n"
            md += "\n"

        md += "## 2.数据表结构\n\n"
        md += f"### 2.1.表名\n{data['table_name']}\n\n"
        md += "### 2.2.关键字段\n"
        md += "| 字段名|字段描述 | 用途说明 | 枚举值说明|\n"
        md += "|----------|----------|----------|----------|\n"
        for field in data.get("fields", []):
            enum_values = field.get("enum_values", "")
            md += f"|{field['name']}|{field.get('description', '')}|{field.get('usage', '')}|{enum_values}|\n"
        md += "\n"

        md += "## 3.SQL使用示例\n\n"
        for i, example in enumerate(data.get("sql_examples", []), 1):
            md += f"### 3.{i}.{example['name']}\n"
            if example.get("description"):
                md += f"{example['description']}\n\n"
            md += "```sql\n"
            md += example["sql"]
            md += "\n```\n\n"

        # 关键查询模式
        key_patterns = data.get("key_query_patterns", [])
        if key_patterns:
            md += "## 4.关键查询模式\n\n"
            for pattern in key_patterns:
                md += f"- {pattern}\n"
            md += "\n"

        # 常用关联表
        related_tables = data.get("common_related_tables", [])
        if related_tables:
            md += "## 5.常用关联表\n\n"
            md += "| 表名 | 关联字段 | 关联用途 |\n"
            md += "|------|----------|----------|\n"
            for table in related_tables:
                md += f"|{table.get('table_name', '')}|{table.get('join_field', '')}|{table.get('usage', '')}|\n"
            md += "\n"

        # 调整后续章节编号 - 典型应用场景已经移动到前面作为"业务场景"章节
        md += "## 6.使用说明和注意事项\n\n"
        md += f"### 6.1.使用说明\n{data.get('usage_instructions', '')}\n\n"
        md += f"### 6.2.注意事项\n{data.get('notes', '')}\n\n"

        md += "## 7.数据质量说明\n\n"
        quality = data.get("data_quality", {})
        md += "### 7.1.数据量\n"
        md += f"\t- 日记录数：{quality.get('daily_records', '')}\n"
        md += f"\t- 日覆盖用户数：{quality.get('daily_users', '')}\n\n"
        md += f"### 7.2.数据覆盖情况\n{quality.get('coverage', '')}\n\n"
        md += f"### 7.3.上报及时性\n{quality.get('timeliness', '')}\n\n"

        md += "## 8.关联案例\n"
        md += "|案例名称|案例类型|使用场景|\n"
        md += "|------------|------------|------------|\n"
        for case in data.get("related_cases", []):
            md += f"|{case['name']}|{case.get('type', '')}|{case.get('scenario', '')}|\n"

        return md


    def _detect_update_points(self, old_content: str, new_content: str) -> List[str]:
        """检测更新点，返回更新类型列表"""
        update_points = []

        # 简单的规则检测更新类型
        old_lines = set(old_content.splitlines())
        new_lines = set(new_content.splitlines())
        added_lines = new_lines - old_lines

        added_text = "\n".join(added_lines)

        if "```sql" in added_text:
            update_points.append("新增SQL示例")
        if "字段名" in added_text:
            update_points.append("补充字段说明")
        if "枚举值说明" in added_text or "enum_values" in added_text:
            update_points.append("更新字段枚举值")
        if any(k in added_text for k in ["数据库：", "雪花层：", "分区字段：", "主要用途："]):
            update_points.append("更新数据源基本信息")
        if "业务场景" in added_text:
            update_points.append("补充业务场景信息")
        if "关键查询模式" in added_text:
            update_points.append("补充关键查询模式")
        if "常用关联表" in added_text:
            update_points.append("新增常用关联表信息")
        if "数据质量" in added_text or "日记录数" in added_text:
            update_points.append("更新数据质量信息")
        if "关联案例" in added_text or "案例名称" in added_text:
            update_points.append("新增关联案例")
        if "使用说明" in added_text or "注意事项" in added_text:
            update_points.append("完善使用信息")
        if any(k in added_text for k in ["数据源描述", "字段描述"]):
            update_points.append("修正描述信息")

        if not update_points:
            update_points.append("合并重复信息")

        return update_points
