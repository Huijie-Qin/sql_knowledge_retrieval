from pathlib import Path
from typing import Dict, Any, Tuple, List
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

    def exists(self, table_name: str, business_domain: str) -> bool:
        return self._get_data_source_path(table_name, business_domain).exists()

    def create_data_source(self, data: Dict[str, Any], business_domain: str) -> Path:
        """创建新的数据源文件"""
        file_path = self._get_data_source_path(data["table_name"], business_domain)

        content = self._generate_markdown(data)
        file_path.write_text(content, encoding="utf-8")
        return file_path

    def merge_data_source(self, old_content: str, new_data: Dict[str, Any]) -> Tuple[str, List[str]]:
        """合并新旧数据源信息，返回合并后的内容和更新点"""
        # 先生成新数据源的markdown
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
        """更新现有数据源文件"""
        file_path = self._get_data_source_path(table_name, business_domain)
        old_content = file_path.read_text(encoding="utf-8")

        merged_content, update_points = self.merge_data_source(old_content, new_data)
        file_path.write_text(merged_content, encoding="utf-8")
        return file_path, update_points

    def _generate_markdown(self, data: Dict[str, Any]) -> str:
        """生成数据源文件的Markdown内容"""
        md = f"# {data['table_name']}\n\n"
        md += "## 1.数据源基本信息\n\n"
        md += f"### 1.1.数据源名称\n{data.get('name', data['table_name'])}\n\n"
        md += f"### 1.2.数据源描述\n{data.get('description', '')}\n\n"
        md += f"### 1.3.业务域\n{data.get('business_domain', '')}\n\n"

        md += "## 2.数据表结构\n\n"
        md += f"### 2.1.表名\n{data['table_name']}\n\n"
        md += "### 2.2.关键字段\n"
        md += "| 字段名|字段描述 | 用途说明|\n"
        md += "|----------|----------|----------|\n"
        for field in data.get("fields", []):
            md += f"|{field['name']}|{field.get('description', '')}|{field.get('usage', '')}|\n"
        md += "\n"

        md += "## 3.SQL使用示例\n\n"
        for i, example in enumerate(data.get("sql_examples", []), 1):
            md += f"### 3.{i}.{example['name']}\n"
            md += "```sql\n"
            md += example["sql"]
            md += "\n```\n\n"

        md += "## 4.使用说明和注意事项\n\n"
        md += f"### 4.1.使用说明\n{data.get('usage_instructions', '')}\n\n"
        md += f"### 4.2.注意事项\n{data.get('notes', '')}\n\n"

        md += "## 5.数据质量说明\n\n"
        quality = data.get("data_quality", {})
        md += "### 5.1.数据量\n"
        md += f"\t- 日记录数：{quality.get('daily_records', '')}\n"
        md += f"\t- 日覆盖用户数：{quality.get('daily_users', '')}\n\n"
        md += f"### 5.2.数据覆盖情况\n{quality.get('coverage', '')}\n\n"
        md += f"### 5.3.上报及时性\n{quality.get('timeliness', '')}\n\n"

        md += "## 6.关联案例\n"
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
