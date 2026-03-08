import json
from src.llm_client import LLMClient
from src.prompt_manager import PromptManager
from typing import Dict, Any

class FileParser:
    def __init__(self):
        self.llm_client = LLMClient()
        self.prompt_manager = PromptManager()
        self.system_prompt = "你是专业的SQL数据源分析师，严格按照要求返回JSON格式结果，不要添加任何额外说明。"

    def parse_md(self, content: str) -> Dict[str, Any]:
        prompt = self.prompt_manager.get_prompt("parse_md", content=content)
        response = self.llm_client.chat(prompt, self.system_prompt)
        # 清理可能的markdown标记
        response = response.strip()
        if response.startswith("```json"):
            response = response[7:-3].strip()
        return json.loads(response)

    def parse_sql(self, content: str, filename: str) -> Dict[str, Any]:
        prompt = self.prompt_manager.get_prompt("parse_sql", content=content, filename=filename)
        response = self.llm_client.chat(prompt, self.system_prompt)
        # 清理可能的markdown标记
        response = response.strip()
        if response.startswith("```json"):
            response = response[7:-3].strip()
        return json.loads(response)
