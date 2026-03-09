import json
import re
import time
from src.llm_client import LLMClient
from src.prompt_manager import PromptManager
from typing import Dict, Any

class FileParser:
    def __init__(self):
        self.llm_client = LLMClient()
        self.prompt_manager = PromptManager()
        self.system_prompt = """你是专业的SQL数据源分析师，严格按照要求返回JSON格式结果：
1. 不要添加任何额外的说明、解释、markdown标记
2. 确保JSON格式100%合法，所有字符串中的双引号必须转义
3. 确保返回完整的JSON结构，不要截断
4. 所有字段必须严格按照要求的格式返回

返回结果必须是可以直接被json.loads()解析的合法JSON。"""
        self.max_retries = 3  # 最多重试3次

    def _parse_json_safely(self, response: str) -> Dict[str, Any]:
        """安全解析JSON，自动修复常见格式问题"""
        # 清理markdown标记
        response = response.strip()
        if response.startswith("```json"):
            response = response[7:].strip()
        if response.endswith("```"):
            response = response[:-3].strip()

        # 尝试修复不完整的JSON（自动补全闭合）
        def fix_incomplete_json(s: str) -> str:
            # 计数括号和引号
            braces = []
            brackets = []
            in_string = False
            escape = False

            for c in s:
                if escape:
                    escape = False
                    continue
                if c == '\\':
                    escape = True
                    continue
                if c == '"' and not escape:
                    in_string = not in_string
                if not in_string:
                    if c == '{':
                        braces.append('}')
                    elif c == '[':
                        brackets.append(']')
                    elif c == '}' and braces:
                        braces.pop()
                    elif c == ']' and brackets:
                        brackets.pop()

            # 补全缺失的闭合符号
            s = s.rstrip().rstrip(',')  # 去掉末尾的逗号
            s += ''.join(reversed(braces)) + ''.join(reversed(brackets))
            return s

        # 先尝试正常解析
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            # 尝试修复后解析
            try:
                fixed_response = fix_incomplete_json(response)
                return json.loads(fixed_response)
            except json.JSONDecodeError:
                # 尝试用正则提取JSON部分
                match = re.search(r'\{.*\}', response, re.DOTALL)
                if match:
                    try:
                        return json.loads(match.group(0))
                    except:
                        pass
                # 所有方法都失败
                raise ValueError(f"Failed to parse JSON after fix attempts: {response[:500]}...")

    def parse_md(self, content: str) -> Dict[str, Any]:
        prompt = self.prompt_manager.get_prompt("parse_md", content=content)
        for attempt in range(self.max_retries):
            try:
                response = self.llm_client.chat(prompt, self.system_prompt)
                return self._parse_json_safely(response)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(1)  # 重试前等待
                print(f"Parse failed, retrying ({attempt+1}/{self.max_retries}): {e}")

    def parse_sql(self, content: str, filename: str) -> Dict[str, Any]:
        prompt = self.prompt_manager.get_prompt("parse_sql", content=content, filename=filename)
        for attempt in range(self.max_retries):
            try:
                response = self.llm_client.chat(prompt, self.system_prompt)
                return self._parse_json_safely(response)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(1)  # 重试前等待
                print(f"Parse failed, retrying ({attempt+1}/{self.max_retries}): {e}")
