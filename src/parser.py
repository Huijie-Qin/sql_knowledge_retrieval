import json
import re
import time
from src.llm_client import LLMClient
from src.prompt_manager import PromptManager
from config.settings import settings
from typing import Dict, Any

class FileParser:
    def __init__(self):
        self.llm_client = LLMClient()
        self.prompt_manager = PromptManager()
        self.system_prompt = """你是专业的SQL数据源分析师，严格按照要求返回JSON格式结果：
1. 不要添加任何额外的说明、解释、markdown标记
2. 确保JSON格式100%合法，所有字符串中的双引号必须转义
3. 必须返回完整的JSON结构，绝对不要截断，所有字段都要完整输出
4. 所有字段必须严格按照要求的格式返回
5. 即使输出内容很长，也要完整返回，不要省略任何部分

返回结果必须是可以直接被json.loads()解析的合法JSON，确保所有括号、引号都正确闭合。"""
        self.max_retries = 3  # 最多重试3次

    def _parse_json_safely(self, response: str) -> Dict[str, Any]:
        """安全解析JSON，自动修复常见格式问题"""
        # 清理markdown标记
        response = response.strip()
        if response.startswith("```json"):
            response = response[7:].strip()
        if response.endswith("```"):
            response = response[:-3].strip()

        # 清理过度转义的反斜杠，处理 " -> \" -> \\" 等多层转义，同时保留合法的转义序列
        def unescape_json(s: str) -> str:
            # 首先处理多层转义的引号：将 \\\" 变为 \"，将 \\\\\" 变为 \\\" 等，保持正确的转义层级
            s = re.sub(r'\\+(")', lambda m: '\\' * (len(m.group(0)) // 2) + '"', s)

            # 处理无效的转义序列：将 \ 后面不是合法JSON转义字符的情况替换为普通的 \
            # JSON合法转义字符：" \ / b f n r t uXXXX
            def replace_invalid_escape(match):
                escape_char = match.group(1)
                if escape_char in '"\\/bfnrtu':
                    return match.group(0)
                # 无效转义，保留反斜杠和字符，或只保留字符？这里选择保留字符，避免解析错误
                return escape_char

            s = re.sub(r'\\(.)', replace_invalid_escape, s)
            return s

        # 尝试清理转义
        response = unescape_json(response)

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
            # 尝试用strict=False模式解析，允许控制字符和一些格式问题
            try:
                return json.loads(response, strict=False)
            except json.JSONDecodeError:
                # 尝试修复后解析
                try:
                    fixed_response = fix_incomplete_json(response)
                    return json.loads(fixed_response, strict=False)
                except json.JSONDecodeError:
                    # 尝试用正则提取JSON部分
                    match = re.search(r'\{.*\}', response, re.DOTALL)
                    if match:
                        try:
                            return json.loads(match.group(0), strict=False)
                        except:
                            pass
                    # 检测是否为截断的响应
                    def is_response_truncated(s: str) -> bool:
                        # 常见截断特征：
                        # 1. 末尾不是有效的JSON结束字符
                        s_stripped = s.rstrip()
                        if not s_stripped.endswith(('}', ']', '"')):
                            return True
                        # 2. 括号不匹配
                        open_braces = s.count('{')
                        close_braces = s.count('}')
                        open_brackets = s.count('[')
                        close_brackets = s.count(']')
                        if open_braces != close_braces or open_brackets != close_brackets:
                            return True
                        # 3. 字符串没有闭合
                        quote_count = s.count('"')
                        if quote_count % 2 != 0:
                            return True
                        # 4. 末尾有明显的截断痕迹（如："description": "xxx", 后面没有内容）
                        if s_stripped.endswith((',', ':', '=')):
                            return True
                        return False

                    # 检查是否被截断
                    if is_response_truncated(response):
                        raise ValueError(f"Response truncated (incomplete JSON): {response[-200:]}...")

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

    def parse_sql_multi_round(self, content: str, filename: str) -> Dict[str, Any]:
        """分三轮抽取SQL文件信息，确保信息完整"""
        rounds = []
        # 第一轮：基础信息抽取
        for attempt in range(self.max_retries):
            try:
                prompt = self.prompt_manager.get_prompt("parse_sql_round1", content=content, filename=filename)
                response = self.llm_client.chat(prompt, self.system_prompt)
                round1 = self._parse_json_safely(response)
                rounds.append(round1)
                break
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(1)
                print(f"SQL Round 1 parse failed, retrying ({attempt+1}/{self.max_retries}): {e}")

        # 第二轮：深度信息抽取
        for attempt in range(self.max_retries):
            try:
                prompt = self.prompt_manager.get_prompt("parse_sql_round2", content=content, filename=filename, round1_result=json.dumps(round1, ensure_ascii=False))
                response = self.llm_client.chat(prompt, self.system_prompt)
                round2 = self._parse_json_safely(response)
                rounds.append(round2)
                break
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(1)
                print(f"SQL Round 2 parse failed, retrying ({attempt+1}/{self.max_retries}): {e}")

        # 第三轮：补充验证抽取
        for attempt in range(self.max_retries):
            try:
                prompt = self.prompt_manager.get_prompt("parse_sql_round3", content=content, filename=filename,
                                                      round1_result=json.dumps(round1, ensure_ascii=False),
                                                      round2_result=json.dumps(round2, ensure_ascii=False))
                response = self.llm_client.chat(prompt, self.system_prompt)
                round3 = self._parse_json_safely(response)
                rounds.append(round3)
                break
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(1)
                print(f"SQL Round 3 parse failed, retrying ({attempt+1}/{self.max_retries}): {e}")

        # 合并三轮结果
        return self._merge_multi_round_data(rounds)

    def parse_md_multi_round(self, content: str) -> Dict[str, Any]:
        """分三轮抽取MD文件信息，确保信息完整"""
        rounds = []
        # 第一轮：基础信息抽取
        for attempt in range(self.max_retries):
            try:
                prompt = self.prompt_manager.get_prompt("parse_md_round1", content=content)
                response = self.llm_client.chat(prompt, self.system_prompt)
                round1 = self._parse_json_safely(response)
                rounds.append(round1)
                break
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(1)
                print(f"MD Round 1 parse failed, retrying ({attempt+1}/{self.max_retries}): {e}")

        # 第二轮：深度信息抽取
        for attempt in range(self.max_retries):
            try:
                prompt = self.prompt_manager.get_prompt("parse_md_round2", content=content, round1_result=json.dumps(round1, ensure_ascii=False))
                response = self.llm_client.chat(prompt, self.system_prompt)
                round2 = self._parse_json_safely(response)
                rounds.append(round2)
                break
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(1)
                print(f"MD Round 2 parse failed, retrying ({attempt+1}/{self.max_retries}): {e}")

        # 第三轮：补充验证抽取
        for attempt in range(self.max_retries):
            try:
                prompt = self.prompt_manager.get_prompt("parse_md_round3", content=content,
                                                      round1_result=json.dumps(round1, ensure_ascii=False),
                                                      round2_result=json.dumps(round2, ensure_ascii=False))
                response = self.llm_client.chat(prompt, self.system_prompt)
                round3 = self._parse_json_safely(response)
                rounds.append(round3)
                break
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(1)
                print(f"MD Round 3 parse failed, retrying ({attempt+1}/{self.max_retries}): {e}")

        # 合并三轮结果
        return self._merge_multi_round_data(rounds)

    def _merge_multi_round_data(self, rounds: list) -> Dict[str, Any]:
        """合并多轮抽取结果，取最全最准确的信息"""
        if not rounds:
            return {}

        # 以第一轮结果为基础
        merged = rounds[0].copy() if isinstance(rounds[0], dict) else {}

        # 合并策略：
        # 1. 所有轮次中存在的字段都保留
        # 2. 数组类型字段进行合并去重
        # 3. 字符串类型字段取最长的内容
        # 4. 数值类型字段取最后一轮的值
        # 5. 嵌套对象递归合并
        # 6. 数据源数组按table_name合并

        def merge_values(old_val, new_val):
            if old_val is None:
                return new_val
            if new_val is None:
                return old_val

            # 数组类型合并去重
            if isinstance(old_val, list) and isinstance(new_val, list):
                # 尝试转换为可哈希类型去重
                try:
                    combined = old_val + new_val
                    # 如果是空数组，直接返回
                    if not combined:
                        return []
                    # 如果是字典数组
                    if isinstance(combined[0], dict):
                        # 数据源数组按table_name合并
                        if all('table_name' in item for item in combined):
                            items_by_table = {}
                            for item in combined:
                                table_name = item['table_name']
                                if table_name not in items_by_table:
                                    items_by_table[table_name] = item.copy()
                                else:
                                    # 合并同表的信息
                                    merged_item = merge_values(items_by_table[table_name], item)
                                    items_by_table[table_name] = merged_item
                            return list(items_by_table.values())
                        # 字段数组按name去重，保留最新的信息
                        elif all('name' in item for item in combined):
                            items_by_name = {}
                            for item in combined:
                                name = item['name']
                                if name not in items_by_name:
                                    items_by_name[name] = item.copy()
                                else:
                                    # 合并同名字段的信息
                                    merged_item = merge_values(items_by_name[name], item)
                                    items_by_name[name] = merged_item
                            return list(items_by_name.values())
                        # 其他字典数组按内容去重
                        else:
                            seen = set()
                            unique = []
                            for item in combined:
                                item_str = json.dumps(item, sort_keys=True, ensure_ascii=False)
                                if item_str not in seen:
                                    seen.add(item_str)
                                    unique.append(item)
                            return unique
                    # 普通数组去重
                    else:
                        return list(dict.fromkeys(combined))
                except Exception as e:
                    # 去重失败返回合并后的数组
                    print(f"Merge array warning: {e}, returning combined array")
                    return old_val + new_val

            # 字典类型递归合并
            if isinstance(old_val, dict) and isinstance(new_val, dict):
                merged_dict = old_val.copy()
                for key, new_value in new_val.items():
                    if key in merged_dict:
                        merged_dict[key] = merge_values(merged_dict[key], new_value)
                    else:
                        merged_dict[key] = new_value
                return merged_dict

            # 字符串类型取最长的
            if isinstance(old_val, str) and isinstance(new_val, str):
                return new_val if len(new_val) > len(old_val) else old_val

            # 其他类型取最后一轮的值
            return new_val

        # 按顺序合并后续所有轮次的结果
        for round_data in rounds[1:]:
            if not isinstance(round_data, dict):
                continue
            merged = merge_values(merged, round_data)

        return merged

    def parse(self, content: str, file_type: str, filename: str = None) -> Dict[str, Any]:
        """
        统一解析入口，根据配置选择单轮或多轮抽取模式
        :param content: 文件内容
        :param file_type: 文件类型，支持 'md' 或 'sql'
        :param filename: 文件名（仅SQL文件需要）
        :return: 解析结果
        """
        if settings.use_multi_round_extraction:
            if file_type == "md":
                return self.parse_md_multi_round(content)
            elif file_type == "sql":
                if not filename:
                    raise ValueError("filename is required for SQL parsing")
                return self.parse_sql_multi_round(content, filename)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
        else:
            if file_type == "md":
                return self.parse_md(content)
            elif file_type == "sql":
                if not filename:
                    raise ValueError("filename is required for SQL parsing")
                return self.parse_sql(content, filename)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
