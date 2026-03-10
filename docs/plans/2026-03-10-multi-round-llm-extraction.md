# 分轮次LLM数据源抽取优化方案 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 实现分轮次LLM数据源抽取，解决响应截断问题，同时适配最新的Detail.md第四章节格式要求，提升抽取质量和稳定性。

**Architecture:** 将原有单次LLM请求拆分为3轮独立请求，每轮只抽取数据源的部分章节内容，大幅降低单次请求的token需求量；最终合并多轮结果生成完整的结构化数据源信息，保持原有功能和输出格式不变。

**Tech Stack:** Python, Jinja2, OpenAI SDK, Pydantic

---

### Task 1: 确认最新Detail.md格式要求
**Files:**
- Read: `/Users/huijieqin/project/sql_knowledge_retrieval/项目需求/Detail.md`

**Step 1: 确认最新Detail.md第四章节格式要求**
```python
# 确认第四章节"使用说明和注意事项"的完整结构
# 包含：4.1使用说明、4.2注意事项、4.3关键的查询模式、4.4常用关联表、4.5典型应用场景
```

**Step 2: 确认现有代码已支持所有章节输出**
Run: `pytest tests/test_data_source_manager.py::test_generate_markdown -v`
Expected: PASS

---

### Task 2: 设计分轮次抽取的prompt模板
**Files:**
- Create: `prompts/parse_sql_round1.j2` (抽取章节1+2：基本信息+表结构)
- Create: `prompts/parse_sql_round2.j2` (抽取章节3+4：SQL示例+使用说明)
- Create: `prompts/parse_sql_round3.j2` (抽取章节5+6：数据质量+关联案例)
- Create: `prompts/parse_md_round1.j2` (对应md文件的分轮prompt)
- Create: `prompts/parse_md_round2.j2`
- Create: `prompts/parse_md_round3.j2`

**Step 1: 编写第一轮prompt（基础信息抽取）**
```jinja2
{# prompts/parse_sql_round1.j2 #}
你是专业的SQL数据源分析师，请解析以下SQL文件内容，提取数据源的基础信息和表结构。

文件内容：
{{ content }}
文件名: {{ filename }}

请严格按照以下JSON格式返回结果，不要添加任何额外说明：
{
  "business_domain": "从文件名和内容推断业务域（广告/应用/音乐/公共/全域搜索/电商/其他）",
  "data_sources": [
    {
      "table_name": "完整表名",
      "database": "数据库名",
      "snowflake_layer": "雪花层",
      "partition_field": "分区字段",
      "main_usage": "主要用途",
      "description": "数据源描述",
      "fields": [
        {
          "name": "字段名",
          "description": "字段描述",
          "usage": "用途说明",
          "enum_values": "字段的常用枚举值及含义"
        }
      ]
    }
  ]
}
```

**Step 2: 编写第二轮prompt（SQL和使用说明抽取）**
```jinja2
{# prompts/parse_sql_round2.j2 #}
你是专业的SQL数据源分析师，请解析以下SQL文件内容，提取SQL示例和使用说明相关信息。

文件内容：
{{ content }}
文件名: {{ filename }}

请严格按照以下JSON格式返回结果，不要添加任何额外说明：
{
  "data_sources": [
    {
      "table_name": "完整表名（必须和第一轮返回的表名一致）",
      "sql_examples": [
        {
          "name": "示例名称",
          "sql": "SQL代码",
          "description": "详细的SQL示例说明"
        }
      ],
      "key_query_patterns": [
        "关键查询模式1",
        "关键查询模式2"
      ],
      "common_related_tables": [
        {
          "table_name": "关联表名",
          "join_field": "关联字段",
          "usage": "关联用途说明"
        }
      ],
      "usage_instructions": "使用说明",
      "notes": "注意事项",
      "typical_application_scenarios": [
        "典型应用场景1",
        "典型应用场景2"
      ]
    }
  ]
}
```

**Step 3: 编写第三轮prompt（数据质量和关联案例抽取）**
```jinja2
{# prompts/parse_sql_round3.j2 #}
你是专业的SQL数据源分析师，请解析以下SQL文件内容，提取数据质量和关联案例相关信息。

文件内容：
{{ content }}
文件名: {{ filename }}

请严格按照以下JSON格式返回结果，不要添加任何额外说明：
{
  "data_sources": [
    {
      "table_name": "完整表名（必须和第一轮返回的表名一致）",
      "data_quality": {
        "daily_records": "日记录数",
        "daily_users": "日覆盖用户数",
        "coverage": "数据覆盖情况",
        "timeliness": "上报及时性"
      },
      "related_cases": [
        {
          "name": "{{ filename }}",
          "type": "SQL案例",
          "scenario": "从SQL内容推断使用场景"
        }
      ]
    }
  ]
}
```

**Step 4: 同步编写对应md文件的分轮prompt**
按照相同结构编写parse_md_round1/2/3.j2

**Step 5: Commit**
```bash
git add prompts/*.j2
git commit -m "feat: add multi-round extraction prompt templates"
```

---

### Task 3: 修改Parser类支持多轮抽取和结果合并
**Files:**
- Modify: `/Users/huijieqin/project/sql_knowledge_retrieval/src/parser.py`
- Modify: `/Users/huijieqin/project/sql_knowledge_retrieval/src/main.py`
- Test: `tests/test_parser.py`

**重要约束：** 必须等到所有轮次抽取完成且结果合并完成后，才可以触发数据源更新/创建操作，禁止部分数据中途写入。

**Step 1: 新增多轮抽取方法**
```python
def parse_sql_multi_round(self, content: str, filename: str) -> Dict[str, Any]:
    """分三轮抽取SQL文件信息"""
    # 第一轮：抽取基础信息和表结构
    round1_prompt = self.prompt_manager.get_prompt(
        "parse_sql_round1",
        content=content,
        filename=filename
    )
    round1_response = self.llm_client.chat(round1_prompt)
    round1_data = self._parse_json_safely(round1_response)

    # 第二轮：抽取SQL示例和使用说明
    round2_prompt = self.prompt_manager.get_prompt(
        "parse_sql_round2",
        content=content,
        filename=filename
    )
    round2_response = self.llm_client.chat(round2_prompt)
    round2_data = self._parse_json_safely(round2_response)

    # 第三轮：抽取数据质量和关联案例
    round3_prompt = self.prompt_manager.get_prompt(
        "parse_sql_round3",
        content=content,
        filename=filename
    )
    round3_response = self.llm_client.chat(round3_prompt)
    round3_data = self._parse_json_safely(round3_response)

    # 合并三轮结果
    return self._merge_multi_round_data(round1_data, round2_data, round3_data)
```

**Step 2: 新增结果合并方法**
```python
def _merge_multi_round_data(self, round1: Dict, round2: Dict, round3: Dict) -> Dict[str, Any]:
    """合并多轮抽取结果"""
    merged = round1.copy()

    # 按表名匹配合并
    for i, ds in enumerate(merged.get("data_sources", [])):
        table_name = ds["table_name"]

        # 合并第二轮数据
        for round2_ds in round2.get("data_sources", []):
            if round2_ds["table_name"] == table_name:
                ds.update(round2_ds)
                break

        # 合并第三轮数据
        for round3_ds in round3.get("data_sources", []):
            if round3_ds["table_name"] == table_name:
                ds.update(round3_ds)
                break

    return merged
```

**Step 3: 同样实现parse_md_multi_round方法**

**Step 4: 新增开关配置，支持切换单轮/多轮模式**
在config/settings.py中添加：
```python
use_multi_round_extraction: bool = True
```

**Step 5: 修改parse方法，根据配置自动选择抽取模式**
```python
def parse(self, file_path: Path) -> List[Dict[str, Any]]:
    if file_path.suffix == ".sql":
        content = file_path.read_text(encoding="utf-8")
        if settings.use_multi_round_extraction:
            result = self.parse_sql_multi_round(content, file_path.name)
        else:
            result = self.parse_sql(content, file_path.name)
    elif file_path.suffix == ".md":
        content = file_path.read_text(encoding="utf-8")
        if settings.use_multi_round_extraction:
            result = self.parse_md_multi_round(content)
        else:
            result = self.parse_md(content)
    else:
        raise ValueError(f"Unsupported file type: {file_path.suffix}")

    return result.get("data_sources", [])
```

**Step 6: 运行测试验证多轮抽取逻辑**
Run: `pytest tests/test_parser.py -v`
Expected: PASS

**Step 7: 校验执行顺序确保数据源更新在合并完成后触发**
检查src/main.py中的处理流程：
```python
# 正确执行顺序：
for file in files_to_process:
    # 1. 完成该文件的所有轮次抽取和结果合并，得到完整的数据源列表
    data_sources = parser.parse(file)

    # 2. 所有数据完整后，才执行数据源更新操作
    for ds in data_sources:
        file_path, action, updates = data_source_manager.create_or_update_data_source(
            ds["table_name"], ds["business_domain"], ds
        )
```
确保不会出现部分数据提前写入的情况。

**Step 8: Commit**
```bash
git add src/parser.py config/settings.py src/main.py
git commit -m "feat: implement multi-round LLM extraction and result merging"
```

---

### Task 4: 测试验证优化效果
**Files:**
- Test: `test_single_file.py`

**Step 1: 使用示例SQL文件测试多轮抽取**
Run: `python test_single_file.py 案例/ads_ba_adv_ecommerce_channel_paid_analysis_dm.sql`
Expected:
- 无JSON截断错误
- 生成完整的数据源文件，包含所有章节信息
- 内容质量不低于单轮抽取模式

**Step 2: 对比单轮和多轮抽取的token消耗**
- 检查LLM请求日志，确认单轮请求token量减少60%以上
- 无响应截断现象

**Step 3: 验证合并逻辑正确性**
- 检查生成的markdown文件是否符合最新Detail.md格式要求
- 所有字段信息完整，无丢失

**Step 4: Commit**
```bash
git add test_single_file.py
git commit -m "test: verify multi-round extraction with sample SQL file"
```

---

### Task 5: 回退兼容机制
**Files:**
- Modify: `.env.example`

**Step 1: 在.env.example中添加配置说明**
```env
# 是否使用分轮次抽取模式，解决LLM响应截断问题
USE_MULTI_ROUND_EXTRACTION=true
```

**Step 2: 确保切换到单轮模式仍然正常工作**
Run: `USE_MULTI_ROUND_EXTRACTION=false python test_single_file.py 案例/ads_ba_adv_ecommerce_channel_paid_analysis_dm.sql`
Expected: PASS

**Step 3: Commit**
```bash
git add .env.example
git commit -m "docs: add multi-round extraction configuration documentation"
```
