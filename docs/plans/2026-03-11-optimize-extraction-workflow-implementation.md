# 数据源抽取流程优化 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 实现优化后的数据源抽取流程，包含专用的轻量级表名发现环节和明确指定目标表的分轮抽取逻辑，解决表混淆和token截断问题。

**Architecture:**
- 新增2个专用表名抽取prompt模板，仅返回表名列表，大幅提升表名发现速度
- 更新现有6个分轮抽取prompt，增加target_table_name参数，明确要求仅抽取指定表的信息
- 重写parser.py中的表发现逻辑，替换原有的批次发现方法
- 单表抽取时每轮都传递目标表名，彻底避免表信息混淆

**Tech Stack:** Python 3.9+, Jinja2, OpenAI API, JSON

---

### Task 1: 新增SQL表名抽取prompt模板
**Files:**
- Create: `prompts/extract_table_names_sql.j2`

**Step 1: Create the prompt file**
```jinja2
你是专业的SQL元数据分析师，擅长从SQL文件中提取所有出现的表名。

### 任务：
从提供的SQL内容中提取所有出现的**完整表名**（包含数据库前缀，如biads.table_name）

### 输出要求：
- 严格返回JSON格式，不要任何其他解释文字
- 只输出表名列表，不要其他任何字段
- 去重，不要重复的表名
- 确保JSON格式100%正确

### 输出结构：
```json
{
  "table_names": ["表名1", "表名2", "表名3"]
}
```

### SQL内容：
文件名: {{ filename }}
{{ content }}
```

**Step 2: Verify file created correctly**
Run: `ls prompts/extract_table_names_sql.j2`
Expected: File exists

**Step 3: Commit**
```bash
git add prompts/extract_table_names_sql.j2
git commit -m "feat: add SQL table name extraction prompt"
```

---

### Task 2: 新增MD表名抽取prompt模板
**Files:**
- Create: `prompts/extract_table_names_md.j2`

**Step 1: Create the prompt file**
```jinja2
你是专业的数据分析师，擅长从Markdown文档中提取所有提到的数据表表名。

### 任务：
从提供的Markdown内容中提取所有出现的**完整表名**（包含数据库前缀，如biads.table_name）

### 输出要求：
- 严格返回JSON格式，不要任何其他解释文字
- 只输出表名列表，不要其他任何字段
- 去重，不要重复的表名
- 确保JSON格式100%正确

### 输出结构：
```json
{
  "table_names": ["表名1", "表名2", "表名3"]
}
```

### Markdown内容：
{{ content }}
```

**Step 2: Verify file created correctly**
Run: `ls prompts/extract_table_names_md.j2`
Expected: File exists

**Step 3: Commit**
```bash
git add prompts/extract_table_names_md.j2
git commit -m "feat: add MD table name extraction prompt"
```

---

### Task 3: 更新SQL分轮prompt，增加target_table_name支持
**Files:**
- Modify: `prompts/parse_sql_round1.j2`
- Modify: `prompts/parse_sql_round2.j2`
- Modify: `prompts/parse_sql_round3.j2`

**Step 1: Update parse_sql_round1.j2**
在prompt开头添加以下内容：
```jinja2
{% if target_table_name %}
### 重要提示：
本次仅抽取 **{{ target_table_name }}** 表的相关信息，不要返回其他表的任何内容！
{% endif %}
```

**Step 2: Update parse_sql_round2.j2**
在prompt开头添加相同的提示内容：
```jinja2
{% if target_table_name %}
### 重要提示：
本次仅抽取 **{{ target_table_name }}** 表的相关信息，不要返回其他表的任何内容！
{% endif %}
```

**Step 3: Update parse_sql_round3.j2**
在prompt开头添加相同的提示内容：
```jinja2
{% if target_table_name %}
### 重要提示：
本次仅抽取 **{{ target_table_name }}** 表的相关信息，不要返回其他表的任何内容！
{% endif %}
```

**Step 4: Verify changes**
Run: `grep -n "重要提示" prompts/parse_sql_round*.j2`
Expected: 3 matches, each file has the new prompt section

**Step 5: Commit**
```bash
git add prompts/parse_sql_round*.j2
git commit -m "feat: add target_table_name support to SQL round prompts"
```

---

### Task 4: 更新MD分轮prompt，增加target_table_name支持
**Files:**
- Modify: `prompts/parse_md_round1.j2`
- Modify: `prompts/parse_md_round2.j2`
- Modify: `prompts/parse_md_round3.j2`

**Step 1: Update parse_md_round1.j2**
在prompt开头添加以下内容：
```jinja2
{% if target_table_name %}
### 重要提示：
本次仅抽取 **{{ target_table_name }}** 表的相关信息，不要返回其他表的任何内容！
{% endif %}
```

**Step 2: Update parse_md_round2.j2**
在prompt开头添加相同的提示内容：
```jinja2
{% if target_table_name %}
### 重要提示：
本次仅抽取 **{{ target_table_name }}** 表的相关信息，不要返回其他表的任何内容！
{% endif %}
```

**Step 3: Update parse_md_round3.j2**
在prompt开头添加相同的提示内容：
```jinja2
{% if target_table_name %}
### 重要提示：
本次仅抽取 **{{ target_table_name }}** 表的相关信息，不要返回其他表的任何内容！
{% endif %}
```

**Step 4: Verify changes**
Run: `grep -n "重要提示" prompts/parse_md_round*.j2`
Expected: 3 matches, each file has the new prompt section

**Step 5: Commit**
```bash
git add prompts/parse_md_round*.j2
git commit -m "feat: add target_table_name support to MD round prompts"
```

---

### Task 5: 在parser.py中实现新的表名抽取方法
**Files:**
- Modify: `src/parser.py:158-211` (replace old _discover_all_tables_sql and _discover_all_tables_md methods)

**Step 1: Add new _extract_table_names_sql method**
```python
def _extract_table_names_sql(self, content: str, filename: str) -> list:
    """快速抽取SQL文件中所有表名，使用专用轻量化prompt"""
    for attempt in range(self.max_retries):
        try:
            prompt = self.prompt_manager.get_prompt(
                "extract_table_names_sql",
                content=content,
                filename=filename
            )
            response = self.llm_client.chat(prompt, self.system_prompt)
            result = self._parse_json_safely(response)

            table_names = result.get("table_names", [])
            if not isinstance(table_names, list):
                table_names = []

            # 过滤空值，去重
            table_names = list(set([t.strip() for t in table_names if t.strip()]))
            print(f"Extracted {len(table_names)} tables from SQL file")
            return table_names

        except Exception as e:
            if attempt == self.max_retries - 1:
                raise
            time.sleep(1)
            print(f"SQL table name extraction failed, retrying ({attempt+1}/{self.max_retries}): {e}")
```

**Step 2: Add new _extract_table_names_md method**
```python
def _extract_table_names_md(self, content: str) -> list:
    """快速抽取MD文件中所有表名，使用专用轻量化prompt"""
    for attempt in range(self.max_retries):
        try:
            prompt = self.prompt_manager.get_prompt(
                "extract_table_names_md",
                content=content
            )
            response = self.llm_client.chat(prompt, self.system_prompt)
            result = self._parse_json_safely(response)

            table_names = result.get("table_names", [])
            if not isinstance(table_names, list):
                table_names = []

            # 过滤空值，去重
            table_names = list(set([t.strip() for t in table_names if t.strip()]))
            print(f"Extracted {len(table_names)} tables from MD file")
            return table_names

        except Exception as e:
            if attempt == self.max_retries - 1:
                raise
            time.sleep(1)
            print(f"MD table name extraction failed, retrying ({attempt+1}/{self.max_retries}): {e}")
```

**Step 3: Remove old _discover_all_tables_sql and _discover_all_tables_md methods**
Delete lines 158-211 (old _discover_all_tables_sql) and lines 303-355 (old _discover_all_tables_md)

**Step 4: Verify code syntax**
Run: `python -c "from src.parser import FileParser; print('Import success')"`
Expected: No syntax errors, prints "Import success"

**Step 5: Commit**
```bash
git add src/parser.py
git commit -m "feat: add lightweight table name extraction methods"
```

---

### Task 6: 更新单表抽取方法，传递target_table_name参数
**Files:**
- Modify: `src/parser.py:213-267` (_extract_single_table_sql)
- Modify: `src/parser.py:357-410` (_extract_single_table_md)

**Step 1: Update _extract_single_table_sql method**
Modify each round's prompt call to add target_table_name parameter:
```python
# 第一轮：基础信息抽取
prompt = self.prompt_manager.get_prompt(
    "parse_sql_round1",
    content=content,
    filename=filename,
    extracted_tables=[],
    max_tables=1,
    target_table_name=table_name  # 新增参数
)

# 第二轮：深度信息抽取
prompt = self.prompt_manager.get_prompt(
    "parse_sql_round2",
    content=content,
    filename=filename,
    target_table_name=table_name  # 新增参数
)

# 第三轮：补充验证抽取
prompt = self.prompt_manager.get_prompt(
    "parse_sql_round3",
    content=content,
    filename=filename,
    target_table_name=table_name  # 新增参数
)
```

**Step 2: Update _extract_single_table_md method**
Modify each round's prompt call to add target_table_name parameter:
```python
# 第一轮：基础信息抽取
prompt = self.prompt_manager.get_prompt(
    "parse_md_round1",
    content=content,
    extracted_tables=[],
    max_tables=1,
    target_table_name=table_name  # 新增参数
)

# 第二轮：深度信息抽取
prompt = self.prompt_manager.get_prompt(
    "parse_md_round2",
    content=content,
    target_table_name=table_name  # 新增参数
)

# 第三轮：补充验证抽取
prompt = self.prompt_manager.get_prompt(
    "parse_md_round3",
    content=content,
    target_table_name=table_name  # 新增参数
)
```

**Step 3: Verify code syntax**
Run: `python -c "from src.parser import FileParser; print('Import success')"`
Expected: No syntax errors

**Step 4: Commit**
```bash
git add src/parser.py
git commit -m "feat: pass target_table_name to per-table extraction rounds"
```

---

### Task 7: 更新主抽取流程，使用新的表名发现逻辑
**Files:**
- Modify: `src/parser.py:269-301` (parse_sql_multi_round)
- Modify: `src/parser.py:412-444` (parse_md_multi_round)

**Step 1: Update parse_sql_multi_round method**
Replace the table discovery part:
```python
# 第一阶段：快速抽取所有表名
print("Starting SQL table name extraction...")
all_tables = self._extract_table_names_sql(content, filename)
if not all_tables:
    print("No tables found in SQL file")
    return {}

print(f"Discovered {len(all_tables)} tables: {', '.join(all_tables)}")
```

**Step 2: Update parse_md_multi_round method**
Replace the table discovery part:
```python
# 第一阶段：快速抽取所有表名
print("Starting MD table name extraction...")
all_tables = self._extract_table_names_md(content)
if not all_tables:
    print("No tables found in MD file")
    return {}

print(f"Discovered {len(all_tables)} tables: {', '.join(all_tables)}")
```

**Step 3: Verify code syntax**
Run: `python -c "from src.parser import FileParser; print('Import success')"`
Expected: No syntax errors

**Step 4: Commit**
```bash
git add src/parser.py
git commit -m "feat: update main extraction flow to use new table discovery logic"
```

---

### Task 8: 测试新流程
**Files:**
- Test: `test_single_file.py`

**Step 1: Run test with sample SQL file**
Run: `python test_single_file.py 案例/电商行业案例/ads_ba_adv_ecommerce_channel_paid_analysis_dm.sql`
Expected:
- Extracts table names successfully
- Processes each table with target_table_name parameter
- No token truncation errors
- Output contains complete information for all tables

**Step 2: Run test with sample MD file**
Run: `python test_single_file.py 案例/电商行业案例/电商广告分析指南.md`
Expected: Same as above, works for MD files

**Step 3: Commit test results (if changes needed)**
```bash
# Only if test files need updates
git add test_single_file.py
git commit -m "test: update test file for new extraction flow"
```

---

## 执行选项
Plan complete and saved to `docs/plans/2026-03-11-optimize-extraction-workflow-implementation.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration
**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
