# 数据源抽取流程优化设计方案

## 优化目标
1. 解决当前流程中`_extract_single_table_sql`方法没有传递table_name到prompt的问题，避免表信息混淆
2. 提升表名发现速度，使用专用轻量化prompt仅抽取表名，不返回额外字段
3. 彻底解决多表抽取时的token截断问题
4. 提升整体抽取准确率

## 架构设计
### 新流程
```
读取文件内容 → 调用专用表名抽取prompt → 得到所有表名列表 → 遍历每个表名：
    → 第一轮抽取：传入target_table_name，仅抽取该表的基础信息+表结构
    → 第二轮抽取：传入target_table_name，仅抽取该表的SQL示例+使用说明
    → 第三轮抽取：传入target_table_name，仅抽取该表的数据质量+关联案例
→ 合并所有表结果 → 输出最终数据
```

## 实施内容
### 1. 新增prompt模板
- `prompts/extract_table_names_sql.j2`：SQL文件专用表名抽取prompt，仅返回表名列表
- `prompts/extract_table_names_md.j2`：Markdown文件专用表名抽取prompt，仅返回表名列表
- 更新现有6个分轮抽取prompt（parse_sql_round1-3.j2、parse_md_round1-3.j2），增加`target_table_name`参数支持，明确要求仅抽取指定表的信息

### 2. 代码修改
- 新增`_extract_table_names_sql()`方法：调用专用prompt抽取SQL文件所有表名
- 新增`_extract_table_names_md()`方法：调用专用prompt抽取MD文件所有表名
- 移除原有的`_discover_all_tables_sql()`和`_discover_all_tables_md()`批次发现方法
- 修改`_extract_single_table_sql()`和`_extract_single_table_md()`方法，每轮抽取都传入`target_table_name`参数
- 修改`parse_sql_multi_round()`和`parse_md_multi_round()`主流程，使用新的表名发现逻辑

### 3. 兼容处理
- 保留原有合并逻辑`_merge_multi_round_data()`不变
- 保留对外API接口不变，不影响上层调用
- 配置开关保持兼容，仍可选择单轮/多轮抽取模式

## 预期效果
- 表名抽取速度提升10倍以上，输出token消耗减少90%
- 彻底解决多表抽取时的信息混淆问题
- 完全避免token截断问题，抽取完整性100%
- 抽取准确率提升，字段遗漏率降低

## 下一步
1. 按照本设计编写具体的实现计划
2. 逐步实施代码修改
3. 测试验证优化效果
