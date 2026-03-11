# Prompt优化设计方案

## 1. 优化目标
全面优化数据源抽取prompt，提高LLM信息抽取的准确率和格式稳定性，降低错误率。

## 2. 优化内容
### 2.1 通用优化项（应用到所有prompt模板）
1. **增加系统角色定义**：明确模型的角色是专业的数据仓库分析师，擅长从SQL/MD文档中精准提取元数据信息
2. **增加约束性规则**：明确禁止项，避免幻觉和格式错误
3. **明确字段定义**：对每个输出字段给出明确的定义说明，消除歧义
4. **增加Few-Shot示例**：在每个prompt中加入1个高质量的抽取示例，引导模型输出正确格式
5. **格式严格性要求**：明确要求输出完整的JSON，禁止截断，无额外解释文字

### 2.2 各轮prompt专项优化
#### Round1（基础信息+表结构抽取）
- 明确`business_domain`的可选枚举值：广告/应用/音乐/公共/全域搜索/电商/其他
- 明确`snowflake_layer`的识别规则：ADS（应用数据层）/DWD（明细数据层）/DWS（汇总数据层）/DIM（维度层）/ODS（原始数据层）
- 明确`partition_field`的提取规则：从SQL的WHERE条件中提取，常见如pt_d、dt、date等
- 明确`fields.enum_values`的提取规则：仅提取SQL中明确出现的枚举值，如`event_type IN ('exposure', 'click')`，没有则留空

#### Round2（SQL示例+使用说明抽取）
- 明确`typical_application_scenarios`的定义：从业务使用角度描述，如"电商广告投放效果分析"、"用户行为路径分析"等
- 明确`key_query_patterns`的定义：描述常用的查询模式，如"按日期分区过滤"、"按事件类型分组统计"等

#### Round3（数据质量+关联案例抽取）
- 明确`data_quality`各字段的提取规则：没有信息则留空，禁止编造
- 明确`related_cases`的提取规则：从案例文件名和SQL内容推断使用场景

## 3. 实施步骤
1. 优化所有6个分轮抽取prompt模板（parse_sql_round1/2/3.j2、parse_md_round1/2/3.j2）
2. 同步优化2个单轮抽取prompt模板（parse_sql.j2、parse_md.j2）保持一致性
3. 优化merge_data_source.j2模板
4. 测试优化后的抽取效果，验证准确率提升
