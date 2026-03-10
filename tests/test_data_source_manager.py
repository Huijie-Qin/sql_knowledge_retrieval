from src.data_source_manager import DataSourceManager
from pathlib import Path
import pytest
from unittest.mock import Mock, patch, MagicMock

@patch('src.data_source_manager.LLMClient')
@patch('src.data_source_manager.PromptManager')
@patch('src.data_source_manager.settings')
def test_create_data_source(mock_settings, mock_prompt_manager, mock_llm_client):
    # Mock settings
    mock_settings.output_dir = "/tmp/test_output"

    manager = DataSourceManager()
    source_data = {
        "table_name": "test.dwd_test_table",
        "description": "测试表",
        "business_domain": "测试",
        "fields": [{"name": "id", "description": "主键", "usage": "唯一标识"}]
    }
    file_path = manager.create_data_source(source_data, "测试")
    assert file_path.exists()
    assert file_path.read_text().find("test.dwd_test_table") > -1
    # Clean up
    file_path.unlink()
    file_path.parent.rmdir()
    Path(mock_settings.output_dir).rmdir()

@patch('src.data_source_manager.LLMClient')
@patch('src.data_source_manager.PromptManager')
@patch('src.data_source_manager.settings')
def test_merge_data_source(mock_settings, mock_prompt_manager, mock_llm_client):
    # Mock settings
    mock_settings.output_dir = "/tmp/test_output"

    # Mock LLM client response
    mock_llm_instance = Mock()
    mock_llm_instance.chat.return_value = """
    # test.dwd_test_table
    ## 1.数据源基本信息
    ### 1.1.数据源名称
    测试表
    ### 1.2.数据源描述
    更新后的测试表描述

    ## 2.数据表结构
    ### 2.1.表名
    test.dwd_test_table
    ### 2.2.关键字段
    | 字段名|字段描述 | 用途说明|
    |----------|----------|----------|
    |name|名称|用户名称|
    """
    mock_llm_client.return_value = mock_llm_instance

    # Mock prompt manager
    mock_prompt_instance = Mock()
    mock_prompt_instance.get_prompt.return_value = "merge prompt"
    mock_prompt_manager.return_value = mock_prompt_instance

    manager = DataSourceManager()
    old_content = """
    # test.dwd_test_table
    ## 1.数据源基本信息
    ### 1.1.数据源名称
    测试表
    ### 1.2.数据源描述
    测试表描述
    """
    new_data = {
        "table_name": "test.dwd_test_table",
        "description": "更新后的测试表描述",
        "fields": [{"name": "name", "description": "名称", "usage": "用户名称"}]
    }

    merged_content, update_points = manager.merge_data_source(old_content, new_data)
    assert "更新后的测试表描述" in merged_content
    assert "name" in merged_content
    assert len(update_points) > 0
    assert "修正描述信息" in update_points
    assert "补充字段说明" in update_points

@patch('src.data_source_manager.LLMClient')
@patch('src.data_source_manager.PromptManager')
@patch('src.data_source_manager.settings')
def test_detect_update_points(mock_settings, mock_prompt_manager, mock_llm_client):
    # Mock settings
    mock_settings.output_dir = "/tmp/test_output"

    manager = DataSourceManager()
    old_content = """
    # test.dwd_test_table
    ## 1.数据源基本信息
    ### 1.1.数据源名称
    测试表
    ### 1.2.数据源描述
    测试表描述
    """

    # Test with added fields
    new_content_with_fields = old_content + """
    ## 2.数据表结构
    ### 2.2.关键字段
    | 字段名|字段描述 | 用途说明|
    |----------|----------|----------|
    |id|主键|唯一标识|
    """
    update_points = manager._detect_update_points(old_content, new_content_with_fields)
    assert "补充字段说明" in update_points

    # Test with added SQL example
    new_content_with_sql = old_content + """
    ## 3.SQL使用示例
    ### 3.1.查询示例
    ```sql
    SELECT * FROM test.dwd_test_table LIMIT 100
    ```
    """
    update_points = manager._detect_update_points(old_content, new_content_with_sql)
    assert "新增SQL示例" in update_points

    # Test with data quality update
    new_content_with_quality = old_content + """
    ## 5.数据质量说明
    ### 5.1.数据量
    - 日记录数：100万
    - 日覆盖用户数：50万
    """
    update_points = manager._detect_update_points(old_content, new_content_with_quality)
    assert "更新数据质量信息" in update_points

@patch('src.data_source_manager.LLMClient')
@patch('src.data_source_manager.PromptManager')
@patch('src.data_source_manager.settings')
def test_exists(mock_settings, mock_prompt_manager, mock_llm_client):
    # Mock settings
    mock_settings.output_dir = "/tmp/test_output"

    manager = DataSourceManager()
    # First create a test file
    source_data = {
        "table_name": "test.dwd_test_exists",
        "description": "测试存在性检查",
        "business_domain": "测试",
    }
    file_path = manager.create_data_source(source_data, "测试")

    # Test exists returns True
    assert manager.exists("test.dwd_test_exists", "测试") is True

    # Test exists returns False for non-existent table
    assert manager.exists("test.dwd_non_existent", "测试") is False

    # Clean up
    file_path.unlink()
    file_path.parent.rmdir()
    Path(mock_settings.output_dir).rmdir()

@patch('src.data_source_manager.LLMClient')
@patch('src.data_source_manager.PromptManager')
@patch('src.data_source_manager.settings')
def test_update_data_source(mock_settings, mock_prompt_manager, mock_llm_client):
    # Mock settings
    mock_settings.output_dir = "/tmp/test_output"

    # Mock LLM client response
    mock_llm_instance = Mock()
    mock_llm_instance.chat.return_value = """
    # test.dwd_test_update
    ## 1.数据源基本信息
    ### 1.1.数据源名称
    test.dwd_test_update
    ### 1.2.数据源描述
    更新后的描述
    ### 1.3.业务域
    测试

    ## 2.数据表结构
    ### 2.1.表名
    test.dwd_test_update
    ### 2.2.关键字段
    | 字段名|字段描述 | 用途说明|
    |----------|----------|----------|
    |id|主键|唯一标识|
    |name|名称|用户名称|

    ## 3.SQL使用示例
    ### 3.1.查询所有记录
    ```sql
    SELECT * FROM test.dwd_test_update WHERE id = 1
    ```
    """
    mock_llm_client.return_value = mock_llm_instance

    # Mock prompt manager
    mock_prompt_instance = Mock()
    mock_prompt_instance.get_prompt.return_value = "merge prompt"
    mock_prompt_manager.return_value = mock_prompt_instance

    manager = DataSourceManager()
    # Create initial data source
    initial_data = {
        "table_name": "test.dwd_test_update",
        "description": "初始描述",
        "business_domain": "测试",
        "fields": [{"name": "id", "description": "主键", "usage": "唯一标识"}]
    }
    file_path = manager.create_data_source(initial_data, "测试")

    # Prepare update data
    update_data = {
        "table_name": "test.dwd_test_update",
        "description": "更新后的描述",
        "fields": [
            {"name": "id", "description": "主键", "usage": "唯一标识"},
            {"name": "name", "description": "名称", "usage": "用户名称"}
        ],
        "sql_examples": [
            {"name": "查询所有记录", "sql": "SELECT * FROM test.dwd_test_update WHERE id = 1"}
        ]
    }

    updated_path, update_points = manager.update_data_source("test.dwd_test_update", "测试", update_data)
    assert updated_path == file_path
    assert len(update_points) > 0
    assert "修正描述信息" in update_points
    assert "补充字段说明" in update_points
    assert "新增SQL示例" in update_points

    # Verify the file was updated
    content = updated_path.read_text()
    assert "更新后的描述" in content
    assert "name" in content
    assert "查询所有记录" in content

    # Clean up
    file_path.unlink()
    file_path.parent.rmdir()
    Path(mock_settings.output_dir).rmdir()


@patch('src.data_source_manager.LLMClient')
@patch('src.data_source_manager.PromptManager')
@patch('src.data_source_manager.settings')
def test_generate_markdown(mock_settings, mock_prompt_manager, mock_llm_client):
    # Mock settings
    mock_settings.output_dir = "/tmp/test_output"

    manager = DataSourceManager()
    test_data = {
        "table_name": "test.dwd_complete_table",
        "name": "完整测试表",
        "description": "用于测试完整markdown生成的测试表",
        "business_domain": "测试",
        "fields": [
            {"name": "id", "description": "主键", "usage": "唯一标识", "enum_values": ""},
            {"name": "user_id", "description": "用户ID", "usage": "关联用户表", "enum_values": ""},
            {"name": "event_type", "description": "事件类型", "usage": "区分不同业务事件", "enum_values": "click:点击, view:浏览, purchase:购买"}
        ],
        "sql_examples": [
            {
                "name": "查询用户活跃事件",
                "description": "统计用户每天的活跃事件数",
                "sql": "SELECT user_id, date, COUNT(*) as event_count FROM test.dwd_complete_table WHERE event_type = 'click' GROUP BY user_id, date"
            }
        ],
        "usage_instructions": "该表主要用于用户行为分析，建议按日期分区查询",
        "notes": "数据延迟为T+1，凌晨3点完成前一天的数据同步",
        "key_query_patterns": [
            "按用户ID分组统计行为次数",
            "按事件类型过滤查询特定行为",
            "按日期范围统计趋势"
        ],
        "common_related_tables": [
            {"table_name": "test.dim_user", "join_field": "user_id", "usage": "关联用户属性信息"},
            {"table_name": "test.dwd_user_payment", "join_field": "user_id", "usage": "关联支付数据进行转化分析"}
        ],
        "typical_application_scenarios": [
            "用户活跃度分析",
            "用户行为路径分析",
            "营销活动效果评估",
            "产品功能使用率统计"
        ],
        "data_quality": {
            "daily_records": "1000万",
            "daily_users": "500万",
            "coverage": "覆盖95%的活跃用户行为",
            "timeliness": "T+1更新，每日凌晨3点完成"
        },
        "related_cases": [
            {"name": "用户活跃度日报", "type": "分析报表", "scenario": "每日计算用户活跃指标"},
            {"name": "营销活动效果分析", "type": "专项分析", "scenario": "评估营销活动带来的用户行为变化"}
        ]
    }

    # Generate markdown
    content = manager._generate_markdown(test_data)

    # Verify all sections are present
    assert "# test.dwd_complete_table" in content

    # Chapter 1: 数据源基本信息
    assert "## 1.数据源基本信息" in content
    assert "### 1.1.数据源名称" in content
    assert "完整测试表" in content
    assert "### 1.2.数据源描述" in content
    assert "用于测试完整markdown生成的测试表" in content
    assert "### 1.3.业务域" in content
    assert "测试" in content

    # Chapter 2: 数据表结构
    assert "## 2.数据表结构" in content
    assert "### 2.1.表名" in content
    assert "test.dwd_complete_table" in content
    assert "### 2.2.关键字段" in content
    assert "|id|主键|唯一标识||" in content
    assert "|user_id|用户ID|关联用户表||" in content
    assert "|event_type|事件类型|区分不同业务事件|click:点击, view:浏览, purchase:购买|" in content

    # Chapter 3: SQL使用示例
    assert "## 3.SQL使用示例" in content
    assert "### 3.1.查询用户活跃事件" in content
    assert "统计用户每天的活跃事件数" in content
    assert "```sql" in content
    assert "SELECT user_id, date, COUNT(*) as event_count FROM test.dwd_complete_table WHERE event_type = 'click' GROUP BY user_id, date" in content

    # Chapter 4: 使用说明和注意事项
    assert "## 4.使用说明和注意事项" in content
    assert "### 4.1.使用说明" in content
    assert "该表主要用于用户行为分析，建议按日期分区查询" in content
    assert "### 4.2.注意事项" in content
    assert "数据延迟为T+1，凌晨3点完成前一天的数据同步" in content
    assert "### 4.3.关键的查询模式" in content
    assert "- 按用户ID分组统计行为次数" in content
    assert "- 按事件类型过滤查询特定行为" in content
    assert "- 按日期范围统计趋势" in content
    assert "### 4.4.常用关联表" in content
    assert "|test.dim_user|user_id|关联用户属性信息|" in content
    assert "|test.dwd_user_payment|user_id|关联支付数据进行转化分析|" in content
    assert "### 4.5.典型应用场景" in content
    assert "- 用户活跃度分析" in content
    assert "- 用户行为路径分析" in content
    assert "- 营销活动效果评估" in content
    assert "- 产品功能使用率统计" in content

    # Chapter 5: 数据质量说明
    assert "## 5.数据质量说明" in content
    assert "### 5.1.数据量" in content
    assert "- 日记录数：1000万" in content
    assert "- 日覆盖用户数：500万" in content
    assert "### 5.2.数据覆盖情况" in content
    assert "覆盖95%的活跃用户行为" in content
    assert "### 5.3.上报及时性" in content
    assert "T+1更新，每日凌晨3点完成" in content

    # Chapter 6: 关联案例
    assert "## 6.关联案例" in content
    assert "|用户活跃度日报|分析报表|每日计算用户活跃指标|" in content
    assert "|营销活动效果分析|专项分析|评估营销活动带来的用户行为变化|" in content
