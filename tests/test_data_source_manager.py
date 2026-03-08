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
