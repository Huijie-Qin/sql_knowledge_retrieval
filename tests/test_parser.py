import json
from unittest.mock import patch, MagicMock
from src.parser import FileParser

def test_parse_md_file():
    # Mock LLMClient before creating FileParser instance
    with patch('src.parser.LLMClient') as mock_llm_class:
        # Setup mock LLM client
        mock_llm = MagicMock()
        mock_response = json.dumps({
            "data_sources": [
                {
                    "table_name": "dwd_user_behavior_d",
                    "description": "用户行为表，包含用户的浏览、点击、购买等行为数据",
                    "fields": [
                        {"name": "did", "description": "设备id"},
                        {"name": "event_type", "description": "事件类型"},
                        {"name": "event_time", "description": "事件时间"}
                    ]
                }
            ]
        })
        mock_llm.chat.return_value = mock_response
        mock_llm_class.return_value = mock_llm

        # Mock PromptManager
        with patch('src.parser.PromptManager') as mock_prompt_class:
            mock_prompt = MagicMock()
            mock_prompt.get_prompt.return_value = "Test prompt"
            mock_prompt_class.return_value = mock_prompt

            parser = FileParser()
            test_content = """
            # 电商分析案例
            用户行为表dwd_user_behavior_d包含用户的浏览、点击、购买等行为数据。
            字段包括did(设备id)，event_type(事件类型)，event_time(事件时间)。
            """
            result = parser.parse_md(test_content)
            assert "data_sources" in result
            assert len(result["data_sources"]) > 0
            assert result["data_sources"][0]["table_name"] == "dwd_user_behavior_d"
            assert len(result["data_sources"][0]["fields"]) == 3

def test_parse_sql_file():
    # Mock LLMClient before creating FileParser instance
    with patch('src.parser.LLMClient') as mock_llm_class:
        # Setup mock LLM client
        mock_llm = MagicMock()
        mock_response = json.dumps({
            "data_sources": [
                {
                    "table_name": "dwd.dwd_user_behavior_d",
                    "description": "用户行为表",
                    "fields": [
                        {"name": "did", "description": "设备id"},
                        {"name": "event_type", "description": "事件类型"}
                    ]
                }
            ],
            "analysis_logic": "统计每个用户的购买次数",
            "business_scenario": "电商用户购买分析"
        })
        mock_llm.chat.return_value = mock_response
        mock_llm_class.return_value = mock_llm

        # Mock PromptManager
        with patch('src.parser.PromptManager') as mock_prompt_class:
            mock_prompt = MagicMock()
            mock_prompt.get_prompt.return_value = "Test prompt"
            mock_prompt_class.return_value = mock_prompt

            parser = FileParser()
            test_content = """
            -- 电商用户购买分析
            SELECT did, count(*) as buy_cnt
            FROM dwd.dwd_user_behavior_d
            WHERE event_type = 'buy'
            GROUP BY did
            """
            result = parser.parse_sql(test_content, "analysis_ecommerce_buy.sql")
            assert "data_sources" in result
            assert len(result["data_sources"]) > 0
            assert result["data_sources"][0]["table_name"] == "dwd.dwd_user_behavior_d"
            assert "analysis_logic" in result
            assert "business_scenario" in result

def test_parse_md_with_json_markdown():
    # Mock LLMClient before creating FileParser instance
    with patch('src.parser.LLMClient') as mock_llm_class:
        # Setup mock LLM client with markdown wrapped JSON
        mock_llm = MagicMock()
        mock_response = """```json
        {
            "data_sources": [
                {
                    "table_name": "test_table",
                    "description": "测试数据表",
                    "fields": []
                }
            ]
        }
        ```"""
        mock_llm.chat.return_value = mock_response
        mock_llm_class.return_value = mock_llm

        # Mock PromptManager
        with patch('src.parser.PromptManager') as mock_prompt_class:
            mock_prompt = MagicMock()
            mock_prompt.get_prompt.return_value = "Test prompt"
            mock_prompt_class.return_value = mock_prompt

            parser = FileParser()
            test_content = """
            # 测试内容
            表test_table包含测试数据。
            """
            result = parser.parse_md(test_content)
            assert "data_sources" in result
            assert result["data_sources"][0]["table_name"] == "test_table"
