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


def test_parse_sql_multi_round():
    """测试SQL文件多轮抽取正常流程"""
    with patch('src.parser.LLMClient') as mock_llm_class:
        mock_llm = MagicMock()

        # 模拟三轮不同的返回结果
        round1_response = json.dumps({
            "business_domain": "电商",
            "data_sources": [
                {
                    "table_name": "dwd.dwd_user_behavior_d",
                    "name": "用户行为表",
                    "fields": [
                        {"name": "did", "description": "设备id"},
                        {"name": "event_type", "description": "事件类型"}
                    ]
                }
            ]
        })

        round2_response = json.dumps({
            "business_domain": "电商",
            "data_sources": [
                {
                    "table_name": "dwd.dwd_user_behavior_d",
                    "description": "用户行为表，包含所有用户端行为数据",
                    "fields": [
                        {"name": "did", "description": "设备唯一标识", "usage": "主键"},
                        {"name": "event_time", "description": "事件发生时间"}
                    ],
                    "sql_examples": [
                        {"name": "查询日活", "sql": "SELECT COUNT(DISTINCT did) FROM dwd.dwd_user_behavior_d WHERE dt = '2024-01-01'"}
                    ]
                }
            ]
        })

        round3_response = json.dumps({
            "business_domain": "电商",
            "data_sources": [
                {
                    "table_name": "dwd.dwd_user_behavior_d",
                    "fields": [
                        {"name": "event_type", "description": "事件类型", "enum_values": "浏览:view,点击:click,购买:buy"},
                        {"name": "page_id", "description": "页面ID"}
                    ],
                    "notes": "数据延迟约1小时，按dt分区",
                    "common_related_tables": [
                        {"table_name": "dim.dim_user_info", "join_field": "did", "usage": "关联用户属性"}
                    ]
                }
            ]
        })

        # 按调用顺序返回三轮结果
        mock_llm.chat.side_effect = [round1_response, round2_response, round3_response]
        mock_llm_class.return_value = mock_llm

        with patch('src.parser.PromptManager') as mock_prompt_class:
            mock_prompt = MagicMock()
            mock_prompt.get_prompt.return_value = "Test prompt"
            mock_prompt_class.return_value = mock_prompt

            parser = FileParser()
            test_content = """
            SELECT did, event_type, event_time
            FROM dwd.dwd_user_behavior_d
            WHERE dt = '2024-01-01'
            """
            result = parser.parse_sql_multi_round(test_content, "test.sql")

            # 验证合并结果
            assert "data_sources" in result
            ds = result["data_sources"][0]
            assert ds["table_name"] == "dwd.dwd_user_behavior_d"
            assert ds["name"] == "用户行为表"
            assert ds["description"] == "用户行为表，包含所有用户端行为数据"
            assert len(ds["fields"]) == 4  # did, event_type, event_time, page_id
            assert ds["notes"] == "数据延迟约1小时，按dt分区"
            assert len(ds["sql_examples"]) == 1
            assert len(ds["common_related_tables"]) == 1

            # 验证字段合并正确
            fields = {f["name"]: f for f in ds["fields"]}
            assert fields["did"]["description"] == "设备唯一标识"
            assert fields["did"]["usage"] == "主键"
            assert fields["event_type"]["enum_values"] == "浏览:view,点击:click,购买:buy"


def test_parse_md_multi_round():
    """测试MD文件多轮抽取正常流程"""
    with patch('src.parser.LLMClient') as mock_llm_class:
        mock_llm = MagicMock()

        # 模拟三轮不同的返回结果
        round1_response = json.dumps({
            "business_domain": "金融",
            "data_sources": [
                {
                    "table_name": "dwd.dwd_trade_order_d",
                    "name": "交易订单表",
                    "fields": [
                        {"name": "order_id", "description": "订单ID"},
                        {"name": "user_id", "description": "用户ID"}
                    ]
                }
            ]
        })

        round2_response = json.dumps({
            "business_domain": "金融",
            "data_sources": [
                {
                    "table_name": "dwd.dwd_trade_order_d",
                    "description": "记录所有交易订单的主表",
                    "fields": [
                        {"name": "order_id", "description": "订单唯一标识", "usage": "主键"},
                        {"name": "order_amount", "description": "订单金额"}
                    ],
                    "sql_examples": [
                        {"name": "统计日交易额", "sql": "SELECT SUM(order_amount) FROM dwd.dwd_trade_order_d WHERE dt = '2024-01-01'"}
                    ]
                }
            ]
        })

        round3_response = json.dumps({
            "business_domain": "金融",
            "data_sources": [
                {
                    "table_name": "dwd.dwd_trade_order_d",
                    "fields": [
                        {"name": "user_id", "description": "下单用户ID", "usage": "关联用户表"},
                        {"name": "order_status", "description": "订单状态"}
                    ],
                    "notes": "订单状态：1-待支付，2-已支付，3-已取消",
                    "typical_application_scenarios": ["交易报表", "用户消费分析"]
                }
            ]
        })

        # 按调用顺序返回三轮结果
        mock_llm.chat.side_effect = [round1_response, round2_response, round3_response]
        mock_llm_class.return_value = mock_llm

        with patch('src.parser.PromptManager') as mock_prompt_class:
            mock_prompt = MagicMock()
            mock_prompt.get_prompt.return_value = "Test prompt"
            mock_prompt_class.return_value = mock_prompt

            parser = FileParser()
            test_content = """
            # 交易分析案例
            订单表dwd.dwd_trade_order_d存储了所有交易数据，包含订单ID、用户ID、金额等字段。
            常用于交易报表和用户消费分析。
            """
            result = parser.parse_md_multi_round(test_content)

            # 验证合并结果
            assert "data_sources" in result
            ds = result["data_sources"][0]
            assert ds["table_name"] == "dwd.dwd_trade_order_d"
            assert ds["name"] == "交易订单表"
            assert ds["description"] == "记录所有交易订单的主表"
            assert len(ds["fields"]) == 4  # order_id, user_id, order_amount, order_status
            assert ds["notes"] == "订单状态：1-待支付，2-已支付，3-已取消"
            assert len(ds["sql_examples"]) == 1
            assert len(ds["typical_application_scenarios"]) == 2


def test_merge_multi_round_data():
    """测试合并策略，包括数组去重、字符串取最长、嵌套对象合并等"""
    parser = FileParser()

    # 测试1：数组去重（字段合并）
    rounds1 = [
        {"fields": [{"name": "id", "description": "ID"}, {"name": "name", "description": "姓名"}]},
        {"fields": [{"name": "id", "description": "唯一标识", "usage": "主键"}, {"name": "age", "description": "年龄"}]}
    ]
    result1 = parser._merge_multi_round_data(rounds1)
    assert len(result1["fields"]) == 3
    fields1 = {f["name"]: f for f in result1["fields"]}
    assert fields1["id"]["description"] == "唯一标识"
    assert fields1["id"]["usage"] == "主键"

    # 测试2：字符串取最长
    rounds2 = [
        {"description": "用户表"},
        {"description": "用户信息主表"},
        {"description": "存储系统所有用户的基本信息表"}
    ]
    result2 = parser._merge_multi_round_data(rounds2)
    assert result2["description"] == "存储系统所有用户的基本信息表"

    # 测试3：嵌套对象合并
    rounds3 = [
        {"data_quality": {"daily_records": "100万", "coverage": "全量用户"}},
        {"data_quality": {"daily_users": "50万", "timeliness": "T+1"}}
    ]
    result3 = parser._merge_multi_round_data(rounds3)
    assert result3["data_quality"]["daily_records"] == "100万"
    assert result3["data_quality"]["daily_users"] == "50万"
    assert result3["data_quality"]["coverage"] == "全量用户"
    assert result3["data_quality"]["timeliness"] == "T+1"

    # 测试4：数据源数组按table_name合并
    rounds4 = [
        {"data_sources": [{"table_name": "user", "name": "用户表"}]},
        {"data_sources": [{"table_name": "user", "description": "用户信息表"}, {"table_name": "order", "name": "订单表"}]}
    ]
    result4 = parser._merge_multi_round_data(rounds4)
    assert len(result4["data_sources"]) == 2
    tables = {ds["table_name"]: ds for ds in result4["data_sources"]}
    assert tables["user"]["name"] == "用户表"
    assert tables["user"]["description"] == "用户信息表"
    assert tables["order"]["name"] == "订单表"

    # 测试5：普通数组去重
    rounds5 = [
        {"tags": ["用户", "基础数据"]},
        {"tags": ["基础数据", "核心表"]},
        {"tags": ["用户", "交易"]}
    ]
    result5 = parser._merge_multi_round_data(rounds5)
    assert sorted(result5["tags"]) == ["交易", "基础数据", "核心表", "用户"]

    # 测试6：数值类型取最后一轮
    rounds6 = [
        {"version": 1},
        {"version": 2},
        {"version": 3}
    ]
    result6 = parser._merge_multi_round_data(rounds6)
    assert result6["version"] == 3


def test_parse_multi_round_config():
    """测试use_multi_round_extraction配置切换功能"""
    from config.settings import settings

    # 备份原始配置
    original_config = settings.use_multi_round_extraction

    try:
        # 测试多轮模式
        settings.use_multi_round_extraction = True
        with patch('src.parser.LLMClient') as mock_llm_class:
            mock_llm = MagicMock()
            mock_llm.chat.return_value = json.dumps({"data_sources": []})
            mock_llm_class.return_value = mock_llm

            with patch('src.parser.PromptManager') as mock_prompt_class:
                mock_prompt = MagicMock()
                mock_prompt.get_prompt.return_value = "Test prompt"
                mock_prompt_class.return_value = mock_prompt

                parser = FileParser()

                # 测试SQL多轮模式 - 应该调用3次chat
                with patch.object(parser, 'parse_sql_multi_round') as mock_multi_sql:
                    mock_multi_sql.return_value = {"data_sources": []}
                    parser.parse("SELECT * FROM test", "sql", "test.sql")
                    mock_multi_sql.assert_called_once_with("SELECT * FROM test", "test.sql")

                # 测试MD多轮模式 - 应该调用3次chat
                with patch.object(parser, 'parse_md_multi_round') as mock_multi_md:
                    mock_multi_md.return_value = {"data_sources": []}
                    parser.parse("# Test", "md")
                    mock_multi_md.assert_called_once_with("# Test")

        # 测试单轮模式
        settings.use_multi_round_extraction = False
        with patch('src.parser.LLMClient') as mock_llm_class:
            mock_llm = MagicMock()
            mock_llm.chat.return_value = json.dumps({"data_sources": []})
            mock_llm_class.return_value = mock_llm

            with patch('src.parser.PromptManager') as mock_prompt_class:
                mock_prompt = MagicMock()
                mock_prompt.get_prompt.return_value = "Test prompt"
                mock_prompt_class.return_value = mock_prompt

                parser = FileParser()

                # 测试SQL单轮模式 - 应该调用1次chat
                with patch.object(parser, 'parse_sql') as mock_single_sql:
                    mock_single_sql.return_value = {"data_sources": []}
                    parser.parse("SELECT * FROM test", "sql", "test.sql")
                    mock_single_sql.assert_called_once_with("SELECT * FROM test", "test.sql")

                # 测试MD单轮模式 - 应该调用1次chat
                with patch.object(parser, 'parse_md') as mock_single_md:
                    mock_single_md.return_value = {"data_sources": []}
                    parser.parse("# Test", "md")
                    mock_single_md.assert_called_once_with("# Test")

    finally:
        # 恢复原始配置
        settings.use_multi_round_extraction = original_config
