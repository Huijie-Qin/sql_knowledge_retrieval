#!/usr/bin/env python3
"""测试prompt模板结构是否符合要求"""

import json
from src.prompt_manager import PromptManager

def extract_json_structure(prompt):
    """从prompt中提取JSON结构部分"""
    start = prompt.find("```json")
    end = prompt.find("```", start + 7)
    if start == -1 or end == -1:
        raise ValueError("Prompt中未找到JSON结构")
    json_str = prompt[start + 7:end].strip()
    return json.loads(json_str)

def test_prompt_structures():
    prompt_manager = PromptManager()

    # 测试parse_sql_round2.j2结构
    print("测试parse_sql_round2.j2结构:")
    prompt = prompt_manager.get_prompt("parse_sql_round2", content="test", filename="test.sql")
    structure = extract_json_structure(prompt)
    ds = structure["data_sources"][0]
    assert "typical_application_scenarios" in ds, "缺少typical_application_scenarios字段"
    assert isinstance(ds["typical_application_scenarios"], list), "typical_application_scenarios应该是数组类型"
    print("  ✓ 包含typical_application_scenarios字段，类型正确")

    # 测试parse_md_round2.j2结构
    print("\n测试parse_md_round2.j2结构:")
    prompt = prompt_manager.get_prompt("parse_md_round2", content="test")
    structure = extract_json_structure(prompt)
    ds = structure["data_sources"][0]
    assert "typical_application_scenarios" in ds, "缺少typical_application_scenarios字段"
    assert isinstance(ds["typical_application_scenarios"], list), "typical_application_scenarios应该是数组类型"
    print("  ✓ 包含typical_application_scenarios字段，类型正确")

    # 测试parse_sql_round3.j2的data_quality结构
    print("\n测试parse_sql_round3.j2的data_quality结构:")
    prompt = prompt_manager.get_prompt("parse_sql_round3", content="test", filename="test.sql")
    structure = extract_json_structure(prompt)
    ds = structure["data_sources"][0]
    assert "data_quality" in ds, "缺少data_quality字段"
    dq = ds["data_quality"]
    expected_fields = ["daily_records", "daily_users", "coverage", "timeliness"]
    for field in expected_fields:
        assert field in dq, f"data_quality缺少{field}字段"
    # 检查不应该存在的字段
    assert "data_source" not in dq, "data_quality不应该包含data_source字段"
    assert "accuracy" not in dq, "data_quality不应该包含accuracy字段"
    assert "completeness" not in dq, "data_quality不应该包含completeness字段"
    assert "consistency" not in dq, "data_quality不应该包含consistency字段"
    assert "quality_check_rules" not in dq, "data_quality不应该包含quality_check_rules字段"
    print("  ✓ data_quality结构正确，包含所有预期字段")

    # 测试parse_md_round3.j2的data_quality结构
    print("\n测试parse_md_round3.j2的data_quality结构:")
    prompt = prompt_manager.get_prompt("parse_md_round3", content="test")
    structure = extract_json_structure(prompt)
    ds = structure["data_sources"][0]
    assert "data_quality" in ds, "缺少data_quality字段"
    dq = ds["data_quality"]
    for field in expected_fields:
        assert field in dq, f"data_quality缺少{field}字段"
    # 检查不应该存在的字段
    assert "data_source" not in dq, "data_quality不应该包含data_source字段"
    assert "accuracy" not in dq, "data_quality不应该包含accuracy字段"
    assert "completeness" not in dq, "data_quality不应该包含completeness字段"
    assert "consistency" not in dq, "data_quality不应该包含consistency字段"
    assert "quality_check_rules" not in dq, "data_quality不应该包含quality_check_rules字段"
    print("  ✓ data_quality结构正确，包含所有预期字段")

    # 验证模板中没有使用round1_result和round2_result变量
    print("\n验证模板不使用round1_result和round2_result变量:")
    for template_name in [
        "parse_sql_round1", "parse_sql_round2", "parse_sql_round3",
        "parse_md_round1", "parse_md_round2", "parse_md_round3"
    ]:
        template_path = f"prompts/{template_name}.j2"
        with open(template_path, 'r') as f:
            content = f.read()
            assert "round1_result" not in content, f"{template_name} 不应该包含round1_result变量"
            assert "round2_result" not in content, f"{template_name} 不应该包含round2_result变量"
        print(f"  ✓ {template_name} 未使用round1_result和round2_result变量")

    print("\n所有结构测试通过！")

if __name__ == "__main__":
    test_prompt_structures()