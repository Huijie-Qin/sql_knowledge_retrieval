#!/usr/bin/env python3
"""测试prompt模板变量替换是否正常"""

from src.prompt_manager import PromptManager

def test_prompt_templates():
    prompt_manager = PromptManager()

    # 测试SQL多轮模板
    print("测试SQL多轮模板:")
    for round_num in range(1, 4):
        template_name = f"parse_sql_round{round_num}"
        try:
            prompt = prompt_manager.get_prompt(
                template_name,
                content="测试文档内容",
                filename="test.sql",
                round1_result='{"test": "result1"}',
                round2_result='{"test": "result2"}'
            )
            print(f"  ✓ {template_name} 渲染成功")
            # 检查是否包含正确的变量
            assert "测试文档内容" in prompt, f"{template_name} 未包含content变量"
            assert "test.sql" in prompt, f"{template_name} 未包含filename变量"
        except Exception as e:
            print(f"  ✗ {template_name} 渲染失败: {e}")
            raise

    # 测试MD多轮模板
    print("\n测试MD多轮模板:")
    for round_num in range(1, 4):
        template_name = f"parse_md_round{round_num}"
        try:
            prompt = prompt_manager.get_prompt(
                template_name,
                content="测试文档内容",
                round1_result='{"test": "result1"}',
                round2_result='{"test": "result2"}'
            )
            print(f"  ✓ {template_name} 渲染成功")
            # 检查是否包含正确的变量
            assert "测试文档内容" in prompt, f"{template_name} 未包含content变量"
        except Exception as e:
            print(f"  ✗ {template_name} 渲染失败: {e}")
            raise

    # 检查旧的变量名是否已经被替换
    print("\n检查旧变量名是否已移除:")
    for template_name in [
        "parse_sql_round1", "parse_sql_round2", "parse_sql_round3",
        "parse_md_round1", "parse_md_round2", "parse_md_round3"
    ]:
        template_path = f"prompts/{template_name}.j2"
        with open(template_path, 'r') as f:
            content = f.read()
            if "{{ document_content }}" in content:
                print(f"  ✗ {template_name} 仍然包含旧变量名 document_content")
                raise ValueError(f"{template_name} 包含未替换的旧变量名")
            else:
                print(f"  ✓ {template_name} 已移除旧变量名 document_content")

    print("\n所有模板测试通过！")

if __name__ == "__main__":
    test_prompt_templates()