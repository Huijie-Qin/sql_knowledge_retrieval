from src.prompt_manager import PromptManager

def test_prompt_load():
    manager = PromptManager()
    md_prompt = manager.get_prompt("parse_md", content="test content")
    sql_prompt = manager.get_prompt("parse_sql", content="test sql", filename="test.sql")
    merge_prompt = manager.get_prompt("merge_data_source", old_content="old", new_content="new")
    assert len(md_prompt) > 0
    assert len(sql_prompt) > 0
    assert len(merge_prompt) > 0
