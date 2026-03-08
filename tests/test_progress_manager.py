from src.progress_manager import ProgressManager
from pathlib import Path
from datetime import datetime
from config.settings import settings

def test_progress_initialization():
    # Clean up any existing file first
    progress_file = settings.output_dir / "解析进度.md"
    if progress_file.exists():
        progress_file.unlink()

    manager = ProgressManager()
    assert manager.progress_file.exists()
    content = manager.progress_file.read_text()
    assert "待解析文件" in content
    assert "已解析文件" in content
    assert "数据源索引" in content
    assert "解析记录" in content

def test_add_pending_file():
    # Clean up any existing file first
    progress_file = settings.output_dir / "解析进度.md"
    if progress_file.exists():
        progress_file.unlink()

    manager = ProgressManager()
    test_file = Path("案例/测试/test.md")
    manager.add_pending_file(test_file)

    content = manager.progress_file.read_text()
    assert "- [ ] 案例/测试/test.md" in content

def test_add_pending_files():
    # Clean up any existing file first
    progress_file = settings.output_dir / "解析进度.md"
    if progress_file.exists():
        progress_file.unlink()

    manager = ProgressManager()
    test_files = [
        Path("案例/测试/test1.md"),
        Path("案例/测试/test2.md"),
        Path("案例/测试/test3.sql")
    ]
    manager.add_pending_files(test_files)

    content = manager.progress_file.read_text()
    assert "- [ ] 案例/测试/test1.md" in content
    assert "- [ ] 案例/测试/test2.md" in content
    assert "- [ ] 案例/测试/test3.sql" in content

def test_add_duplicate_pending_file():
    # Clean up any existing file first
    progress_file = settings.output_dir / "解析进度.md"
    if progress_file.exists():
        progress_file.unlink()

    manager = ProgressManager()
    test_file = Path("案例/测试/test.md")

    # Add twice
    manager.add_pending_file(test_file)
    manager.add_pending_file(test_file)

    content = manager.progress_file.read_text()
    # Should only appear once
    assert content.count("- [ ] 案例/测试/test.md") == 1

def test_mark_file_processed():
    # Clean up any existing file first
    progress_file = settings.output_dir / "解析进度.md"
    if progress_file.exists():
        progress_file.unlink()

    manager = ProgressManager()
    test_file = Path("案例/测试/test.md")
    manager.add_pending_file(test_file)

    # Verify it's in pending
    content = manager.progress_file.read_text()
    assert "- [ ] 案例/测试/test.md" in content

    manager.mark_file_processed(test_file)

    content = manager.progress_file.read_text()
    # Should not be in pending anymore
    assert "- [ ] 案例/测试/test.md" not in content
    # Should be in processed
    assert "- [x] 案例/测试/test.md" in content

def test_get_pending_files():
    # Clean up any existing file first
    progress_file = settings.output_dir / "解析进度.md"
    if progress_file.exists():
        progress_file.unlink()

    manager = ProgressManager()
    test_files = [
        Path("案例/测试/test1.md"),
        Path("案例/测试/test2.md"),
        Path("案例/测试/test3.sql")
    ]
    manager.add_pending_files(test_files)

    # Mark one as processed
    manager.mark_file_processed(test_files[1])

    pending_files = manager.get_pending_files()
    assert len(pending_files) == 2
    assert test_files[0] in pending_files
    assert test_files[2] in pending_files
    assert test_files[1] not in pending_files

def test_add_data_source_index():
    # Clean up any existing file first
    progress_file = settings.output_dir / "解析进度.md"
    if progress_file.exists():
        progress_file.unlink()

    manager = ProgressManager()
    test_file = settings.output_dir / "用户/用户信息表.md"

    manager.add_data_source_index("用户信息表", "用户域", test_file)

    content = manager.progress_file.read_text()
    assert "|用户信息表| 用户域 |用户/用户信息表.md|" in content

def test_add_duplicate_data_source_index():
    # Clean up any existing file first
    progress_file = settings.output_dir / "解析进度.md"
    if progress_file.exists():
        progress_file.unlink()

    manager = ProgressManager()
    test_file = settings.output_dir / "用户/用户信息表.md"

    # Add twice
    manager.add_data_source_index("用户信息表", "用户域", test_file)
    manager.add_data_source_index("用户信息表", "用户域", test_file)

    content = manager.progress_file.read_text()
    # Should only appear once
    assert content.count("|用户信息表| 用户域 |用户/用户信息表.md|") == 1

def test_add_parse_record():
    # Clean up any existing file first
    progress_file = settings.output_dir / "解析进度.md"
    if progress_file.exists():
        progress_file.unlink()

    manager = ProgressManager()

    manager.add_parse_record(
        "用户信息表",
        "新建",
        ["新增用户信息表解析", "补充字段注释"]
    )

    content = manager.progress_file.read_text()
    assert "|用户信息表|" in content
    assert "|新建|" in content
    assert "| 新增用户信息表解析；补充字段注释|" in content

def test_add_parse_record_without_update_points():
    # Clean up any existing file first
    progress_file = settings.output_dir / "解析进度.md"
    if progress_file.exists():
        progress_file.unlink()

    manager = ProgressManager()

    manager.add_parse_record("用户信息表", "更新")

    content = manager.progress_file.read_text()
    assert "|用户信息表|" in content
    assert "|更新|" in content
    # Should have empty update content
    assert "|更新| |" in content
