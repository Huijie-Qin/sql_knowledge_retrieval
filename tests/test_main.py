import pytest
from unittest.mock import Mock, patch
from src.main import DataSourceParser
from pathlib import Path

@pytest.fixture
def mock_dependencies():
    """Mock all external dependencies for DataSourceParser"""
    with patch('src.main.FileParser') as mock_parser, \
         patch('src.main.DataSourceManager') as mock_ds_manager, \
         patch('src.main.ProgressManager') as mock_progress_manager:
        # Create mock instances
        mock_parser_instance = Mock()
        mock_ds_manager_instance = Mock()
        mock_progress_manager_instance = Mock()

        # Set return values
        mock_parser.return_value = mock_parser_instance
        mock_ds_manager.return_value = mock_ds_manager_instance
        mock_progress_manager.return_value = mock_progress_manager_instance

        yield {
            'parser': mock_parser_instance,
            'ds_manager': mock_ds_manager_instance,
            'progress_manager': mock_progress_manager_instance
        }

def test_scan_source_files(mock_dependencies):
    parser = DataSourceParser()
    # 创建测试文件
    test_dir = Path("案例/测试")
    test_dir.mkdir(parents=True, exist_ok=True)
    (test_dir / "test1.md").write_text("# 测试", encoding="utf-8")
    (test_dir / "test2.sql").write_text("SELECT * FROM test", encoding="utf-8")
    (test_dir / "test.txt").write_text("Other file", encoding="utf-8")  # Should not be included

    files = parser.scan_source_files()
    assert len(files) >= 2
    assert any("test1.md" in f.as_posix() for f in files)
    assert any("test2.sql" in f.as_posix() for f in files)
    # Test that non-supported files are not included
    assert not any("test.txt" in f.as_posix() for f in files)

def test_process_md_file(mock_dependencies):
    """Test processing a markdown file"""
    parser = DataSourceParser()
    mock_parser = mock_dependencies['parser']
    mock_ds_manager = mock_dependencies['ds_manager']
    mock_progress = mock_dependencies['progress_manager']

    # Create test markdown file
    test_file = Path("案例/test.md")
    test_content = """# 电商业务
    订单表包含用户订单信息
    """
    test_file.write_text(test_content, encoding="utf-8")

    # Mock parse result
    mock_parser.parse.return_value = {
        "business_domain": "电商",
        "data_sources": [
            {
                "table_name": "order",
                "description": "订单表",
                "fields": []
            }
        ]
    }

    # Mock data source manager
    mock_ds_manager.create_or_update_data_source.return_value = (Path("数据源/电商/order.md"), "新建数据源", [])

    # Process the file
    parser.process_file(test_file)

    # Verify calls
    mock_parser.parse.assert_called_once_with(test_content, "md")
    mock_ds_manager.create_or_update_data_source.assert_called_once_with("order", "电商", {
        "table_name": "order",
        "description": "订单表",
        "fields": [],
        "business_domain": "电商"
    })
    mock_progress.add_data_source_index.assert_called_once()
    mock_progress.add_parse_record.assert_called_once()
    mock_progress.mark_file_processed.assert_called_once_with(test_file)

def test_process_sql_file(mock_dependencies):
    """Test processing a SQL file"""
    parser = DataSourceParser()
    mock_parser = mock_dependencies['parser']
    mock_ds_manager = mock_dependencies['ds_manager']
    mock_progress = mock_dependencies['progress_manager']

    # Create test SQL file
    test_file = Path("案例/test.sql")
    test_content = "SELECT * FROM user WHERE age > 18"
    test_file.write_text(test_content, encoding="utf-8")

    # Mock parse result
    mock_parser.parse.return_value = {
        "business_domain": "其他",
        "data_sources": [
            {
                "table_name": "user",
                "description": "用户表",
                "fields": []
            }
        ]
    }

    # Mock data source manager (exists, so update)
    mock_ds_manager.create_or_update_data_source.return_value = (Path("数据源/其他/user.md"), "更新数据源", ["更新描述"])

    # Process the file
    parser.process_file(test_file)

    # Verify calls
    mock_parser.parse.assert_called_once_with(test_content, "sql", "test.sql")
    mock_ds_manager.create_or_update_data_source.assert_called_once_with("user", "其他", {
        "table_name": "user",
        "description": "用户表",
        "fields": [],
        "business_domain": "其他"
    })
    mock_progress.add_data_source_index.assert_called_once()
    mock_progress.add_parse_record.assert_called_once()
    mock_progress.mark_file_processed.assert_called_once_with(test_file)

def test_process_unsupported_file(mock_dependencies, capfd):
    """Test processing an unsupported file type"""
    parser = DataSourceParser()
    mock_parser = mock_dependencies['parser']
    mock_progress = mock_dependencies['progress_manager']

    # Create test file with unsupported extension
    test_file = Path("案例/test.txt")
    test_file.write_text("Test content", encoding="utf-8")

    # Process the file
    parser.process_file(test_file)

    # Verify no parsing was done
    mock_parser.parse.assert_not_called()
    mock_progress.mark_file_processed.assert_not_called()

    # Check output
    captured = capfd.readouterr()
    assert "Unsupported file type: .txt" in captured.out

def test_run_workflow(mock_dependencies):
    """Test the complete run workflow"""
    parser = DataSourceParser()
    mock_progress = mock_dependencies['progress_manager']

    # Mock pending files
    mock_progress.get_pending_files.return_value = [
        Path("案例/file1.md"),
        Path("案例/file2.sql"),
        Path("案例/nonexistent.md")  # This file doesn't exist
    ]

    # Create existing files
    Path("案例/file1.md").write_text("# Test", encoding="utf-8")
    Path("案例/file2.sql").write_text("SELECT * FROM test", encoding="utf-8")

    # Mock process_file to avoid actual processing
    with patch.object(parser, 'process_file') as mock_process:
        parser.run()

        # Verify calls
        assert len(parser.scan_source_files()) >= 2
        mock_progress.add_pending_files.assert_called_once()
        mock_progress.get_pending_files.assert_called_once()
        # Should call process_file for existing files only
        assert mock_process.call_count == 2
        mock_process.assert_any_call(Path("案例/file1.md"))
        mock_process.assert_any_call(Path("案例/file2.sql"))
