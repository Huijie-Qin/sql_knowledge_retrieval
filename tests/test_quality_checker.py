from src.quality_checker import QualityChecker
from pathlib import Path
import tempfile
import shutil
from config.settings import settings

def test_detect_duplicates():
    # Create temporary directory for test
    original_output_dir = settings.output_dir
    test_dir = tempfile.mkdtemp()
    settings.output_dir = Path(test_dir)

    try:
        # Create test data source files
        test_subdir = Path(test_dir) / "测试"
        test_subdir.mkdir(parents=True, exist_ok=True)
        (test_subdir / "table1.md").write_text("# table1\n## 1.1.数据源描述\n测试表1", encoding="utf-8")
        (test_subdir / "table2.md").write_text("# table1\n## 1.1.数据源描述\n测试表1重复", encoding="utf-8")
        (test_subdir / "table3.md").write_text("# table2\n## 1.1.数据源描述\n测试表2", encoding="utf-8")

        checker = QualityChecker()
        duplicates = checker.detect_duplicates()

        # Verify duplicates detected
        assert len(duplicates) == 1
        assert duplicates[0][0] == "table1"
        assert len(duplicates[0][1]) == 2

    finally:
        # Restore original settings and clean up
        settings.output_dir = original_output_dir
        shutil.rmtree(test_dir)

def test_detect_missing():
    # Create temporary directory for test
    original_output_dir = settings.output_dir
    test_dir = tempfile.mkdtemp()
    settings.output_dir = Path(test_dir)

    try:
        # Create test data source files
        test_subdir = Path(test_dir) / "测试"
        test_subdir.mkdir(parents=True, exist_ok=True)
        (test_subdir / "table1.md").write_text("# table1\n## 1.1.数据源描述\n测试表1", encoding="utf-8")
        (test_subdir / "table2.md").write_text("# table2\n## 1.1.数据源描述\n测试表2", encoding="utf-8")

        checker = QualityChecker()
        used_tables = ["table1", "table2", "table3", "table4"]
        missing = checker.detect_missing(used_tables)

        # Verify missing tables detected
        assert len(missing) == 2
        assert "table3" in missing
        assert "table4" in missing

    finally:
        # Restore original settings and clean up
        settings.output_dir = original_output_dir
        shutil.rmtree(test_dir)

def test_generate_report():
    # Create temporary directory for test
    original_output_dir = settings.output_dir
    test_dir = tempfile.mkdtemp()
    settings.output_dir = Path(test_dir)

    try:
        # Create test data source files
        test_subdir = Path(test_dir) / "测试"
        test_subdir.mkdir(parents=True, exist_ok=True)
        (test_subdir / "table1.md").write_text("# table1\n## 1.1.数据源描述\n测试表1", encoding="utf-8")
        (test_subdir / "table2.md").write_text("# table1\n## 1.1.数据源描述\n测试表1重复", encoding="utf-8")
        (test_subdir / "table3.md").write_text("# table2\n## 1.1.数据源描述\n测试表2", encoding="utf-8")

        checker = QualityChecker()
        duplicates = checker.detect_duplicates()
        missing = checker.detect_missing(["table1", "table2", "table3"])
        report_file = checker.generate_report(duplicates, missing)

        # Verify report file exists and has content
        assert report_file.exists()
        content = report_file.read_text(encoding="utf-8")
        assert "# 数据源检查报告" in content
        assert "发现 1 个重复数据源" in content
        assert "table1" in content
        assert "发现 1 个遗漏的数据源" in content
        assert "table3" in content
        assert "总数据源数量：2" in content

    finally:
        # Restore original settings and clean up
        settings.output_dir = original_output_dir
        shutil.rmtree(test_dir)

def test_run_full_check():
    # Create temporary directory for test
    original_output_dir = settings.output_dir
    test_dir = tempfile.mkdtemp()
    settings.output_dir = Path(test_dir)

    try:
        # Create test data source files
        test_subdir = Path(test_dir) / "测试"
        test_subdir.mkdir(parents=True, exist_ok=True)
        (test_subdir / "table1.md").write_text("# table1\n## 1.1.数据源描述\n测试表1", encoding="utf-8")
        (test_subdir / "table2.md").write_text("# table1\n## 1.1.数据源描述\n测试表1重复", encoding="utf-8")
        (test_subdir / "table3.md").write_text("# table2\n## 1.1.数据源描述\n测试表2", encoding="utf-8")

        checker = QualityChecker()
        report_file = checker.run(used_tables=["table1", "table2", "table3"])

        # Verify report was generated
        assert report_file.exists()
        content = report_file.read_text(encoding="utf-8")
        assert "完整性：" in content

    finally:
        # Restore original settings and clean up
        settings.output_dir = original_output_dir
        shutil.rmtree(test_dir)
