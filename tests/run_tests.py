"""
Test runner for chatbot pipeline tests.

Usage in Databricks notebook:
    import sys
    sys.path.insert(0, "/Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline")
    from tests.run_tests import run_all_tests, run_unit_tests, run_integration_tests
    run_all_tests()

Usage from command line:
    cd /Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline
    python tests/run_tests.py
"""

import sys
import os

# Prevent Python from creating __pycache__ directories (Databricks Workspace limitation)
sys.dont_write_bytecode = True
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'


# Get the absolute path to the pipeline root
PIPELINE_ROOT = "/Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline"
TEST_DIR = os.path.join(PIPELINE_ROOT, "tests")


def run_all_tests():
    """
    Run all tests (unit + integration) using pytest.
    
    Returns:
        Exit code (0 for success, non-zero for failures)
    """
    try:
        import pytest
    except ImportError:
        print("❌ pytest not installed. Installing...")
        os.system("pip install pytest -q")
        import pytest
    
    # Change to pipeline root directory
    original_dir = os.getcwd()
    
    try:
        os.chdir(PIPELINE_ROOT)
        
        # Configure pytest arguments with cache disabled for Databricks
        args = [
            "-v",                          # Verbose output
            "--tb=short",                  # Short traceback format
            "-s",                          # Don't capture stdout (show print statements)
            "--color=yes",                 # Colored output
            "-p", "no:cacheprovider",      # Disable cache (fixes Databricks pycache issue)
            "-W", "ignore::DeprecationWarning",  # Ignore deprecation warnings
            TEST_DIR,                      # Test directory (absolute path)
        ]
        
        print("=" * 70)
        print(" " * 15 + "CHATBOT PIPELINE FULL TEST SUITE")
        print("=" * 70)
        print(f"Running all tests from: {TEST_DIR}\n")
        
        # Run pytest
        exit_code = pytest.main(args)
        
        print("\n" + "=" * 70)
        if exit_code == 0:
            print("✅ ALL TESTS PASSED!")
        else:
            print(f"❌ TESTS FAILED (exit code: {exit_code})")
        print("=" * 70)
        
        return exit_code
        
    finally:
        # Always restore original directory
        os.chdir(original_dir)


def run_unit_tests():
    """
    Run only unit tests (test_silver.py, test_gold.py).
    
    Returns:
        Exit code (0 for success, non-zero for failures)
    """
    import pytest
    original_dir = os.getcwd()
    try:
        os.chdir(PIPELINE_ROOT)
        
        print("=" * 70)
        print(" " * 20 + "UNIT TESTS ONLY")
        print("=" * 70)
        
        return pytest.main([
            "-v", "-s", "-p", "no:cacheprovider",
            os.path.join(TEST_DIR, "test_silver.py"),
            os.path.join(TEST_DIR, "test_gold.py")
        ])
    finally:
        os.chdir(original_dir)


def run_integration_tests():
    """
    Run only integration tests (test_integration.py).
    
    These tests validate the entire pipeline end-to-end using actual pipeline datasets.
    
    Returns:
        Exit code (0 for success, non-zero for failures)
    """
    import pytest
    original_dir = os.getcwd()
    try:
        os.chdir(PIPELINE_ROOT)
        
        print("=" * 70)
        print(" " * 18 + "INTEGRATION TESTS ONLY")
        print("=" * 70)
        print("Testing end-to-end pipeline data flow...\n")
        
        return pytest.main([
            "-v", "-s", "-p", "no:cacheprovider",
            os.path.join(TEST_DIR, "test_integration.py")
        ])
    finally:
        os.chdir(original_dir)


def run_silver_tests():
    """Run only Silver layer unit tests."""
    import pytest
    original_dir = os.getcwd()
    try:
        os.chdir(PIPELINE_ROOT)
        return pytest.main(["-v", "-s", "-p", "no:cacheprovider", 
                           os.path.join(TEST_DIR, "test_silver.py")])
    finally:
        os.chdir(original_dir)


def run_gold_tests():
    """Run only Gold layer unit tests."""
    import pytest
    original_dir = os.getcwd()
    try:
        os.chdir(PIPELINE_ROOT)
        return pytest.main(["-v", "-s", "-p", "no:cacheprovider",
                           os.path.join(TEST_DIR, "test_gold.py")])
    finally:
        os.chdir(original_dir)


def run_specific_test(test_path):
    """
    Run a specific test or test class.
    
    Example:
        run_specific_test("test_silver.py::TestCleanThreadLookup")
        run_specific_test("test_integration.py::TestPipelineDatasets")
        run_specific_test("test_gold.py::TestGoldUserTeamMetrics::test_calculates_checkpoint_count")
    """
    import pytest
    original_dir = os.getcwd()
    try:
        os.chdir(PIPELINE_ROOT)
        full_path = os.path.join(TEST_DIR, test_path) if not test_path.startswith("/") else test_path
        return pytest.main(["-v", "-s", "-p", "no:cacheprovider", full_path])
    finally:
        os.chdir(original_dir)


if __name__ == "__main__":
    # When run as a script, execute all tests
    sys.exit(run_all_tests())


# For use in Databricks notebooks
def test():
    """Convenience function for notebook execution."""
    return run_all_tests()
