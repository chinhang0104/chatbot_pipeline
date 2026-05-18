"""
# Chatbot Pipeline Test Suite Documentation

## Overview

This directory contains the pytest-based test suite for the chatbot pipeline.
Tests include both **unit tests** (testing individual transformation functions) 
and **integration tests** (testing end-to-end pipeline data flow).

## Directory Structure

```
tests/
├── __init__.py           # Makes tests a Python package
├── conftest.py           # Pytest fixtures and shared test data
├── test_silver.py        # Unit tests for Silver layer transformations
├── test_gold.py          # Unit tests for Gold layer aggregations
├── test_integration.py   # Integration tests for end-to-end pipeline
├── run_tests.py          # Test runner script
└── README.py             # This documentation file
```

## Test Types

### Unit Tests
* Test individual transformation functions with mock data
* Fast, isolated, no dependency on actual pipeline
* Files: `test_silver.py`, `test_gold.py`

### Integration Tests
* Test actual pipeline datasets from Bronze → Silver → Gold
* Validate schemas, data flow, business logic, and data quality
* Require pipeline to have been run successfully
* File: `test_integration.py`

## Running Tests

### Option 1: Run All Tests (Databricks Notebook)

```python
%run ./tests/run_tests
test()
```

### Option 2: Run All Tests (Python)

```python
from tests.run_tests import run_all_tests
run_all_tests()  # Runs both unit and integration tests
```

### Option 3: Run Unit Tests Only

```python
from tests.run_tests import run_unit_tests
run_unit_tests()  # Runs only test_silver.py and test_gold.py
```

### Option 4: Run Integration Tests Only

```python
from tests.run_tests import run_integration_tests
run_integration_tests()  # Runs only test_integration.py
```

### Option 5: Run Specific Layer Tests

```python
from tests.run_tests import run_silver_tests, run_gold_tests

# Run only Silver layer unit tests
run_silver_tests()

# Run only Gold layer unit tests
run_gold_tests()
```

### Option 6: Run Specific Test Class or Method

```python
from tests.run_tests import run_specific_test

# Run all tests in a class
run_specific_test("test_silver.py::TestCleanThreadLookup")
run_specific_test("test_integration.py::TestDataFlow")

# Run a specific test method
run_specific_test("test_integration.py::TestPipelineHealth::test_pipeline_has_data")
```

### Option 7: Use pytest directly (if installed)

```bash
# From pipeline root directory
pytest tests/ -v                          # All tests
pytest tests/test_integration.py -v      # Integration only
pytest tests/test_silver.py tests/test_gold.py -v  # Unit tests only
```

## Test Coverage

### Silver Layer Unit Tests (test_silver.py)

**TestCleanThreadLookup:**
* ✓ Adds updated_at timestamp column
* ✓ Preserves all rows (no deduplication)
* ✓ Output schema validation
* ✓ Type casting (string conversion)
* ✓ Duplicate handling

**TestEnrichCheckpoints:**
* ✓ JSON column parsing (checkpoint, metadata)
* ✓ Dimension table join
* ✓ Timestamp parsing and date extraction
* ✓ Version casting to integer
* ✓ Row preservation after join

### Gold Layer Unit Tests (test_gold.py)

**TestGoldUserTeamMetrics:**
* ✓ Grouping by date/user/team
* ✓ Checkpoint count calculation
* ✓ Thread count calculation
* ✓ Unique checkpoint counting
* ✓ Step statistics (min, max, avg)
* ✓ Timestamp range calculation
* ✓ Output schema validation

**TestGoldTeamSummary:**
* ✓ Grouping by team and date
* ✓ Checkpoint count per team
* ✓ User count per team
* ✓ Thread count per team
* ✓ Output schema validation

### Integration Tests (test_integration.py)

**TestPipelineDatasets:**
* ✓ Bronze datasets exist and are accessible
* ✓ Silver datasets exist and are accessible
* ✓ Gold datasets exist and are accessible

**TestBronzeLayerSchema:**
* ✓ bronze_thread_lookup has expected columns
* ✓ bronze_checkpoints has expected columns

**TestSilverLayerSchema:**
* ✓ silver_thread_lookup schema and data types
* ✓ silver_checkpoints enrichment columns

**TestGoldLayerSchema:**
* ✓ gold_user_team_metrics aggregation columns
* ✓ gold_team_summary team-level metrics

**TestDataFlow:**
* ✓ Silver thread lookup derived from Bronze
* ✓ Silver checkpoints joins thread lookup
* ✓ Gold metrics aggregate Silver checkpoints

**TestDataQuality:**
* ✓ No null thread_ids in Silver
* ✓ User_id coverage after enrichment
* ✓ Timestamp validity checks
* ✓ Positive checkpoint counts
* ✓ Team summary aggregation correctness

**TestPipelineConsistency:**
* ✓ Cross-layer thread-to-user mapping
* ✓ Team coverage across layers

**TestBusinessLogic:**
* ✓ User checkpoint counts match detail rows
* ✓ First/last checkpoint ordering

**TestPipelineHealth:**
* ✓ Pipeline has data in all layers
* ✓ Data freshness checks

## Fixtures (conftest.py)

Shared pytest fixtures provide reusable test data:

- **spark**: Spark session (uses Databricks session or creates local)
- **sample_thread_lookup**: Sample thread→user→team mapping
- **sample_bronze_checkpoints**: Raw checkpoint data with JSON columns
- **sample_enriched_checkpoints**: Enriched data for Gold layer tests

## Writing New Tests

### Best Practices

1. **Use descriptive test names:**
   ```python
   def test_calculates_checkpoint_count(self, spark, sample_data):
       # Test logic
   ```

2. **Organize tests into classes by feature:**
   ```python
   class TestMyTransformation:
       def test_feature_1(self):
           pass
       def test_feature_2(self):
           pass
   ```

3. **Use fixtures for test data:**
   ```python
   def test_my_function(self, spark, sample_bronze_checkpoints):
       result = my_function(sample_bronze_checkpoints)
       assert result.count() > 0
   ```

4. **Add new fixtures to conftest.py** for shared data

5. **Test one thing per test function**

6. **Include assertions for:**
   - Row counts
   - Schema/columns
   - Data types
   - Specific values
   - Edge cases

### When to Use Unit Tests vs Integration Tests

**Use Unit Tests when:**
* Testing transformation logic in isolation
* Testing edge cases and error handling
* Fast feedback loop during development
* No dependency on actual data

**Use Integration Tests when:**
* Validating end-to-end pipeline flow
* Verifying actual schema matches expectations
* Testing cross-layer data consistency
* Validating business metrics on real data
* Smoke testing after pipeline runs

## Example: Adding a New Unit Test

```python
# In test_silver.py
def test_handles_null_values(self, spark):
    \"\"\"Test that function handles NULL values gracefully.\"\"\"
    df = spark.createDataFrame(
        [(None, "u1", "team_a"), ("t1", None, "team_b")],
        ["thread_id", "user_id", "team"]
    )
    result = clean_thread_lookup(df)
    assert result.count() == 2
    # Add more assertions...
```

## Example: Adding a New Integration Test

```python
# In test_integration.py
class TestNewFeature:
    \"\"\"Test new feature end-to-end.\"\"\"
    
    def test_feature_works_in_pipeline(self, spark):
        \"\"\"Verify new feature produces expected results.\"\"\"
        gold = spark.read.table("main.chat_history.gold_table")
        
        # Verify new column exists
        assert "new_column" in gold.columns
        
        # Verify data quality
        assert gold.filter(F.col("new_column").isNull()).count() == 0
```

## Continuous Integration

For CI/CD pipelines, tests can be run automatically:

```bash
# Install dependencies
pip install pytest pyspark

# Run unit tests (fast, no data dependencies)
pytest tests/test_silver.py tests/test_gold.py -v

# Run integration tests (requires pipeline data)
pytest tests/test_integration.py -v

# Run with coverage
pytest tests/ -v --cov=transformations --cov-report=html
```

## Test Execution Strategy

**During Development:**
1. Run unit tests frequently (fast feedback)
2. Run integration tests after pipeline updates

**Before Deployment:**
1. Run all unit tests
2. Execute pipeline update
3. Run integration tests to validate

**In Production:**
1. Schedule integration tests after scheduled pipeline runs
2. Alert on test failures

## Troubleshooting

**Issue:** pytest not found
- Solution: Run `pip install pytest` or use the test runner which auto-installs

**Issue:** Import errors for transformations
- Solution: Ensure __init__.py exists in transformations/ directory

**Issue:** Spark session errors in local testing
- Solution: Install pyspark: `pip install pyspark`

**Issue:** Tests fail in Databricks but pass locally
- Solution: Check Spark version compatibility and use Databricks Runtime specific features carefully

**Issue:** Integration tests fail with "Table not found"
- Solution: Run the pipeline first to create the datasets, then run integration tests

**Issue:** Integration test fails with schema mismatches
- Solution: Pipeline code may have changed - update the pipeline and rerun it before integration tests
"""
