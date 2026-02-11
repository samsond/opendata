# PromQL Test Framework

Implements a Prometheus-inspired promqltest DSL for validating PromQL semantics.

## Architecture

The test framework follows Prometheus's execution model with automatic test discovery:

1. **Entry Point**: `run_all_promql_tests()` in `mod.rs`
2. **Test Discovery**: Scans `testdata/` for `*.test` files
3. **Parser**: `parse_test_file()` in `dsl.rs` parses commands
4. **Executor**: `run_test_with_storage()` executes commands against TSDB
5. **Assertion**: Compares query results against expected values

```
run_all_promql_tests() (mod.rs)
    ↓
run_builtin_tests() (runner.rs)
    ↓
discover_test_files() → *.test files in testdata/
    ↓
parse_test_file() (dsl.rs) → Command objects
    ↓
run_test_with_storage() → execute each command
    ↓
eval_instant() → compare results 
```

### Parser Responsibilities

- The test DSL parser parses only test commands (`load`, `eval`, `clear`).
- PromQL expressions are parsed by the same `promql_parser` crate used by the application.
- This ensures test evaluation uses identical query parsing logic as production code.

## Adding New Tests

Create a `.test` file in `testdata/` - no registration required.

1. Create `testdata/my_feature.test`
2. Optionally add a test function in `mod.rs` for isolated debugging:
   ```rust
   #[tokio::test]
   async fn my_feature() {
       run_test("my_feature", include_str!("testdata/my_feature.test"))
           .await
           .unwrap();
   }
   ```

The test is automatically discovered and executed by `run_all_promql_tests()`.

## Running Tests

```bash
# Run all discovered tests
cargo test -p timeseries run_all_promql_tests

# Show individual test file results
cargo test -p timeseries run_all_promql_tests -- --nocapture

# Run a specific test file (requires test function in mod.rs)
cargo test -p timeseries promql::promqltest::tests::at_modifier
```

## Test File Format

```
# Load time series data
load 5m
  metric_name{label="value"} 0+100x11

# Evaluate instant query
eval instant at 10m
  metric_name{label="value"}
    {label="value"} 200

# Clear storage
clear
```

### Commands

- `load <interval>` - Load time series at specified interval (units: ms, s, m, h)
- `eval instant at <time>` - Execute instant query at timestamp (units: ms, s, m, h, or unitless)
- `clear` - Reset storage (creates fresh TSDB instance)

### Value Patterns

- `0 100 200` - Explicit values at steps 0, 1, 2
- `0+100x11` - Start at 0, increment by 100, repeat 11 times (generates steps 0-10)
- `_` - Omit value at this step
- `0+10x100 100+20x50` - Multiple expressions (see "Multiple Value Expressions" below)

### Multiple Value Expressions

When multiple value expressions appear on one line, they are treated as **sequential blocks** to ensure monotonically increasing timestamps:

```
load 5m
  metric 0+10x3 100+20x2
```

This produces:
- Steps 0-2: values 0, 10, 20 (from first expression)
- Steps 3-4: values 100, 120 (from second expression)

**Not** steps 0-2 and 0-1 (which would cause backwards timestamps).

This is a test-driver constraint to guarantee strictly increasing timestamps before ingestion.

### Timestamp Computation

Timestamps are computed as: `timestamp = base_time + (step_index × interval)`

Where `base_time` defaults to `UNIX_EPOCH` in this implementation.

Example with `load 5m`:
- Step 0: base_time + 0m
- Step 1: base_time + 5m
- Step 2: base_time + 10m

### Duration vs Timestamp Semantics

**Durations** (relative time spans):
- Used in: `load <interval>`, `offset`, range selectors `[5m]`
- **Require units**: `10s`, `5m`, `1h`, `100ms`
- Examples: `load 5m`, `metric offset 50s`, `rate(metric[5m])`

**Timestamps** (absolute time points):
- Used in: `eval instant at <time>`, `@` modifier in queries
- **Units optional**: `100` (Unix seconds), `100.5` (fractional), `100s`, `5m`, `1m40s`
- Examples: `eval instant at 100`, `metric @ 100.5`, `metric @ 1m40s`
- All equivalent: `@ 100`, `@ 100s`, `@ 1m40s` (all mean 100 seconds since Unix epoch)

### Label Matching

The metric name is represented as the `__name__` label. Test expectations follow this model:

- `{job="test"} 42` - Matches any metric with `job="test"` (ignores `__name__`)
- `{__name__="metric", job="test"} 42` - Must match both metric name and labels

The assertion logic only checks labels present in the expected sample, allowing flexible matching.

### Result Ordering

PromQL does not guarantee ordering of instant vector results unless explicitly requested (e.g., via `sort()`).

To avoid flaky tests, the framework deterministically sorts both expected and actual result sets by label set before comparison.

## Reference

Based on Prometheus promqltest: https://github.com/prometheus/prometheus/tree/main/promql/promqltest
