use crate::promql::QueryValue;
use crate::promql::engine::Tsdb;
use crate::promql::engine::test_utils::MockSeriesQuerier;
use crate::promql::promqltest::assert::assert_results;
use crate::promql::promqltest::dsl::*;
use crate::promql::promqltest::evaluator::{eval_instant, eval_range};
use crate::promql::promqltest::loader::load_series;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
// ============================================================================
// Test Discovery
// ============================================================================

/// Discover all .test files in a directory (matches Prometheus fs.Glob pattern)
fn discover_test_files(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();

    for entry in fs::read_dir(dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("test") {
            files.push(path);
        }
    }

    files.sort();
    Ok(files)
}

// ============================================================================
// Test Runner (Orchestration)
// ============================================================================

/// Run all embedded test files (matches Prometheus RunBuiltinTests)
fn run_builtin_tests() -> Result<(), String> {
    run_builtin_tests_with_storage(new_test_storage)
}

/// Run all tests with a custom storage factory (matches Prometheus RunBuiltinTestsWithStorage)
fn run_builtin_tests_with_storage<F>(storage_factory: F) -> Result<(), String>
where
    F: Fn() -> (Tsdb, Arc<MockSeriesQuerier>),
{
    let test_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("promql")
        .join("promqltest")
        .join("testdata");

    let files = discover_test_files(&test_dir)?;

    for path in files {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or("Invalid test filename")?;

        let content = fs::read_to_string(&path).map_err(|e| format!("{name}: {}", e))?;

        run_test_with_storage(name, &content, &storage_factory)
            .map_err(|e| format!("{name}: {}", e))?;
    }

    Ok(())
}

/// Run a single test file (matches Prometheus RunTest)
pub fn run_test(name: &str, content: &str) -> Result<(), String> {
    run_test_with_storage(name, content, &new_test_storage)
}

/// Run a single test file with custom storage (matches Prometheus RunTestWithStorage)
fn run_test_with_storage<F>(name: &str, content: &str, storage_factory: &F) -> Result<(), String>
where
    F: Fn() -> (Tsdb, Arc<MockSeriesQuerier>),
{
    let commands = parse_test_file(content)?;
    let (mut tsdb, mut storage) = storage_factory();
    let mut eval_count = 0;
    let mut ignoring = false;

    for cmd in commands {
        match cmd {
            Command::Clear(_) => {
                // Create fresh TSDB instance - clears all state including caches and snapshots
                let pair = storage_factory();
                tsdb = pair.0;
                storage = pair.1;
            }

            Command::Ignore(_) => {
                ignoring = true;
            }

            Command::Resume(_) => {
                ignoring = false;
            }

            Command::Load(load_cmd) => {
                if !ignoring {
                    load_series(Arc::clone(&storage), load_cmd.interval, &load_cmd.series)?;
                }
            }

            Command::EvalRange(eval_cmd) => {
                if !ignoring {
                    eval_count += 1;
                    let result = eval_range(
                        &tsdb,
                        eval_cmd.start,
                        eval_cmd.end,
                        eval_cmd.step,
                        &eval_cmd.query,
                    )?;
                    let result = QueryValue::Matrix(result);
                    let expected = eval_cmd.expected.clone();
                    assert_results(result, expected, false, name, eval_count, &eval_cmd.query)?;
                }
            }

            Command::EvalInstant(eval_cmd) => {
                if !ignoring {
                    eval_count += 1;
                    let result = eval_instant(&tsdb, eval_cmd.time, &eval_cmd.query)?;

                    let expected = eval_cmd.expected.clone();
                    assert_results(
                        result,
                        expected,
                        eval_cmd.expect_ordered,
                        name,
                        eval_count,
                        &eval_cmd.query,
                    )?;
                }
            }
        }
    }

    Ok(())
}

// ============================================================================
// Storage Factory
// ============================================================================

fn new_test_storage() -> (Tsdb, Arc<MockSeriesQuerier>) {
    let storage = Arc::new(MockSeriesQuerier::new());
    // Coerce Arc<MockSeriesQuerier> to Arc<dyn SeriesQuerier> for Tsdb
    let querier: Arc<dyn crate::promql::engine::QueryReader> = storage.clone();
    (Tsdb::new(querier), storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::promqltest::evaluator::eval_instant;
    use std::collections::HashMap;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn should_load_series_into_storage() {
        // given
        let (tsdb, storage) = new_test_storage();
        let series = vec![SeriesLoad {
            labels: HashMap::from([
                ("__name__".to_string(), "test_metric".to_string()),
                ("job".to_string(), "test".to_string()),
            ]),
            values: vec![(0, 1.0), (1, 2.0), (2, 3.0)],
        }];

        // when
        load_series(Arc::clone(&storage), Duration::from_secs(60), &series).unwrap();

        // then
        let result =
            eval_instant(&tsdb, UNIX_EPOCH + Duration::from_secs(60), "test_metric").unwrap();
        let QueryValue::Vector(result) = result else {
            panic!("Expected instant vector result");
        };
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 2.0);
    }

    #[test]
    fn should_evaluate_query_at_specific_time() {
        // given
        let (tsdb, storage) = new_test_storage();
        let series = vec![SeriesLoad {
            labels: HashMap::from([("__name__".to_string(), "metric".to_string())]),
            values: vec![(0, 10.0), (1, 20.0), (2, 30.0)],
        }];
        load_series(Arc::clone(&storage), Duration::from_secs(60), &series).unwrap();

        // when
        let result = eval_instant(&tsdb, UNIX_EPOCH + Duration::from_secs(120), "metric").unwrap();
        let result = result.into_matrix().unwrap();

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].samples[0].value, 30.0);
    }

    #[test]
    fn should_run_simple_test_file() {
        // given
        let content = r#"
load 5m
  metric 1 2 3

eval instant at 10m
  metric
    {} 3
"#;

        // when
        let result = run_test("simple_test", content);

        // then
        assert!(result.is_ok());
    }

    #[test]
    fn pi_test() {
        let content = r#"
load 1m
    metric1{a="a"} 0+1x100
    metric2{b="b"} 0+1x50

# operator with offset
eval instant at 90m metric1 offset 15m or metric2 offset 45m
    metric1{a="a"} 75
    metric2{b="b"} 45

clear
"#;

        // when
        let result = run_test("pi_test", content);

        // then
        if let Err(e) = result {
            panic!("run_test failed: {}", e);
        }
    }

    #[test]
    fn should_reject_negative_step_index() {
        // given
        let (_tsdb, storage) = new_test_storage();
        let series = vec![SeriesLoad {
            labels: HashMap::from([("__name__".to_string(), "metric".to_string())]),
            values: vec![(-1, 10.0)], // Negative step
        }];

        // when
        let result = load_series(Arc::clone(&storage), Duration::from_secs(60), &series);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Negative step index"));
    }
}
