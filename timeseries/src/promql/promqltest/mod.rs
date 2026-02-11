pub mod dsl;
pub mod runner;

#[cfg(test)]
mod tests {
    use super::runner::run_test;

    #[tokio::test]
    async fn run_all_promql_tests() {
        super::runner::run_builtin_tests().await.unwrap();
    }

    #[tokio::test]
    async fn at_modifier() {
        run_test("at_modifier", include_str!("testdata/at_modifier.test"))
            .await
            .unwrap();
    }
}
