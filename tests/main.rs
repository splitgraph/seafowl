// Single main.rs for all integration tests
// https://endler.dev/2020/rust-compile-times/#combine-all-integration-tests-in-a-single-binary

use rstest::fixture;
use seafowl::config::context::setup_metrics;
use seafowl::config::schema::Metrics;

mod clade;
mod cli;
mod flight;
mod http;
mod statements;

#[fixture]
#[once]
fn metrics_setup() {
    setup_metrics(&Metrics::default())
}
