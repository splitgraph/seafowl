/// Wrapper around a DataFusion MemoryPool that records allocations,
/// deallocations, usage. Note that not all query operators register
/// themselves with the pool, so this isn't an exact limit or breakdown
/// of the memory used by the query processing.
use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use metrics::{counter, describe_gauge, gauge, Gauge};

const ALLOCATIONS: &str = "seafowl_datafusion_memory_pool_allocated_bytes_total";
const DEALLOCATIONS: &str = "seafowl_datafusion_memory_pool_freed_bytes_total";
const RESERVED: &str = "seafowl_datafusion_memory_pool_reserved_bytes_current";

struct Metrics {
    memory_reserved: Gauge,
}

impl Metrics {
    pub fn new() -> Self {
        describe_gauge!(
            ALLOCATIONS,
            "Memory allocated in DataFusion's managed memory pool"
        );
        describe_gauge!(
            DEALLOCATIONS,
            "Memory freed in DataFusion's managed memory pool"
        );
        describe_gauge!(
            RESERVED,
            "Current memory reserved in DataFusion's managed memory pool"
        );

        Self {
            memory_reserved: gauge!(RESERVED),
        }
    }

    pub fn register_allocation(
        reservation: &MemoryReservation,
        size: usize,
        success: bool,
    ) {
        let name = extract_consumer_name(reservation.consumer().name()).to_string();
        let result = if success { "success" } else { "error" };
        counter!(ALLOCATIONS, "consumer" => name, "result" => result)
            .increment(size.try_into().unwrap())
    }

    pub fn register_deallocation(reservation: &MemoryReservation, size: usize) {
        let name = extract_consumer_name(reservation.consumer().name()).to_string();
        counter!(DEALLOCATIONS, "consumer" => name).increment(size.try_into().unwrap())
    }
}

// DataFusion's consumers usually register themselves as
// e.g. ExternalSorterMerge[partition_id] where partition is basically the CPU core
// so we just meld them all into one metric for the whole query operation type
fn extract_consumer_name(name: &str) -> &str {
    &name[0..name.find('[').unwrap_or(name.len())]
}

pub struct MemoryPoolMetrics {
    inner: Arc<dyn MemoryPool>,
    metrics: Metrics,
}

impl std::fmt::Debug for MemoryPoolMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryPoolMetrics").finish()
    }
}

impl MemoryPoolMetrics {
    pub fn new(inner: Arc<dyn MemoryPool>) -> Self {
        let metrics = Metrics::new();
        metrics.memory_reserved.set(0.0);

        Self { inner, metrics }
    }
}

impl MemoryPool for MemoryPoolMetrics {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.metrics
            .memory_reserved
            .set(self.inner.reserved() as f64);
        Metrics::register_allocation(reservation, additional, true)
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.metrics
            .memory_reserved
            .set(self.inner.reserved() as f64);
        Metrics::register_deallocation(reservation, shrink)
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion_common::Result<()> {
        let result = self.inner.try_grow(reservation, additional);
        self.metrics
            .memory_reserved
            .set(self.inner.reserved() as f64);

        Metrics::register_allocation(reservation, additional, result.is_ok());

        result
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };
    use metrics::with_local_recorder;
    use metrics_exporter_prometheus::PrometheusBuilder;

    use crate::utils::assert_metric;

    use super::{MemoryPoolMetrics, ALLOCATIONS, DEALLOCATIONS, RESERVED};

    #[test]
    fn metrics() {
        let recorder = PrometheusBuilder::new().build_recorder();
        // Make a 16-byte memory pool
        with_local_recorder(&recorder, || {
            let pool: Arc<dyn MemoryPool> =
                Arc::new(MemoryPoolMetrics::new(Arc::new(GreedyMemoryPool::new(16))));

            let mut reservation = MemoryConsumer::new("SomeConsumer[20]").register(&pool);
            reservation.grow(8);
            reservation.try_grow(8).unwrap();
            reservation.try_grow(1).unwrap_err();
            reservation.free();
        });

        assert_metric(
            &recorder,
            format!(
                "{}{{consumer=\"SomeConsumer\",result=\"success\"}}",
                ALLOCATIONS
            )
            .as_str(),
            16,
        );
        assert_metric(
            &recorder,
            format!(
                "{}{{consumer=\"SomeConsumer\",result=\"error\"}}",
                ALLOCATIONS
            )
            .as_str(),
            1,
        );
        assert_metric(
            &recorder,
            format!("{}{{consumer=\"SomeConsumer\"}}", DEALLOCATIONS).as_str(),
            16,
        );
        assert_metric(&recorder, RESERVED, 0);
    }
}
