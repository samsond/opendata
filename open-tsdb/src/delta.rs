use std::{collections::HashMap, sync::Arc};

use opendata_common::storage::StorageSnapshot;
use opentelemetry_proto::tonic::metrics::v1::Metric;
use uuid::Uuid;

use crate::{
    index::{ForwardIndex, InvertedIndex},
    model::{MetricType, Sample, SeriesFingerprint, SeriesId, SeriesSpec, TimeBucket},
    otel::OtelUtil,
    util::{Fingerprint, Result, normalize_str},
};

/// The delta chunk is the current in-memory segment of OpenTSDB representing
/// the data that has been ingested but not yet flushed to storage.
pub(crate) struct TsdbDeltaBuilder<'a> {
    pub(crate) bucket: TimeBucket,
    pub(crate) forward_index: ForwardIndex,
    pub(crate) inverted_index: InvertedIndex,
    pub(crate) series_dict: &'a HashMap<SeriesFingerprint, SeriesId>,
    pub(crate) series_dict_delta: HashMap<SeriesFingerprint, SeriesId>,
    pub(crate) samples: HashMap<SeriesId, Vec<Sample>>,
    pub(crate) next_series_id: u32,
}

impl<'a> TsdbDeltaBuilder<'a> {
    pub(crate) fn new(
        bucket: TimeBucket,
        series_dict: &'a HashMap<SeriesFingerprint, SeriesId>,
    ) -> Self {
        Self {
            bucket,
            forward_index: ForwardIndex::default(),
            inverted_index: InvertedIndex::default(),
            series_dict,
            series_dict_delta: HashMap::new(),
            samples: HashMap::new(),
            next_series_id: 0,
        }
    }

    pub(crate) fn ingest(mut self, metric: &Metric) -> Result<()> {
        let metric_unit = normalize_str(&metric.unit);
        let metric_type = MetricType::try_from(metric)?;
        let samples_with_attrs = OtelUtil::samples(metric);

        for sample_with_attrs in samples_with_attrs {
            let mut attributes = sample_with_attrs.attributes;

            // Sort attributes for consistent fingerprinting
            attributes.sort_by(|a, b| a.key.cmp(&b.key));

            let fingerprint = attributes.fingerprint();

            // register the series in the delta if it's not already present
            // in either the series dictionary or the delta series dictionary
            // (the latter is one that we've registered during building this
            // delta)
            let series_id = self
                .series_dict
                .get(&fingerprint)
                .or_else(|| self.series_dict_delta.get(&fingerprint))
                .copied()
                .unwrap_or_else(|| {
                    let series_id = self.next_series_id;
                    self.next_series_id = series_id + 1;

                    self.series_dict_delta.insert(fingerprint, series_id);

                    let series_spec = SeriesSpec {
                        metric_unit: metric_unit.clone(),
                        metric_type,
                        attributes: attributes.clone(),
                    };

                    self.forward_index.series.insert(series_id, series_spec);

                    for attr in &attributes {
                        let mut entry = self
                            .inverted_index
                            .postings
                            .entry(attr.clone())
                            .or_default();
                        entry.value_mut().insert(series_id);
                    }

                    series_id
                });

            let entry = self.samples.entry(series_id).or_default();
            entry.push(sample_with_attrs.sample);
        }

        Ok(())
    }

    pub(crate) fn build(self) -> TsdbDelta {
        TsdbDelta {
            bucket: self.bucket,
            forward_index: self.forward_index,
            inverted_index: self.inverted_index,
            series_dict: self.series_dict_delta,
            samples: self.samples,
        }
    }
}

pub(crate) struct TsdbDelta {
    pub(crate) bucket: TimeBucket,
    pub(crate) forward_index: ForwardIndex,
    pub(crate) inverted_index: InvertedIndex,
    pub(crate) series_dict: HashMap<SeriesFingerprint, SeriesId>,
    pub(crate) samples: HashMap<SeriesId, Vec<Sample>>,
}

/// Reference information for synchronizing a delta between ingest and query layers.
pub(crate) struct DeltaRef {
    /// The delta chunk that is being synchronized
    pub(crate) delta: TsdbDelta,
    /// The load ID from the ingest that generated this delta
    pub(crate) load_id: Uuid,
    /// The sequence number of this delta within the load
    pub(crate) seq: u64,
    /// A snapshot of the storage state _after_ applying the delta
    pub(crate) snapshot: Arc<dyn StorageSnapshot>,
}
