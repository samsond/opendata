use std::time::{Duration, SystemTime};

/// Request for instant query (/api/v1/query)
#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub query: String,
    pub time: Option<SystemTime>,  // Parsed timestamp
    pub timeout: Option<Duration>, // Parsed duration
}

/// Request for range query (/api/v1/query_range)
#[derive(Debug, Clone)]
pub struct QueryRangeRequest {
    pub query: String,
    pub start: SystemTime,         // Parsed start timestamp
    pub end: SystemTime,           // Parsed end timestamp
    pub step: Duration,            // Parsed step duration
    pub timeout: Option<Duration>, // Parsed timeout duration
}

/// Request for series listing (/api/v1/series)
#[derive(Debug, Clone)]
pub struct SeriesRequest {
    pub matches: Vec<String>, // e.g., ["up", "{job=\"prometheus\"}"]
    pub start: Option<i64>,   // Unix timestamp in seconds
    pub end: Option<i64>,     // Unix timestamp in seconds
    pub limit: Option<usize>, // Max number of series to return
}

/// Request for label names (/api/v1/labels)
#[derive(Debug, Clone)]
pub struct LabelsRequest {
    pub matches: Option<Vec<String>>, // Optional metric selectors
    pub start: Option<i64>,           // Unix timestamp in seconds
    pub end: Option<i64>,             // Unix timestamp in seconds
    pub limit: Option<usize>,         // Max number of labels to return
}

/// Request for label values (/api/v1/label/{name}/values)
#[derive(Debug, Clone)]
pub struct LabelValuesRequest {
    pub label_name: String,
    pub matches: Option<Vec<String>>, // Optional metric selectors
    pub start: Option<i64>,           // Unix timestamp in seconds
    pub end: Option<i64>,             // Unix timestamp in seconds
    pub limit: Option<usize>,         // Max number of values to return
}

/// Request for metric metadata (/api/v1/metadata)
#[derive(Debug, Clone)]
pub struct MetadataRequest {
    pub metric: Option<String>, // Optional metric name filter
    pub limit: Option<usize>,   // Max number of metrics to return
}

/// Request for federation (/federate)
#[derive(Debug, Clone)]
pub struct FederateRequest {
    pub matches: Vec<String>, // Required metric selectors
}
