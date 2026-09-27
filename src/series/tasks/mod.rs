mod optimize_indices;
mod series_trim;
mod stale_ids;
mod utils;

pub(in crate::series) use optimize_indices::{optimize_index_fully, optimize_indices_for_db};
pub(in crate::series) use series_trim::process_series_trim;
pub(in crate::series) use stale_ids::{
    remove_all_stale_series_internal, remove_stale_series_ids_incremental,
};
