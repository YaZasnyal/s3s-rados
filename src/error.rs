use std::panic::Location;

use tracing::error;

#[inline]
#[track_caller]
pub(crate) fn log(source: &dyn std::error::Error) {
    let location = Location::caller();
    let span_trace = tracing_error::SpanTrace::capture();

    error!(
        target: "s3s_rados",
        %location,
        error=%source,
        "span trace:\n{span_trace}"
    );
}

macro_rules! try_ {
    ($result:expr) => {
        match $result {
            Ok(val) => val,
            Err(err) => {
                crate::error::log(&err);
                return Err(::s3s::S3Error::internal_error(err));
            }
        }
    };
}
