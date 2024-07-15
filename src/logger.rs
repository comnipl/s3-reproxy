use std::time::Instant;

use hyper::{body::Incoming, service::Service, Request};
use tracing::{info, instrument};

#[derive(Debug, Clone)]
pub struct Logger<S> {
    inner: S,
}
impl<S> Logger<S> {
    pub fn new(inner: S) -> Self {
        Logger { inner }
    }
}
type Req = Request<Incoming>;

impl<S> Service<Req> for Logger<S>
where
    S: Service<Req>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;
    #[instrument(
        skip_all,
        name = "request",
        fields(
            method = format!("{}", req.method()),
            uri = req.uri().path()
        )
    )]
    fn call(&self, req: Req) -> Self::Future {
        self.inner.call(req)
    }
}
