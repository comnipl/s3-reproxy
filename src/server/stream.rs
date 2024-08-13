use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use http_body::{Body, SizeHint};
use pin_project::pin_project;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{error, info, info_span, instrument, warn, Instrument};

// TODO: unwrap 多すぎ……

//https://docs.rs/aws-sdk-s3/latest/aws_sdk_s3/primitives/struct.SdkBody.html#method.from_body_1_x

type ByteStreamResult = Option<Result<Bytes, ByteStreamError>>;

pub(crate) struct ByteStreamMultiplier {
    subscribe_tx: Option<mpsc::Sender<oneshot::Sender<mpsc::Receiver<ByteStreamResult>>>>,
    size_hint_rx: watch::Receiver<http_body::SizeHint>,
}

pub type FirstByteSignal = oneshot::Receiver<()>;

impl ByteStreamMultiplier {
    pub fn from_bytestream(mut stream: ByteStream) -> (Self, FirstByteSignal) {
        let (first_byte_tx, first_byte_rx) = oneshot::channel();

        let (listen_tx, mut listen_rx) = mpsc::channel(4);

        let (subscribe_tx, mut subscribe_rx) =
            mpsc::channel::<oneshot::Sender<mpsc::Receiver<ByteStreamResult>>>(4);
        let (size_hint_tx, size_hint_rx) = watch::channel(http_body::SizeHint::default());

        tokio::spawn(
            async move {
                size_hint_tx
                    .send(convert_sizehint(stream.size_hint()))
                    .unwrap();
                let mut first_byte_tx = Some(first_byte_tx);
                let spawned_at = tokio::time::Instant::now();
                while let Some(data) = stream.next().await {
                    if let Some(tx) = first_byte_tx.take() {
                        info!(
                            "first byte received ({}ms)",
                            spawned_at.elapsed().as_millis()
                        );
                        tx.send(()).unwrap();
                    }
                    let payload = data.map_err(|e| ByteStreamError::ByteStreamError(e.to_string()));
                    listen_tx.send(Some(payload)).await.unwrap();
                    size_hint_tx
                        .send(convert_sizehint(stream.size_hint()))
                        .unwrap();
                }
                info!("stream ended");
                drop(listen_tx);
            }
            .instrument(info_span!("stream_listener")),
        );

        tokio::spawn(
            async move {
                let mut read_cache = vec![];
                let mut txs = vec![];
                let mut will_be_new_tx = true;

                loop {
                    tokio::select! {
                        Some(frame_rx_tx) = subscribe_rx.recv() => {
                            if !will_be_new_tx {
                                error!("new tx is not allowed");
                                break;
                            }
                            let (tx, rx) = mpsc::channel(16);
                            frame_rx_tx.send(rx).unwrap();
                            for payload in read_cache.iter().cloned() {
                                tx.send(payload).await.unwrap();
                            }
                            txs.push(tx);
                        }
                        Some(payload) = listen_rx.recv() => {
                            if subscribe_rx.is_closed() && will_be_new_tx {
                                will_be_new_tx = false;
                                info!("subscribe_rx is closed");
                            }

                            for tx in txs.iter_mut() {
                                tx.send(payload.clone()).await.unwrap();
                            }

                            if will_be_new_tx {
                                read_cache.push(payload);
                            }
                        }
                        else => {
                            break;
                        }
                    }
                }

                info!("stream broadcaster ended");
            }
            .instrument(info_span!("stream_broadcaster")),
        );

        (
            Self {
                subscribe_tx: Some(subscribe_tx),
                size_hint_rx,
            },
            first_byte_rx,
        )
    }

    pub async fn subscribe_stream(&self, part_number: Option<i32>) -> Option<ByteStream> {
        let subscribe_tx = self.subscribe_tx.clone()?;
        let (tx, rx) = oneshot::channel();
        subscribe_tx.send(tx).await.unwrap();
        let receiver: ByteStreamReceiver = ByteStreamReceiver {
            frame_rx: rx.await.unwrap(),
            size_hint_rx: self.size_hint_rx.clone(),
            is_end_stream_reached: false,
            part_number,
        };
        Some(ByteStream::from_body_1_x(receiver))
    }

    pub fn close(&mut self) {
        drop(self.subscribe_tx.take())
    }
}

fn convert_sizehint(bound: (u64, Option<u64>)) -> SizeHint {
    let mut size_hint = SizeHint::default();
    size_hint.set_lower(bound.0);
    if let Some(upper) = bound.1 {
        size_hint.set_upper(upper);
    }
    size_hint
}

#[derive(Error, Clone, Debug)]
enum ByteStreamError {
    #[error("disconnected")]
    Disconnected,
    #[error("byte stream error: {0}")]
    ByteStreamError(String),
}

#[pin_project]
struct ByteStreamReceiver {
    frame_rx: mpsc::Receiver<ByteStreamResult>,
    size_hint_rx: watch::Receiver<http_body::SizeHint>,
    is_end_stream_reached: bool,
    part_number: Option<i32>,
}

impl Body for ByteStreamReceiver {
    type Data = Bytes;
    type Error = ByteStreamError;

    #[instrument(skip_all, name = "byte_stream_receiver/poll", fields(part_number = self.part_number))]
    #[allow(clippy::type_complexity)]
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let project = self.project();

        project.frame_rx.poll_recv(cx).map(|r| match r {
            Some(Some(frame)) => Some(frame.map(http_body::Frame::data)),
            Some(None) => {
                info!("end stream reached");
                *project.is_end_stream_reached = true;
                None
            }
            None => {
                error!("frame_rx disconnected");
                Some(Err(ByteStreamError::Disconnected))
            }
        })
    }

    fn is_end_stream(&self) -> bool {
        self.is_end_stream_reached
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.size_hint_rx.borrow().clone()
    }
}
