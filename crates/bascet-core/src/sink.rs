pub mod channel;
pub mod drain;

use std::future::Future;

use crate::layer::Layer;
use crate::pipeline::shutdown::Shutdown;
use crate::set::Set;
use crate::source::Pull;
use crate::stage::Output;
use crate::utils::channel::{AsyncPressurisedReceiver, AsyncPressurisedSender};

pub trait Sink: Layer {
    type Input<'a>;

    fn consume<W: Set>(&mut self, input: Self::Input<'_>) -> impl Future<Output = ()> + Send;

    fn drive<W: Set + 'static, T: Send + 'static>(
        self,
        out_res_rx: AsyncPressurisedReceiver<Output<T>>,
        out_req_tx: AsyncPressurisedSender<Pull>,
        shutdown: Shutdown,
    ) where
        Self: Sized + Send + 'static,
        T: Into<Self::Input<'static>>,
    {
        std::thread::spawn(move || {
            let mut sink = self;
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap()
                .block_on(async move {
                    out_req_tx.send_async(Pull::Next).await.ok();
                    loop {
                        match out_res_rx.recv_async().await {
                            Ok(Output::Shutdown) | Err(_) => break,
                            Ok(Output::Value(item)) => {
                                sink.consume::<W>(item.into()).await;
                                out_req_tx.send_async(Pull::Next).await.ok();
                            }
                            Ok(Output::Error(e)) => {
                                tracing::error!("{e}");
                                out_req_tx.send_async(Pull::Next).await.ok();
                            }
                        }
                    }
                    shutdown.trigger();
                });
        });
    }
}
