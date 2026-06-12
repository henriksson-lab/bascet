use std::future::Future;
use std::marker::PhantomData;

use crate::layer::Layer;
use crate::pipeline::shutdown::Shutdown;
use crate::set::Set;
use crate::sink::Sink;
use crate::source::Pull;
use crate::stage::Output;
use crate::utils::channel::{AsyncPressurisedReceiver, AsyncPressurisedSender};

pub struct Drain<T>(PhantomData<T>);

impl<T> Default for Drain<T> {
    fn default() -> Self {
        Drain(PhantomData)
    }
}

impl<T: 'static> Layer for Drain<T> {
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl<T: Send + 'static> Sink for Drain<T> {
    type Input<'a> = T;

    fn consume<W: Set>(&mut self, _: Self::Input<'_>) -> impl Future<Output = ()> + Send {
        async {}
    }

    fn drive<W: Set + 'static, U: Send + 'static>(
        self,
        out_res_rx: AsyncPressurisedReceiver<Output<U>>,
        out_req_tx: AsyncPressurisedSender<Pull>,
        shutdown: Shutdown,
    ) where
        Self: Sized + Send + 'static,
        U: Into<Self::Input<'static>>,
    {
        std::thread::spawn(move || {
            loop {
                if out_req_tx.send(Pull::Next).is_err() {
                    break;
                }
            }
        });
        std::thread::spawn(move || {
            loop {
                match out_res_rx.recv_blocking() {
                    Ok(Output::Shutdown) | Err(_) => break,
                    Ok(Output::Error(e)) => tracing::error!("{e}"),
                    Ok(Output::Value(_)) => {}
                }
            }
            shutdown.trigger();
        });
    }
}
