use std::io::{Stderr, Stdout, Write};

use clio::Output;
use parking_lot::{Mutex, MutexGuard};
use tracing_appender::non_blocking::NonBlocking;
use tracing_subscriber::fmt::MakeWriter;

pub type MakeStdout = MakeLogWriter<fn() -> Stdout, NonBlocking>;
pub type MakeStderr = MakeLogWriter<fn() -> Stderr, NonBlocking>;
pub type MakeFile = MakeLogWriter<BlockingWriter<Output>, NonBlocking>;

pub struct BlockingWriter<W>(Mutex<W>);

impl<W> BlockingWriter<W> {
    pub fn new(writer: W) -> Self {
        Self(Mutex::new(writer))
    }
}

pub struct BlockingWriterGuard<'a, W>(MutexGuard<'a, W>);

impl<W: Write> Write for BlockingWriterGuard<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl<'a, W: Write + 'static> MakeWriter<'a> for BlockingWriter<W> {
    type Writer = BlockingWriterGuard<'a, W>;

    fn make_writer(&'a self) -> Self::Writer {
        BlockingWriterGuard(self.0.lock())
    }
}

pub enum LogWriter<B, N> {
    Blocking(B),
    NonBlocking(N),
}

impl<B, N> Write for LogWriter<B, N>
where
    B: Write,
    N: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            LogWriter::Blocking(w) => w.write(buf),
            LogWriter::NonBlocking(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            LogWriter::Blocking(w) => w.flush(),
            LogWriter::NonBlocking(w) => w.flush(),
        }
    }
}

pub enum MakeLogWriter<B, N> {
    Blocking(B),
    NonBlocking(N),
}

impl<'a, B, N> MakeWriter<'a> for MakeLogWriter<B, N>
where
    B: MakeWriter<'a>,
    N: MakeWriter<'a>,
{
    type Writer = LogWriter<B::Writer, N::Writer>;

    fn make_writer(&'a self) -> Self::Writer {
        match self {
            MakeLogWriter::Blocking(w) => LogWriter::Blocking(w.make_writer()),
            MakeLogWriter::NonBlocking(w) => LogWriter::NonBlocking(w.make_writer()),
        }
    }
}
