use bitflags::bitflags;
use clio::OutputPath;
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use parking_lot::Mutex;

use crate::logging::LogConfig;
use crate::logging::LogStrictnessLayer;
use crate::logging::writer::{BlockingWriter, MakeFile, MakeLogWriter, MakeStderr, MakeStdout};

#[derive(Clone, Debug, Default)]
pub struct LogMode {
    pub terminal: bool,
    pub file: Option<OutputPath>,
}

bitflags! {
    #[derive(Clone, Copy, Debug, Default)]
    pub struct LogOrdered: u8 {
        const NONE = 0;
        const TERMINAL = 1;
        const FILE = 2;
    }
}

pub struct LogGuard;

impl LogGuard {
    fn guards() -> &'static Mutex<Vec<WorkerGuard>> {
        static GUARDS: Mutex<Vec<WorkerGuard>> = Mutex::new(Vec::new());
        &GUARDS
    }

    pub fn flush() {
        Self::guards().lock().clear();
    }
}

macro_rules! terminal_layer {
    ($writer:expr) => {
        fmt::layer()
            .with_writer($writer)
            .with_target(false)
            .with_ansi(true)
    };
}

macro_rules! file_layer {
    ($writer:expr) => {
        fmt::layer()
            .with_writer($writer)
            .with_target(true)
            .with_thread_names(true)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .with_ansi(false)
    };
}

impl LogGuard {
    pub fn with_config(config: LogConfig) {
        LogStrictnessLayer::set(config.strictness);

        let terminal_ordered = config.order.contains(LogOrdered::TERMINAL);
        let file_ordered = config.order.contains(LogOrdered::FILE);

        let stdout: MakeStdout = if terminal_ordered {
            MakeLogWriter::Blocking(std::io::stdout)
        } else {
            let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
            Self::guards().lock().push(guard);
            MakeLogWriter::NonBlocking(writer)
        };

        let stderr: MakeStderr = if terminal_ordered {
            MakeLogWriter::Blocking(std::io::stderr)
        } else {
            let (writer, guard) = tracing_appender::non_blocking(std::io::stderr());
            Self::guards().lock().push(guard);
            MakeLogWriter::NonBlocking(writer)
        };

        let file_writer: Option<MakeFile> = config.mode.file.map(|path| {
            let file = path.create().expect("Failed to create log file");
            if file_ordered {
                MakeLogWriter::Blocking(BlockingWriter::new(file))
            } else {
                let (writer, guard) = tracing_appender::non_blocking(file);
                Self::guards().lock().push(guard);
                MakeLogWriter::NonBlocking(writer)
            }
        });

        match (config.mode.terminal, file_writer) {
            (false, None) => {
                // Discard
                tracing_subscriber::registry().with(config.level).init();
            }

            (true, None) => {
                // Terminal only
                tracing_subscriber::registry()
                    .with(
                        config.level, //
                    )
                    .with(
                        terminal_layer!(stdout)
                            .with_filter(filter_fn(|meta| *meta.level() > Level::WARN)),
                    )
                    .with(
                        terminal_layer!(stderr)
                            .with_filter(filter_fn(|meta| *meta.level() <= Level::WARN)),
                    )
                    .with(LogStrictnessLayer)
                    .init();
            }

            (false, Some(file)) => {
                // File only
                tracing_subscriber::registry()
                    .with(
                        config.level, //
                    )
                    .with(
                        file_layer!(file), //
                    )
                    .with(LogStrictnessLayer)
                    .init();
            }

            (true, Some(file)) => {
                // Terminal + File
                tracing_subscriber::registry()
                    .with(
                        config.level, //
                    )
                    .with(
                        file_layer!(file), //
                    )
                    .with(
                        terminal_layer!(stdout)
                            .with_filter(filter_fn(|meta| *meta.level() > Level::WARN)),
                    )
                    .with(
                        terminal_layer!(stderr)
                            .with_filter(filter_fn(|meta| *meta.level() <= Level::WARN)),
                    )
                    .with(LogStrictnessLayer)
                    .init();
            }
        }
    }
}
