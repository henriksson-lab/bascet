mod formatter;

use tracing::Level;

pub fn init(level: Level) {
    let _ = tracing_subscriber::fmt()
        .event_format(formatter::Formatter)
        .fmt_fields(formatter::Bare)
        .with_max_level(level)
        .try_init();
}
