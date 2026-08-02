use std::fmt;

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::field::RecordFields;
use tracing_subscriber::fmt::format::{Format, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

const KINDS: [(&str, &str, &str); 3] = [
    ("fixed", "FIXED", "\x1b[1m"),
    ("help", "HELP", "\x1b[32m"),
    ("suggestion", "SUGGESTION", "\x1b[33m"),
];

const INDENT: &str = "\t";
const WIDTH: usize = width();

const fn width() -> usize {
    let mut width = 0;
    let mut index = 0;
    while index < KINDS.len() {
        let length = KINDS[index].1.len();
        if length > width {
            width = length;
        }
        index += 1;
    }
    width
}

pub struct Report;

pub struct Fields;

#[derive(Default)]
struct Annotations([String; 3]);

struct Plain<'writer, 'a> {
    writer: &'a mut Writer<'writer>,
    first: bool,
    result: fmt::Result,
}

impl<S, N> FormatEvent<S, N> for Report
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        Format::default().format_event(ctx, writer.by_ref(), event)?;

        let mut annotations = Annotations::default();
        event.record(&mut annotations);

        let ansi = writer.has_ansi_escapes();
        for (label, style, text) in annotations.present() {
            match ansi {
                true => writeln!(writer, "{INDENT}{style}{label:<WIDTH$}\x1b[0m  {text}")?,
                false => writeln!(writer, "{INDENT}{label:<WIDTH$}  {text}")?,
            }
        }
        Ok(())
    }
}

impl<'writer> FormatFields<'writer> for Fields {
    fn format_fields<R: RecordFields>(
        &self,
        mut writer: Writer<'writer>,
        fields: R,
    ) -> fmt::Result {
        let mut plain = Plain {
            writer: &mut writer,
            first: true,
            result: Ok(()),
        };
        fields.record(&mut plain);
        plain.result
    }
}

impl Visit for Annotations {
    fn record_str(&mut self, field: &Field, value: &str) {
        if let Some(index) = KINDS.iter().position(|(name, ..)| *name == field.name()) {
            self.0[index].push_str(value);
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

impl Visit for Plain<'_, '_> {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if self.result.is_err() || KINDS.iter().any(|(name, ..)| *name == field.name()) {
            return;
        }
        let padding = match self.first {
            true => {
                self.first = false;
                ""
            }
            false => " ",
        };
        self.result = match field.name() {
            "message" => write!(self.writer, "{padding}{value:?}"),
            name => write!(self.writer, "{padding}{name}={value:?}"),
        };
    }
}

impl Annotations {
    fn present(&self) -> impl Iterator<Item = (&'static str, &'static str, &str)> {
        KINDS
            .iter()
            .zip(&self.0)
            .filter(|(_, text)| !text.is_empty())
            .map(|((_, label, style), text)| (*label, *style, text.as_str()))
    }
}
