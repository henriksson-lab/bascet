use std::{borrow::Cow, fs::File, io::Cursor, num::NonZeroUsize, path::Path};

use anyhow::{Context, Result};
use noodles::{
    bam, bgzf, sam,
    sam::alignment::{
        RecordBuf, io::Write as _, record::data::field::Tag, record_buf::data::field::Value,
    },
};

pub type TaggedBamWriter = bam::io::Writer<bgzf::io::MultithreadedWriter<File>>;

pub fn make_bascet_read_name(record_id: &[u8], record_umi: &[u8], num_read: u64) -> String {
    let mut read_name = String::with_capacity(record_id.len() + record_umi.len() + 32);
    read_name.push_str(&String::from_utf8_lossy(record_id));
    read_name.push(':');
    read_name.push_str(&String::from_utf8_lossy(record_umi));
    read_name.push(':');
    read_name.push_str(&num_read.to_string());
    read_name
}

pub trait SamRecordSource {
    fn write_tagged_records(
        self,
        writer: &mut TaggedBamWriter,
        header: &sam::Header,
        source_name: &str,
    ) -> Result<()>;
}

impl<I, S> SamRecordSource for I
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    fn write_tagged_records(
        self,
        writer: &mut TaggedBamWriter,
        header: &sam::Header,
        source_name: &str,
    ) -> Result<()> {
        for line in self {
            write_tagged_bam_alignment(writer, header, line.as_ref(), source_name)?;
        }
        Ok(())
    }
}

pub fn create_tagged_bam_writer(
    path: &Path,
    header: &sam::Header,
    num_threads: usize,
) -> Result<TaggedBamWriter> {
    let worker_count =
        NonZeroUsize::new(num_threads).context("BAM writer thread count must be nonzero")?;
    let file =
        File::create(path).with_context(|| format!("failed to create BAM writer for {path:?}"))?;
    let bgzf_writer = bgzf::io::MultithreadedWriter::with_worker_count(worker_count, file);
    let mut writer_bam = bam::io::Writer::from(bgzf_writer);
    writer_bam.write_header(header)?;
    Ok(writer_bam)
}

pub fn finish_tagged_bam_writer(writer: TaggedBamWriter) -> Result<()> {
    let mut bgzf_writer = writer.into_inner();
    bgzf_writer.finish()?;
    Ok(())
}

pub fn split_sam_text(sam_contents: &str) -> (String, Vec<&str>) {
    let mut header_text = String::new();
    let mut alignment_lines = Vec::new();
    for line in sam_contents.lines() {
        if line.starts_with('@') {
            header_text.push_str(line);
            header_text.push('\n');
        } else if !line.is_empty() {
            alignment_lines.push(line);
        }
    }
    (header_text, alignment_lines)
}

pub fn write_sam_text_to_tagged_bam(
    sam_contents: &str,
    path: &Path,
    num_threads: usize,
    source_name: &str,
) -> Result<()> {
    let (header_text, alignment_lines) = split_sam_text(sam_contents);
    let header = header_text.parse::<sam::Header>()?;
    let mut writer = create_tagged_bam_writer(path, &header, num_threads)?;
    alignment_lines.write_tagged_records(&mut writer, &header, source_name)?;
    finish_tagged_bam_writer(writer)
}

pub trait SamRecordSink {
    fn record(&mut self, line: &str) -> Result<()>;
}

pub struct TaggedBamSamSink<'a> {
    writer: &'a mut TaggedBamWriter,
    header: &'a sam::Header,
    source_name: &'a str,
    record: RecordBuf,
}

impl<'a> TaggedBamSamSink<'a> {
    pub fn new(
        writer: &'a mut TaggedBamWriter,
        header: &'a sam::Header,
        source_name: &'a str,
    ) -> Self {
        Self {
            writer,
            header,
            source_name,
            record: RecordBuf::default(),
        }
    }
}

impl SamRecordSink for TaggedBamSamSink<'_> {
    fn record(&mut self, line: &str) -> Result<()> {
        let line = line.trim_end_matches('\n');
        if line.is_empty() {
            return Ok(());
        }

        let (cell_id, umi) = cell_umi_from_sam_qname(line, self.source_name)?;
        parse_sam_line_with_cell_umi(
            line,
            self.header,
            &mut self.record,
            self.source_name,
            &cell_id,
            umi.as_deref(),
        )?;
        self.writer
            .write_alignment_record(self.header, &self.record)?;
        Ok(())
    }
}

impl TaggedBamSamSink<'_> {
    pub fn record_with_cell_umi(
        &mut self,
        line: &str,
        cell_id: &str,
        umi: Option<&str>,
    ) -> Result<()> {
        let line = line.trim_end_matches('\n');
        if line.is_empty() {
            return Ok(());
        }

        parse_sam_line_with_cell_umi(
            line,
            self.header,
            &mut self.record,
            self.source_name,
            cell_id,
            umi,
        )?;
        self.writer
            .write_alignment_record(self.header, &self.record)?;
        Ok(())
    }
}

fn write_tagged_bam_alignment(
    writer: &mut TaggedBamWriter,
    header: &sam::Header,
    line: &str,
    source_name: &str,
) -> Result<()> {
    let line = line.trim_end_matches('\n');
    if line.is_empty() {
        return Ok(());
    }

    let (cell_id, umi) = cell_umi_from_sam_qname(line, source_name)?;
    write_tagged_bam_alignment_with_cell_umi(
        writer,
        header,
        line,
        source_name,
        &cell_id,
        umi.as_deref(),
    )
}

fn write_tagged_bam_alignment_with_cell_umi(
    writer: &mut TaggedBamWriter,
    header: &sam::Header,
    line: &str,
    source_name: &str,
    cell_id: &str,
    umi: Option<&str>,
) -> Result<()> {
    let line = line.trim_end_matches('\n');
    if line.is_empty() {
        return Ok(());
    }

    let mut record = RecordBuf::default();
    parse_sam_line_with_cell_umi(line, header, &mut record, source_name, cell_id, umi)?;

    writer.write_alignment_record(header, &record)?;
    Ok(())
}

pub fn parse_tagged_record_with_cell_umi(
    line: &str,
    header: &sam::Header,
    source_name: &str,
    cell_id: &str,
    umi: Option<&str>,
) -> Result<RecordBuf> {
    let mut record = RecordBuf::default();
    parse_sam_line_with_cell_umi(line, header, &mut record, source_name, cell_id, umi)?;
    Ok(record)
}

fn parse_sam_line_with_cell_umi(
    line: &str,
    header: &sam::Header,
    record: &mut RecordBuf,
    source_name: &str,
    cell_id: &str,
    umi: Option<&str>,
) -> Result<()> {
    let normalized_line = normalize_empty_sam_seq_qual(line);
    let mut sam_reader = sam::io::Reader::new(Cursor::new(normalized_line.as_bytes()));
    sam_reader
        .read_record_buf(header, record)
        .with_context(|| format!("failed to parse {source_name} SAM record: {normalized_line}"))?;

    record
        .data_mut()
        .insert(Tag::CELL_BARCODE_ID, Value::from(cell_id.to_owned()));
    if let Some(umi) = umi {
        record
            .data_mut()
            .insert(Tag::new(b'U', b'B'), Value::from(umi.to_owned()));
    }

    Ok(())
}

fn normalize_empty_sam_seq_qual(line: &str) -> Cow<'_, str> {
    let mut fields = line.split('\t');
    let mut normalized = String::new();
    let mut changed = false;

    for field_index in 0..11 {
        let Some(field) = fields.next() else {
            return Cow::Borrowed(line);
        };
        let field = if (field_index == 9 || field_index == 10) && field.is_empty() {
            changed = true;
            "*"
        } else {
            field
        };
        if changed && normalized.is_empty() {
            let mut prefix_end = 0;
            for (seen, (offset, _)) in line.match_indices('\t').enumerate() {
                if seen == field_index {
                    break;
                }
                prefix_end = offset + 1;
            }
            normalized.push_str(&line[..prefix_end]);
        }
        if changed {
            if field_index > 0 && !normalized.ends_with('\t') {
                normalized.push('\t');
            }
            normalized.push_str(field);
        }
    }

    if !changed {
        return Cow::Borrowed(line);
    }

    for field in fields {
        normalized.push('\t');
        normalized.push_str(field);
    }

    Cow::Owned(normalized)
}

fn cell_umi_from_sam_qname(line: &str, source_name: &str) -> Result<(String, Option<String>)> {
    let read_name = line
        .split('\t')
        .next()
        .filter(|field| !field.is_empty())
        .with_context(|| format!("{source_name} SAM record is missing QNAME"))?;
    let (cell_id, umi) = crate::fileformat::bam::readname_to_cell_umi(read_name.as_bytes());
    let cell_id = std::str::from_utf8(cell_id)
        .with_context(|| format!("cell id in read name is not UTF-8: {read_name:?}"))?
        .to_string();
    let umi = if umi.is_empty() {
        None
    } else {
        Some(
            std::str::from_utf8(umi)
                .with_context(|| format!("UMI in read name is not UTF-8: {read_name:?}"))?
                .to_owned(),
        )
    };
    Ok((cell_id, umi))
}

#[cfg(test)]
fn add_cell_umi_tags_to_sam_line_with_values(
    line: &str,
    cell_id: &str,
    umi: Option<&str>,
) -> String {
    let mut tagged_line =
        String::with_capacity(line.len() + cell_id.len() + umi.map_or(0, str::len) + 16);
    let mut wrote_field = false;
    for (index, field) in line.split('\t').enumerate() {
        if index >= 11 && (field.starts_with("CB:Z:") || field.starts_with("UB:Z:")) {
            continue;
        }
        if wrote_field {
            tagged_line.push('\t');
        }
        tagged_line.push_str(field);
        wrote_field = true;
    }

    tagged_line.push_str("\tCB:Z:");
    tagged_line.push_str(cell_id);
    if let Some(umi) = umi {
        tagged_line.push_str("\tUB:Z:");
        tagged_line.push_str(umi);
    }

    tagged_line
}

#[cfg(test)]
mod tests {
    use noodles::sam;

    use super::{
        add_cell_umi_tags_to_sam_line_with_values, cell_umi_from_sam_qname,
        parse_tagged_record_with_cell_umi,
    };

    #[test]
    fn sam_line_tags_are_added_from_bascet_read_name() {
        let line = "CELL:UMI:7\t4\t*\t0\t0\t*\t*\t0\t0\tACGT\tFFFF";
        let (cell_id, umi) = cell_umi_from_sam_qname(line, "test").unwrap();
        let tagged = add_cell_umi_tags_to_sam_line_with_values(line, &cell_id, umi.as_deref());

        assert!(tagged.ends_with("\tCB:Z:CELL\tUB:Z:UMI"));
    }

    #[test]
    fn existing_cell_and_umi_tags_are_replaced_textually() {
        let line = "CELL:UMI:7\t4\t*\t0\t0\t*\t*\t0\t0\tACGT\tFFFF\tCB:Z:OLD\tNM:i:0\tUB:Z:OLD";
        let (cell_id, umi) = cell_umi_from_sam_qname(line, "test").unwrap();
        let tagged = add_cell_umi_tags_to_sam_line_with_values(line, &cell_id, umi.as_deref());

        assert_eq!(
            tagged,
            "CELL:UMI:7\t4\t*\t0\t0\t*\t*\t0\t0\tACGT\tFFFF\tNM:i:0\tCB:Z:CELL\tUB:Z:UMI"
        );
    }

    #[test]
    fn explicit_tags_do_not_depend_on_qname_encoding() {
        let line = "read42\t4\t*\t0\t0\t*\t*\t0\t0\tACGT\tFFFF";
        let tagged = add_cell_umi_tags_to_sam_line_with_values(line, "CELL", Some("UMI"));

        assert_eq!(
            tagged,
            "read42\t4\t*\t0\t0\t*\t*\t0\t0\tACGT\tFFFF\tCB:Z:CELL\tUB:Z:UMI"
        );
    }

    #[test]
    fn parse_tagged_record_handles_sam_line_without_newline() {
        let header = sam::Header::default();
        let line = "CELL:UMI:7\t4\t*\t0\t0\t*\t*\t0\t0\tACGT\tFFFF";
        let record =
            parse_tagged_record_with_cell_umi(line, &header, "test", "CELL", Some("UMI")).unwrap();

        assert_eq!(record.data().len(), 2);
    }

    #[test]
    fn parse_tagged_record_normalizes_empty_sequence_and_quality() {
        let header = sam::Header::default();
        let line = "CELL:UMI:7\t141\t*\t0\t0\t*\t*\t0\t0\t\t\tAS:i:0\tXS:i:0";
        let record =
            parse_tagged_record_with_cell_umi(line, &header, "test", "CELL", Some("UMI")).unwrap();

        assert!(record.sequence().is_empty());
        assert!(record.quality_scores().is_empty());
        assert_eq!(record.data().len(), 4);
    }
}
