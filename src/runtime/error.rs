#[derive(Clone, Copy, Debug)]
pub enum ErrorMode {
    Suppress,
    Skip,
    Fail,
}

impl std::str::FromStr for ErrorMode {
    type Err = String;
    
    fn from_str(s: &str) -> Result<Self, String> {
        let mode = match s.to_lowercase().as_str() {
            "supress" => ErrorMode::Suppress,
            "skip" => ErrorMode::Skip,
            "fail" => ErrorMode::Fail,
            _ => return Err(format!("Invalid error mode: {}", s)),
        };
        Ok(mode)
    }
}