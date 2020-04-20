use std::ops::Range;

#[derive(Clone, Copy, Debug)]
pub struct LogPointer {
    pub generation: u64,
    pub offset: u64,
    pub length: u64,
}

impl From<(u64, Range<u64>)> for LogPointer {
    fn from((generation, range): (u64, Range<u64>)) -> LogPointer {
        LogPointer {
            generation: generation,
            offset: range.start,
            length: range.end - range.start,
        }
    }
}
