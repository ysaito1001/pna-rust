use std::ops::Range;

#[derive(Clone, Copy, Debug)]
pub struct LogPointer {
    pub generation: u64,
    pub offset: usize,
    pub length: usize,
}

impl From<(u64, Range<u64>)> for LogPointer {
    fn from((generation, range): (u64, Range<u64>)) -> LogPointer {
        LogPointer {
            generation,
            offset: range.start as usize,
            length: (range.end - range.start) as usize,
        }
    }
}
