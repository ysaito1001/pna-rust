use std::ops::Range;

pub struct LogPointer {
    pub offset: u64,
    pub length: u64,
}

impl From<Range<u64>> for LogPointer {
    fn from(range: Range<u64>) -> LogPointer {
        LogPointer {
            offset: range.start,
            length: range.end - range.start,
        }
    }
}

impl From<(u64, u64)> for LogPointer {
    fn from((offset, length): (u64, u64)) -> LogPointer {
        LogPointer {
            offset: offset,
            length: length,
        }
    }
}
