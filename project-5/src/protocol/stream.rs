use async_std::{
    io::Read,
    net::TcpStream,
    pin::Pin,
    prelude::*,
    task::{Context, Poll},
};
use serde::Serialize;

use super::{decoder::KvsDecoder, encoder::KvsEncoder, request::Request};
use crate::{KvsError, Result};

const BUFFER_CAPACITY: usize = 2 * 1024;
const MAX_MESSAGE_SIZE: usize = 4 * 1024;

pub struct KvsStream {
    encoder: KvsEncoder,
    decoder: KvsDecoder,
    tcp_stream: TcpStream,
}

impl KvsStream {
    pub fn new(tcp_stream: TcpStream) -> Self {
        KvsStream {
            encoder: KvsEncoder::new(BUFFER_CAPACITY),
            decoder: KvsDecoder::new(BUFFER_CAPACITY),
            tcp_stream,
        }
    }

    pub async fn send<T: Serialize>(&mut self, response: T) -> Result<()> {
        let encoded = self.encoder.encode(response)?;
        Ok(self.tcp_stream.write_all(&encoded).await?)
    }

    fn next_value(&mut self, cx: &mut Context<'_>) -> Poll<Result<usize>> {
        let mut buffer = vec![0u8; MAX_MESSAGE_SIZE];
        match Read::poll_read(Pin::new(&mut self.tcp_stream), cx, &mut buffer) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(n)) => {
                if n > 0 {
                    buffer.truncate(n);
                    self.decoder.append(&buffer);
                }
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(KvsError::Io(e))),
        }
    }
}

impl Stream for KvsStream {
    type Item = Result<Request>;

    fn poll_next(mut self: Pin<&mut KvsStream>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.decoder.decode() {
                Some(a) => return Poll::Ready(Some(a)),
                None => (),
            }

            match self.next_value(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(n)) if n == 0 => return Poll::Ready(None),
                Poll::Ready(Ok(_)) => (),
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}
