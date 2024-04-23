// SPDX-FileCopyrightText: 2024 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::io::ErrorKind;
use std::net::Shutdown;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::io::{Write,Read};
use tokio::io::ReadBuf;
use tokio::spawn;
use tokio_postgres::{Client, NoTls};
use crate::error::LakeSoulMetaDataError;

pub async fn create_connection(config: String) -> crate::error::Result<Client> {
    let config = config.parse::<tokio_postgres::config::Config>()?;
    let host_addr = config.get_hostaddrs().first().ok_or(ErrorKind::ConnectionRefused)?;

    let stream = std::net::TcpStream::connect(host_addr.to_string())?;
    let stream = FakeAsyncStream(stream);
    let (client, connection) = match config.connect_raw(stream, NoTls).await {
        Ok((client, connection)) => (client, connection),
        Err(e) => {
            eprintln!("{}", e);
            return Err(LakeSoulMetaDataError::from(ErrorKind::ConnectionRefused));
        }
    };
    spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}


struct FakeAsyncStream(std::net::TcpStream);

impl tokio::io::AsyncRead for FakeAsyncStream {
    fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let _sz = self.get_mut().0.read(buf.initialize_unfilled())?;
        Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncWrite for FakeAsyncStream {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::result::Result<usize, std::io::Error>> {
        let sz = self.get_mut().0.write(buf)?;
        Poll::Ready(Ok(sz))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), std::io::Error>> {
        self.get_mut().0.flush()?;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), std::io::Error>> {
        let _sd = self.get_mut().0.shutdown(Shutdown::Write)?;
        Poll::Ready(Ok(()))
    }
}

