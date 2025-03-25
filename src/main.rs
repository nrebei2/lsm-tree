mod data;
use std::{net::SocketAddr, sync::Arc};

use command::read_command;
use config::Config;
use data::Database;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

mod command;
mod config;

#[tokio::main]
async fn main() {
    let config = Config::parse_from_args();
    let db = Arc::new(Database::new(config.data_dir));

    let listener = TcpListener::bind(("127.0.0.1", config.port)).await.unwrap();

    loop {
        let (stream, client) = listener.accept().await.unwrap();

        let db_clone = db.clone();
        tokio::spawn(async move {
            println!("New connection with {:?}", client);
            handle_connection(stream, client, db_clone).await;
            println!("Closed connection with {:?}", client);
        });
    }
}

async fn handle_connection(stream: TcpStream, addr: SocketAddr, db: Arc<Database>) {
    let (read, mut write) = stream.into_split();
    let mut buf_read = BufReader::new(read);

    let mut out_buf = String::new();
    loop {
        let command = if let Ok(command) = read_command(&mut buf_read).await {
            command
        } else {
            break;
        };

        println!("Received command {:?} from {:?}", command, addr);
        command.execute(&db, &mut out_buf).await;

        let mut bytes = out_buf.into_bytes();
        // delimiter
        bytes.push(0x00);
        write.write_all(&bytes).await.unwrap();

        bytes.clear();
        out_buf = unsafe { String::from_utf8_unchecked(bytes) };
    }
}
