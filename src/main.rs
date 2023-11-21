use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::task;
use std::{io, str};
use std::io::{BufRead, BufReader};
use std::time::Duration;
use chrono::{SubsecRound, Utc};
use clap::{Parser};
use csv::WriterBuilder;
use stringreader::StringReader;
use tokio::time::sleep;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, value_delimiter = ',')]
    servers: Vec<String>,

    #[arg(short, long, value_parser = humantime::parse_duration, default_value = "1s")]
    timeout: Duration,

    #[arg(short = 'd', long, default_value_t = false)]
    headers: bool,

    #[arg(short, long, value_parser = humantime::parse_duration, default_value = "1s")]
    interval: Duration,

    command: String,
}

async fn do_send_4lw_command(server: &str, command: &str, timeout: Duration) -> io::Result<String> {
    let mut s = tokio::time::timeout(timeout / 2, TcpStream::connect(server)).await??;
    s.write_all(command.as_bytes()).await?;
    let mut rsp = String::new();
    tokio::time::timeout(timeout / 2, s.read_to_string(&mut rsp)).await??;
    Ok(rsp)
}

async fn send_4lw_command(server: &str, command: &str, timeout: Duration) -> io::Result<String> {
    if command == "role" {
        let rsp = do_send_4lw_command(server, "stat", timeout).await?;
        let reader =StringReader::new(rsp.as_str());
        for line in BufReader::new(reader).lines() {
            let mode = "Mode:";
            let line = line?;
            if line.starts_with(mode) {
                return Ok(line.strip_prefix(mode).unwrap().to_string());
            }
        }
        Ok(rsp)
    } else {
        do_send_4lw_command(server, command, timeout).await
    }
}

async fn tick(cli: &Args) -> Result<(), anyhow::Error> {
    let mut tasks = vec![];

    for server in cli.servers.iter() {
        let mut server = server.clone();
        if !server.contains(":") {
            server += ":2181";
        }

        let cmd = cli.command.to_string();
        let timeout = cli.timeout;
        let task = task::spawn(async move {
            send_4lw_command(server.as_str(), cmd.as_str(), timeout).await
        });

        tasks.push(task);
    }

    let mut writer = WriterBuilder::new().from_writer(vec![]);

    if cli.headers {
        writer.write_field("now")?;
        for s in cli.servers.iter() {
            writer.write_field(s)?;
        }
        writer.write_record(None::<&[u8]>)?;
    }

    let now = Utc::now().naive_local().round_subsecs(1);
    writer.write_field(format!("{}", now))?;

    for task in tasks {
        let rsp = task.await?;
        match rsp {
            Ok(response) => {
                writer.write_field(response)?;
            }
            Err(e) => {
                writer.write_field(format!("{}", e))?;
            }
        }
    }
    writer.write_record(None::<&[u8]>)?;

    print!("{}", String::from_utf8(writer.into_inner()?)?);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Args::parse();

    loop {
        tick(&cli).await?;
        sleep(cli.interval).await;
    }
}
