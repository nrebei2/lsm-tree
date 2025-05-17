use core::str;
use std::{
    io::{self, BufRead, BufReader, BufWriter, Write},
    net::TcpStream,
    process::Stdio,
    sync::OnceLock,
    time::Instant,
};

use clap::{command, Parser};
use command::{Command, CommandType};
use gui::{
    client_gui::{ClientGui, ClientInput},
    command_panel::CommandPanelOutput,
    App,
};
use relm4::{ComponentSender, Receiver, RelmApp};
mod command;
mod gui;

static ARGS: OnceLock<Args> = OnceLock::new();

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 1234)]
    port: u16,

    #[arg(long)]
    cli: bool,
}

fn main() {
    let _ = ARGS.set(Args::parse());

    // Connects to the server
    // Repeatedly takes in commands following the CS265 DSL from the user
    // writes command to server
    // reads back the response from the server

    if ARGS.get().unwrap().cli {
        let _ = run_text_client();
    } else {
        let app = RelmApp::new("relm4.lsm.client");
        app.run::<App>(());
    }
}

fn run_text_client() -> io::Result<()> {
    let mut input_buf = String::new();
    let mut output_buf = Vec::new();

    let port = ARGS.get().unwrap().port;

    if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
        let mut read_half = BufReader::new(stream.try_clone()?);
        let mut write_half = BufWriter::new(stream);

        loop {
            // prompt
            print!("127.0.0.1:{}> ", port);
            std::io::stdout().flush()?;

            // read
            if std::io::stdin().read_line(&mut input_buf)? == 0 {
                break;
            }

            input_buf.pop(); // \n
            if let Some(command) = Command::from_input(&input_buf) {
                // send
                send_command(&mut write_half, &mut read_half, &command, &mut output_buf)?;

                // print
                println!("{}", unsafe { str::from_utf8_unchecked(&output_buf) });
            } else {
                println!("Invalid command...");
            }

            input_buf.clear();
        }
    } else {
        println!(
            "Could not connect to server at 127.0.0.1:{}: Connection refused",
            port
        );
    }

    Ok(())
}

struct DurationBuffer<const CAP: usize> {
    durations: Vec<f32>,
    command_types: Vec<CommandType>,
}

impl<const CAP: usize> DurationBuffer<CAP> {
    fn new() -> Self {
        Self {
            durations: Vec::with_capacity(CAP),
            command_types: Vec::with_capacity(CAP),
        }
    }

    fn push(&mut self, val: f32, command_type: CommandType, sender: &ComponentSender<ClientGui>) {
        self.durations.push(val);
        self.command_types.push(command_type);
        if self.durations.len() == CAP {
            self.send_to_gui(sender);
        }
    }

    fn send_to_gui(&mut self, sender: &ComponentSender<ClientGui>) {
        if self.durations.len() == 0 {
            return;
        }
        sender.input(ClientInput::NewData(
            self.durations.clone().into_boxed_slice(),
            self.command_types.clone().into_boxed_slice(),
        ));
        self.durations.clear();
        self.command_types.clear();
    }
}

fn run_gui_client(
    sender: ComponentSender<ClientGui>,
    receiver: Receiver<CommandPanelOutput>,
) -> io::Result<()> {
    let mut output_buf = Vec::new();
    let mut duration_buf = DurationBuffer::<1000>::new();

    let port = ARGS.get().unwrap().port;

    if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
        let mut read_half = BufReader::new(stream.try_clone()?);
        let mut write_half = BufWriter::new(stream);

        while let Some(cpo) = receiver.recv_sync() {
            match cpo {
                CommandPanelOutput::GeneratePuts { num_puts } => {
                    let _ = std::process::Command::new("./generator/generator")
                        .arg("--external-puts")
                        .arg("--puts")
                        .arg(num_puts.to_string())
                        .output();

                    let command = Command::LOAD {
                        file: "./0.dat".into(),
                    };

                    println!("Sending command {command:?}");
                    send_command(&mut write_half, &mut read_half, &command, &mut output_buf)?;
                }
                CommandPanelOutput::GenerateWorkload {
                    num_puts,
                    num_gets,
                    gets_skew,
                    gets_miss_ratio,
                    num_ranges,
                    num_deletes,
                } => {
                    let child = std::process::Command::new("./generator/generator")
                        .arg("--puts")
                        .arg(num_puts.to_string())
                        .arg("--gets")
                        .arg(num_gets.to_string())
                        .arg("--gets-skewness")
                        .arg(gets_skew.to_string())
                        .arg("--gets-misses-ratio")
                        .arg(gets_miss_ratio.to_string())
                        .arg("--ranges")
                        .arg(num_ranges.to_string())
                        .arg("--deletes")
                        .arg(num_deletes.to_string())
                        .stdout(Stdio::piped())
                        .spawn()
                        .unwrap();

                    let reader = BufReader::new(child.stdout.unwrap());

                    for line in reader.lines().map(|s| s.unwrap()) {
                        let command = Command::from_input(&line).unwrap();
                        duration_buf.push(
                            send_command(
                                &mut write_half,
                                &mut read_half,
                                &command,
                                &mut output_buf,
                            )?,
                            command.to_type().unwrap(),
                            &sender,
                        );
                    }
                }
                CommandPanelOutput::RawCommand { command } => {
                    println!("127.0.0.1:{}> {}", port, command);
                    if let Some(command) = Command::from_input(&command) {
                        if let Some(command_type) = command.to_type() {
                            duration_buf.push(
                                send_command(
                                    &mut write_half,
                                    &mut read_half,
                                    &command,
                                    &mut output_buf,
                                )?,
                                command_type,
                                &sender,
                            );
                            println!("{}", unsafe { str::from_utf8_unchecked(&output_buf) });
                        }
                    }
                }
            }

            duration_buf.send_to_gui(&sender);
            sender.input(ClientInput::CommandCompleted);
        }
    } else {
        println!(
            "Could not connect to server at 127.0.0.1:{}: Connection refused",
            port
        );
    }

    Ok(())
}

fn send_command<W: Write, R: BufRead>(
    write: &mut W,
    read: &mut R,
    command: &Command,
    output_buf: &mut Vec<u8>,
) -> io::Result<f32> {
    // send
    command.serialize(write)?;
    write.flush()?;
    output_buf.clear();

    let start = Instant::now();

    // recv
    read.read_until(0x00, output_buf)?;
    let elapsed = Instant::now().duration_since(start).as_secs_f32();

    if output_buf.is_empty() || !output_buf.ends_with(b"\0") {
        // connection was cut off
        println!("Could not read response from server: Connection dropped");
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    output_buf.pop(); // \0
    Ok(elapsed)
}
