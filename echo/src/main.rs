use serde::{Deserialize, Serialize};
use std::error::Error;
use std::io;
use std::io::Write;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Body {
    Echo {
        msg_id: usize,
        echo: String,
    },
    EchoOk {
        msg_id: usize,
        in_reply_to: usize,
        echo: String,
    },
    Init {
        msg_id: usize,
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {
        msg_id: usize,
        in_reply_to: usize,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

fn handle_body(body: Body) -> Body {
    match body {
        Body::Echo { msg_id, echo } => Body::EchoOk {
            msg_id,
            in_reply_to: msg_id,
            echo,
        },
        Body::Init {
            msg_id,
            node_id,
            node_ids,
        } => {
            log::debug!(
                "Received init with id: {}, node_id: {}, and node_ids: {:?}",
                msg_id,
                node_id,
                node_ids
            );
            Body::InitOk {
                msg_id,
                in_reply_to: msg_id,
            }
        }
        Body::EchoOk { .. } => body, // We shouldn't be receiving these
        Body::InitOk { .. } => body, // We shouldn't be receiving these
    }
}

fn handle_message(message: Message) -> Message {
    Message {
        src: message.dest,
        dest: message.src,
        body: handle_body(message.body),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::SimpleLogger::new().env().init()?;
    let stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    let mut reader = serde_json::Deserializer::from_reader(stdin);
    loop {
        match Message::deserialize(&mut reader) {
            Ok(m) => {
                serde_json::to_writer(&mut stdout, &handle_message(m))?;
                stdout.write_all(b"\n")?;
            }
            Err(e) => {
                log::error!("Unable to parse: {}", e);
                continue;
            }
        }
    }
}
