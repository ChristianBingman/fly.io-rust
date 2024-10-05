use serde::Deserialize;
use std::error::Error;
use std::io;
use std::io::Write;
use std::sync::{Arc, Mutex};

mod node {
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    pub struct Node {
        initialized: bool,
        id: String,
        cur_id: u64,
        nodes: HashMap<String, u64>,            // List of all nodes
        logs: HashMap<String, (u64, Vec<u64>)>, // Map of the append only logs
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    pub struct Message {
        src: String,
        dest: String,
        body: Body,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    #[serde(rename_all = "snake_case")]
    #[serde(tag = "type")]
    enum Body {
        Init {
            msg_id: u64,
            node_id: String,
            node_ids: Vec<String>,
        },
        InitOk {
            msg_id: u64,
            in_reply_to: u64,
        },
        Send {
            msg_id: u64,
            key: String,
            msg: u64,
        },
        SendOk {
            msg_id: u64,
            in_reply_to: u64,
            offset: u64,
        },
        Poll {
            msg_id: u64,
            offsets: HashMap<String, u64>,
        },
        PollOk {
            msg_id: u64,
            in_reply_to: u64,
            msgs: HashMap<String, Vec<(u64, u64)>>,
        },
        CommitOffsets {
            msg_id: u64,
            offsets: HashMap<String, u64>,
        },
        CommitOffsetsOk {
            msg_id: u64,
            in_reply_to: u64,
        },
        ListCommittedOffsets {
            msg_id: u64,
            keys: Vec<String>,
        },
        ListCommittedOffsetsOk {
            msg_id: u64,
            in_reply_to: u64,
            offsets: HashMap<String, u64>,
        },
    }

    impl Node {
        pub fn new() -> Self {
            Node {
                initialized: false,
                id: String::default(),
                cur_id: 1,
                nodes: HashMap::new(),
                logs: HashMap::new(),
            }
        }

        pub fn handle_message(&mut self, message: Message) -> Vec<Message> {
            if !self.initialized {
                let Body::Init { .. } = message.body else {
                    panic!("Node received message before initialized!");
                };
            }
            let mut messages = Vec::new();
            let resp_body = self.handle_body(&message.body);
            if let Some(body) = resp_body {
                messages.push(Message {
                    src: message.dest,
                    dest: message.src,
                    body,
                });
                self.cur_id += 1;
            }

            messages
        }

        fn handle_body(&mut self, body: &Body) -> Option<Body> {
            Some(match body {
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
                    if self.initialized {
                        panic!("Node already initialized, but received another initialization message!");
                    }
                    self.id = node_id.clone();
                    self.nodes = node_ids
                        .iter()
                        .cloned()
                        .map(|node| (node, 0))
                        .collect::<HashMap<String, u64>>();
                    self.initialized = true;
                    Body::InitOk {
                        msg_id: self.cur_id,
                        in_reply_to: msg_id.clone(),
                    }
                }
                Body::Send { msg_id, key, msg } => {
                    let offset: u64;
                    if let Some(log) = self.logs.get_mut(key) {
                        log.1.push(msg.clone());
                        offset = (log.1.len() - 1) as u64;
                    } else {
                        self.logs.insert(key.clone(), (0, vec![msg.clone()]));
                        offset = 0;
                    }
                    Body::SendOk {
                        msg_id: self.cur_id,
                        in_reply_to: msg_id.clone(),
                        offset,
                    }
                }
                Body::Poll { msg_id, offsets } => {
                    let mut msgs = HashMap::new();
                    for (key, val) in offsets.iter() {
                        msgs.insert(
                            key.clone(),
                            vec![(
                                val.clone(),
                                self.logs
                                    .get(key)
                                    .unwrap()
                                    .1
                                    .get(val.clone() as usize)
                                    .unwrap()
                                    .clone(),
                            )],
                        );
                    }
                    Body::PollOk {
                        msg_id: self.cur_id,
                        in_reply_to: msg_id.clone(),
                        msgs,
                    }
                }
                Body::CommitOffsets { msg_id, offsets } => {
                    for (key, val) in offsets.iter() {
                        self.logs.get_mut(key).unwrap().0 = *val + 1;
                    }
                    Body::CommitOffsetsOk {
                        msg_id: self.cur_id,
                        in_reply_to: msg_id.clone(),
                    }
                }
                Body::ListCommittedOffsets { msg_id, keys } => {
                    let mut offsets = HashMap::new();
                    for key in keys {
                        offsets.insert(key.clone(), self.logs.get(key).unwrap().0);
                    }
                    Body::ListCommittedOffsetsOk {
                        msg_id: self.cur_id,
                        in_reply_to: msg_id.clone(),
                        offsets,
                    }
                }
                _ => unimplemented!(),
            })
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::SimpleLogger::new().env().init()?;
    let stdin = io::stdin().lock();
    let node = Arc::new(Mutex::new(node::Node::new()));

    let mut reader = serde_json::Deserializer::from_reader(stdin);

    loop {
        match node::Message::deserialize(&mut reader) {
            Ok(m) => {
                let node = Arc::clone(&node);
                tokio::spawn(async move {
                    let messages;
                    {
                        let mut node = node.lock().unwrap();
                        messages = node.handle_message(m);
                    }
                    for message in messages {
                        let mut stdout = io::stdout().lock();
                        serde_json::to_writer(&mut stdout, &message).unwrap();
                        stdout.write_all(b"\n").unwrap();
                    }
                });
            }
            Err(e) => {
                log::error!("Unable to parse: {}", e);
                continue;
            }
        }
    }
}
