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
        nodes: HashMap<String, u64>, // List of all nodes
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
        Add {
            msg_id: u64,
            delta: u64,
        },
        AddOk {
            in_reply_to: u64,
            msg_id: u64,
        },
        Read {
            msg_id: u64,
        },
        ReadOk {
            in_reply_to: u64,
            msg_id: u64,
            value: u64,
        },
        Gossip {
            msg_id: u64,
            value: u64,
            node: String,
        },
    }

    impl Node {
        pub fn new() -> Self {
            Node {
                initialized: false,
                id: String::default(),
                cur_id: 1,
                nodes: HashMap::new(),
            }
        }

        pub fn gossip(&mut self) -> Vec<Message> {
            let mut messages = Vec::new();
            if self.nodes.len() != 0 {
                for (node, value) in self.nodes.iter() {
                    for cnode in self.nodes.keys() {
                        messages.push(Message {
                            src: self.id.clone(),
                            dest: cnode.clone(),
                            body: Body::Gossip {
                                msg_id: self.cur_id,
                                value: *value,
                                node: node.clone(),
                            },
                        });
                        self.cur_id += 1;
                    }
                }
            }
            messages
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
                Body::Add { msg_id, delta } => {
                    let node = self.nodes.get_mut(&self.id).unwrap();
                    *node += delta;
                    Body::AddOk {
                        in_reply_to: msg_id.clone(),
                        msg_id: self.cur_id,
                    }
                }
                Body::Read { msg_id } => Body::ReadOk {
                    in_reply_to: msg_id.clone(),
                    msg_id: self.cur_id,
                    value: self.nodes.values().sum(),
                },
                Body::Gossip {
                    msg_id: _,
                    value,
                    node,
                } => {
                    log::debug!("Received gossip, updating local list");
                    let node = self.nodes.get_mut(node).unwrap();
                    if *node < *value {
                        *node = *value
                    }
                    return None;
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

    {
        let node = Arc::clone(&node);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let mut node = node.lock().unwrap();
                let messages = node.gossip();
                for message in messages {
                    let mut stdout = io::stdout().lock();
                    serde_json::to_writer(&mut stdout, &message).unwrap();
                    stdout.write_all(b"\n").unwrap();
                }
            }
        });
    }

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
