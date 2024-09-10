use serde::Deserialize;
use std::error::Error;
use std::io;
use std::io::Write;

mod node {
    use serde::{Deserialize, Serialize};
    use std::collections::{HashMap, HashSet};

    pub struct Node {
        initialized: bool,
        id: String,
        cur_id: u64,
        broadcast_messages: HashSet<usize>,
        peers: Vec<String>,
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
        Echo {
            msg_id: u64,
            echo: String,
        },
        EchoOk {
            msg_id: u64,
            in_reply_to: u64,
            echo: String,
        },
        Init {
            msg_id: u64,
            node_id: String,
            node_ids: Vec<String>,
        },
        InitOk {
            msg_id: u64,
            in_reply_to: u64,
        },
        Generate {
            msg_id: u64,
        },
        GenerateOk {
            id: uuid::Uuid,
            in_reply_to: u64,
            msg_id: u64,
        },
        Broadcast {
            msg_id: u64,
            message: usize,
        },
        BroadcastOk {
            in_reply_to: u64,
            msg_id: u64,
        },
        Read {
            msg_id: u64,
        },
        ReadOk {
            msg_id: u64,
            in_reply_to: u64,
            messages: Vec<usize>,
        },
        Topology {
            msg_id: u64,
            topology: HashMap<String, Vec<String>>,
        },
        TopologyOk {
            msg_id: u64,
            in_reply_to: u64,
        },
    }

    impl Node {
        pub fn new() -> Self {
            Node {
                initialized: false,
                id: String::default(),
                cur_id: 0,
                broadcast_messages: HashSet::new(),
                peers: Vec::new(),
            }
        }

        pub fn handle_message(&mut self, message: Message) -> Vec<Message> {
            self.cur_id += 1;
            if !self.initialized {
                let Body::Init { .. } = message.body else {
                    panic!("Node received message before initialized!");
                };
            }
            let mut messages = Vec::new();
            if let Body::Broadcast { msg_id: _, message } = &message.body {
                if self.broadcast_messages.get(&message).is_none() {
                    log::debug!(
                        "Unable to find {}, broadcasting to peers {:#?}",
                        &message,
                        self.peers
                    );
                    // rebroadcast
                    for node in &self.peers {
                        messages.push(Message {
                            src: self.id.clone(),
                            dest: node.to_string(),
                            body: Body::Broadcast {
                                msg_id: self.cur_id,
                                message: message.clone(),
                            },
                        });
                        self.cur_id += 1;
                    }
                }
            }
            if let Body::ReadOk {
                msg_id: _,
                in_reply_to: _,
                messages,
            } = &message.body
            {
                self.broadcast_messages.extend(messages.iter().cloned());
                return vec![];
            }
            if let Body::BroadcastOk { .. } = &message.body {
                return vec![];
            }
            let resp_body = self.handle_body(&message.body);

            messages.insert(
                0,
                Message {
                    src: message.dest,
                    dest: message.src,
                    body: resp_body,
                },
            );
            messages
        }

        fn handle_body(&mut self, body: &Body) -> Body {
            match body {
                Body::Echo { msg_id, echo } => Body::EchoOk {
                    msg_id: self.cur_id,
                    in_reply_to: msg_id.clone(),
                    echo: echo.clone(),
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
                    if self.initialized {
                        panic!("Node already initialized, but received another initialization message!");
                    }
                    self.id = node_id.clone();
                    self.initialized = true;
                    Body::InitOk {
                        msg_id: self.cur_id,
                        in_reply_to: msg_id.clone(),
                    }
                }
                Body::Generate { msg_id } => Body::GenerateOk {
                    id: uuid::Uuid::new_v4(),
                    msg_id: self.cur_id,
                    in_reply_to: msg_id.clone(),
                },
                Body::Broadcast { msg_id, message } => {
                    log::debug!("Received broadcast: {}", message);
                    self.broadcast_messages.insert(message.clone());
                    Body::BroadcastOk {
                        in_reply_to: msg_id.clone(),
                        msg_id: self.cur_id,
                    }
                }
                Body::Read { msg_id } => Body::ReadOk {
                    in_reply_to: msg_id.clone(),
                    msg_id: self.cur_id,
                    messages: self.broadcast_messages.clone().into_iter().collect(),
                },
                Body::Topology { msg_id, topology } => {
                    log::debug!("Received topology {:#?}. Updating peers...", topology);
                    match topology.get(&self.id) {
                        Some(peers) => self.peers = peers.clone(),
                        None => log::warn!(
                            "Received topology {:?} that didn't contain our node!",
                            topology
                        ),
                    }
                    Body::TopologyOk {
                        msg_id: self.cur_id,
                        in_reply_to: msg_id.clone(),
                    }
                }
                _ => unimplemented!(),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_create_node() {
            let node = Node::new();
            assert_eq!(node.initialized, false);
        }

        #[test]
        #[should_panic]
        fn test_uninitialized_node() {
            let mut node = Node::new();
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Echo {
                    msg_id: 1,
                    echo: "Hello Fly.io".to_string(),
                },
            });
        }

        #[test]
        fn test_init_node() {
            let mut node = Node::new();
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Init {
                    msg_id: 1,
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            });

            assert_eq!(node.initialized, true);
            assert_eq!(node.id, "n1");
        }

        #[test]
        fn test_echo() {
            let mut node = Node::new();
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Init {
                    msg_id: 1,
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            });

            assert_eq!(
                node.handle_message(Message {
                    src: "c1".into(),
                    dest: "n1".into(),
                    body: Body::Echo {
                        msg_id: 1,
                        echo: "Hello fly.io".into(),
                    }
                }),
                vec![Message {
                    src: "n1".into(),
                    dest: "c1".into(),
                    body: Body::EchoOk {
                        msg_id: 2,
                        in_reply_to: 1,
                        echo: "Hello fly.io".into(),
                    }
                }]
            )
        }

        #[test]
        fn test_increasing_message_id() {
            let mut node = Node::new();
            assert_eq!(node.cur_id, 0);
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Init {
                    msg_id: 1,
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            });
            assert_eq!(node.cur_id, 1);
        }

        #[test]
        fn test_unique_id_generation() {
            let mut node = Node::new();
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Init {
                    msg_id: 1,
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            });
            let Body::GenerateOk {
                id, in_reply_to, ..
            } = node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Generate { msg_id: 1 },
            })[0]
                .body
            else {
                panic!("Generate didn't response with generate_ok");
            };

            assert_eq!(in_reply_to, 1);
            assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
        }

        #[test]
        fn test_broadcast_receive() {
            let mut node = Node::new();
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Init {
                    msg_id: 1,
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            });

            let Body::BroadcastOk { .. } = node.handle_body(&Body::Broadcast {
                msg_id: 1,
                message: 1000,
            }) else {
                panic!("Didn't receive broadcast_ok after sending broadcast message!")
            };

            assert_eq!(
                node.broadcast_messages.into_iter().collect::<Vec<usize>>(),
                vec![1000]
            );
        }

        #[test]
        fn test_duplicates_ignored() {
            let mut node = Node::new();
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Init {
                    msg_id: 1,
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            });

            let Body::BroadcastOk { .. } = node.handle_body(&Body::Broadcast {
                msg_id: 1,
                message: 1000,
            }) else {
                panic!("Didn't receive broadcast_ok after sending broadcast message!")
            };

            let Body::BroadcastOk { .. } = node.handle_body(&Body::Broadcast {
                msg_id: 2,
                message: 1000,
            }) else {
                panic!("Didn't receive broadcast_ok after sending broadcast message!")
            };

            assert_eq!(
                node.broadcast_messages.into_iter().collect::<Vec<usize>>(),
                vec![1000]
            );
        }

        #[test]
        fn test_broadcast_read() {
            let mut node = Node::new();
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Init {
                    msg_id: 1,
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            });

            let Body::BroadcastOk { .. } = node.handle_body(&Body::Broadcast {
                msg_id: 1,
                message: 1000,
            }) else {
                panic!("Didn't receive broadcast_ok after sending broadcast message!");
            };

            let Body::ReadOk { messages, .. } = node.handle_body(&Body::Read { msg_id: 1 }) else {
                panic!("Didn't receive read_ok after sending read message!");
            };

            assert_eq!(messages, vec![1000]);
        }

        #[test]
        fn test_receive_topology() {
            // We don't care about the topology yet
            let mut node = Node::new();
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Init {
                    msg_id: 1,
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            });

            let Body::TopologyOk { .. } = node.handle_body(&Body::Topology {
                msg_id: 1,
                topology: HashMap::new(),
            }) else {
                panic!("didn't receive topology_ok after sending topology message!");
            };
        }

        #[test]
        fn test_update_peer_list() {
            let mut node = Node::new();
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Init {
                    msg_id: 1,
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            });

            let mut topo: HashMap<String, Vec<String>> = HashMap::new();
            topo.insert("n1".into(), vec!["n2".into()]);

            let Body::TopologyOk { .. } = node.handle_body(&Body::Topology {
                msg_id: 1,
                topology: topo,
            }) else {
                panic!("didn't receive topology_ok after sending topology message!");
            };

            assert_eq!(node.peers, vec!["n2".to_string()]);
        }

        #[test]
        fn test_broadcast_to_peers() {
            let mut node = Node::new();
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Init {
                    msg_id: 1,
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            });

            let mut topo: HashMap<String, Vec<String>> = HashMap::new();
            topo.insert("n1".into(), vec!["n2".into()]);

            let Body::TopologyOk { .. } = node.handle_body(&Body::Topology {
                msg_id: 2,
                topology: topo,
            }) else {
                panic!("didn't receive topology_ok after sending topology message!");
            };

            let messages = node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Broadcast {
                    message: 2,
                    msg_id: 3,
                },
            });

            assert_eq!(
                messages[0],
                Message {
                    src: "n1".into(),
                    dest: "n2".into(),
                    body: Body::Broadcast {
                        message: 2,
                        msg_id: 2,
                    }
                }
            );

            assert_eq!(
                messages[1],
                Message {
                    src: "n1".into(),
                    dest: "c1".into(),
                    body: Body::BroadcastOk {
                        msg_id: 3,
                        in_reply_to: 3
                    }
                }
            );
        }

        #[test]
        fn test_broadcast_not_sent_if_key_already_exists() {
            let mut node = Node::new();
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Init {
                    msg_id: 1,
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            });

            let mut topo: HashMap<String, Vec<String>> = HashMap::new();
            topo.insert("n1".into(), vec!["n2".into()]);

            let Body::TopologyOk { .. } = node.handle_body(&Body::Topology {
                msg_id: 2,
                topology: topo,
            }) else {
                panic!("didn't receive topology_ok after sending topology message!");
            };

            let messages = node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Broadcast {
                    message: 2,
                    msg_id: 3,
                },
            });

            assert_eq!(
                messages[0],
                Message {
                    src: "n1".into(),
                    dest: "n2".into(),
                    body: Body::Broadcast {
                        message: 2,
                        msg_id: 2,
                    }
                }
            );

            assert_eq!(
                messages[1],
                Message {
                    src: "n1".into(),
                    dest: "c1".into(),
                    body: Body::BroadcastOk {
                        msg_id: 3,
                        in_reply_to: 3
                    }
                }
            );

            let messages = node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Broadcast {
                    message: 2,
                    msg_id: 4,
                },
            });

            assert_eq!(
                messages[0],
                Message {
                    src: "n1".into(),
                    dest: "c1".into(),
                    body: Body::BroadcastOk {
                        msg_id: 4,
                        in_reply_to: 4
                    }
                }
            );
        }

        #[test]
        fn test_read_ok_merges_broadcast_messages() {
            let mut node = Node::new();
            node.handle_message(Message {
                src: "c1".into(),
                dest: "n1".into(),
                body: Body::Init {
                    msg_id: 1,
                    node_id: "n1".into(),
                    node_ids: vec!["n1".into()],
                },
            });

            node.broadcast_messages.insert(1000);

            node.handle_message(Message {
                src: "n2".into(),
                dest: "n1".into(),
                body: Body::ReadOk {
                    msg_id: 1,
                    in_reply_to: 2,
                    messages: vec![2, 1000],
                },
            });

            assert_eq!(
                node.broadcast_messages.into_iter().collect::<Vec<usize>>(),
                vec![2, 1000]
            );
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::SimpleLogger::new().env().init()?;
    let stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();
    let mut node = node::Node::new();

    let mut reader = serde_json::Deserializer::from_reader(stdin);
    loop {
        match node::Message::deserialize(&mut reader) {
            Ok(m) => {
                let messages = node.handle_message(m);
                for message in messages {
                    serde_json::to_writer(&mut stdout, &message)?;
                    stdout.write_all(b"\n")?;
                }
            }
            Err(e) => {
                log::error!("Unable to parse: {}", e);
                continue;
            }
        }
    }
}
