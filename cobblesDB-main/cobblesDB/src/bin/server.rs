use futures_util::{StreamExt, SinkExt};
use cobblesDB::compact::CompactionOptions;
use serde::{Deserialize, Serialize};

use tempfile::tempdir;

use std::path::PathBuf;
use std::str::from_utf8;
use std::{collections::HashMap, sync::Arc};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio::sync::Mutex;
use cobblesDB::lsm_storage::CobblesDB;
use cobblesDB::lsm_storage::LsmStorageOptions;
mod wrapper;
use wrapper::cobblesDB_wrapper;
use cobblesDB_wrapper::compact::{
    LeveledCompactionController, LeveledCompactionOptions, SimpleLeveledCompactionController,
    SimpleLeveledCompactionOptions, TieredCompactionController, TieredCompactionOptions,
};


#[derive(Serialize, Deserialize, Debug,Clone)]
struct QueryData {
    key: Option<String>,
    value: Option<String>,
    start: Option<String>,
    end: Option<String>,
}

#[derive(Deserialize, Debug,Clone)]
struct Query {
    id: u64,
    query: String,
    data: QueryData,
}

#[derive(Serialize, Debug, Clone)]
struct ResponseData {
    key: String,
    value: String,
}

#[derive(Serialize, Debug,Clone)]
struct Response {
    id: u64,
    queryData: QueryData,
    outputData: Vec<ResponseData>,
    query: String,
    success: String,
    message: Option<String>,
}

#[tokio::main]
async fn main() {
    let dir : PathBuf = "cobbles.db".into();

    let lsm = CobblesDB::open(
        dir,
        LsmStorageOptions::default_option(CompactionOptions::Leveled(
            LeveledCompactionOptions {
                level0_file_num_compaction_trigger: 2,
                level_size_multiplier: 2,
                base_level_size_mb: 2,
                max_levels: 4,
            },
        )),
    ).unwrap();

    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(&addr).await.expect("Failed to bind");
    println!("Listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        let lsm = Arc::clone(&lsm);
        tokio::spawn(async move {
            let ws_stream = accept_async(stream).await.expect("Error during WebSocket handshake");
            println!("New client connected!");

            let (mut ws_sender, mut ws_receiver) = ws_stream.split();

            while let Some(Ok(message)) = ws_receiver.next().await {
                if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
                    println!("Message received: {}", text);

                    let query: Query = serde_json::from_str(&text).expect("Invalid JSON");

                    match query.query.as_str() {
                        "PUT" => {
                            if let Some(key) = &query.data.key {
                                if let Some(value) = &query.data.value {
                                    let ret = lsm.put(key.as_bytes(), value.as_bytes());
                                    let mut success : String;
                                    let mut message;
                                    match ret {
                                        Ok(_) => {
                                            success = "Success".to_string();
                                            message = Some(format!("PUT {} successed!", key));
                                        }
                                        Err(e) => {
                                            success = "Failed".to_string();
                                            message = Some(format!("PUT {} failed!", key));
                                        }
                                    }

                                    let response = Response {
                                        id: query.id,
                                        queryData: query.data.clone(),
                                        outputData: vec![ResponseData {
                                            key: key.clone(),
                                            value: value.clone(),
                                        }],
                                        query: "PUT".to_string(),
                                        success,
                                        message,
                                    };
                                    let response_str = serde_json::to_string(&response).unwrap();
                                    ws_sender.send(tokio_tungstenite::tungstenite::Message::Text(response_str)).await.unwrap();
                                }
                            }
                        }
                        "GET" => {
                            if let Some(key) = &query.data.key {
                                let ret = lsm.get(key.as_bytes());
                                let mut success : String;
                                let mut value =None;
                                let mut output_data = Vec::new();
                                let mut message;
                                match ret {
                                    Ok(Some(ret)) => {
                                        value = Some(from_utf8(&ret).unwrap().to_string());
                                        success = "Success".to_string();
                                        output_data.push(ResponseData {
                                            key: key.clone(),
                                            value: value.unwrap().clone(),
                                        });
                                        message = Some(format!("GET {} successed!", key));
                                    }
                                    Ok(None) => {
                                        success = "Failed".to_string();
                                        message = Some(format!("Key : {} not exist!", key));
                                    }
                                    Err(e) => {
                                        success = "Failed".to_string();
                                        message = Some(format!("GET {} failed!", key));
                                    }
                                }

                                let response = Response {
                                    id: query.id,
                                    queryData: query.data.clone(),
                                    outputData: output_data,
                                    query: "GET".to_string(),
                                    success,
                                    message: None,
                                };
                                let response_str = serde_json::to_string(&response).unwrap();
                                ws_sender.send(tokio_tungstenite::tungstenite::Message::Text(response_str)).await.unwrap();
                            }
                        }
                        "RANGE" => {
                            if let Some(start) = &query.data.start {
                                if let Some(end) = &query.data.end {
                                    let start= start.parse::<i64>().expect("Not a valid number");
                                    let end = end.parse::<i64>().expect("Not a valid number");
                                    let mut output_data = Vec::new();

                                    for i in start..=end {
                                        let key = i.to_string();
                                        let value = i.to_string();
                                        let ret = lsm.put(key.as_bytes(), value.as_bytes());
                                        output_data.push(ResponseData {
                                            key: key.clone(),
                                            value: value.clone(),
                                        });
                                }
                                let response = Response {
                                    id: query.id,
                                    queryData: query.data.clone(),
                                    outputData: output_data,
                                    query: "RANGE".to_string(),
                                    success: "Success".to_string(),
                                    message: None,
                                };
                                let response_str = serde_json::to_string(&response).unwrap();
                                ws_sender.send(tokio_tungstenite::tungstenite::Message::Text(response_str)).await.unwrap();
                            }
                        }
                    }
                        "DELETE" => {
                            if let Some(key) = &query.data.key {
                                let ret = lsm.delete(key.as_bytes());
                                let mut success : String;
                                let mut message;
                                let mut output_data = Vec::new();
                                match ret {
                                    Ok(_) => {
                                        success = "Success".to_string();
                                        message = Some(format!("Delete {} successed!", key));
                                        output_data.push(ResponseData {
                                            key: key.clone(),
                                            value: String::new(),
                                        });
                                    }
                                    Err(e) => {
                                        success = "Failed".to_string();
                                        message = Some(format!("Delete {} failed!", key));
                                    }
                                }

                                let response = Response {
                                    id: query.id,
                                    queryData: query.data.clone(),
                                    outputData: output_data,
                                    query: "DELETE".to_string(),
                                    success,
                                    message,
                                };
                                let response_str = serde_json::to_string(&response).unwrap();
                                ws_sender.send(tokio_tungstenite::tungstenite::Message::Text(response_str)).await.unwrap();
                            }
                        }
                        _ => {
                            println!("Unknown query type");
                        }
                    }
                }
            }
        });
    }
}
