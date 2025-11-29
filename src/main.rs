use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::RwLock;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use tracing::{info, error, warn};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID:AtomicUsize = AtomicUsize::new(1);

#[tokio::main]
async fn main() {
    let addr: String = "0.0.0.0:8080".to_string();
    let listener = TcpListener::bind(&addr).await.expect("Failed to bind");
    let clients = Arc::new(Mutex::new(Vec::new()));
    let names = Arc::new(Mutex::new(HashMap::<usize, String>::new()));
    let rooms = Arc::new(Mutex::new(HashMap::<usize, String>::new()));
    let room_history = Arc::new(Mutex::new(HashMap::<String, Vec<String>>::new()));

    loop {
        let ( stream, addrs ) = listener.accept().await.expect("Failed to accept connection");
        let clients = Arc::clone(&clients);
        let names = Arc::clone(&names);
        let rooms = Arc::clone(&rooms);
        let room_history = Arc::clone(&room_history);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            let ws_stream = accept_async(stream).await;
            let ( mut write, mut read ) = ws_stream.expect("REASON").split();
            let write = Arc::new(Mutex::new(write));
            
            {
            let mut guard = clients.lock().await;
            guard.push((id, Arc::clone(&write)));
            println!("Client {} joined from {}, total = {}", id, addrs, guard.len());
            }

            while let Some(message) = read.next().await {
                match message {
                    Ok(msg) => {
                        match msg {
                            Message::Text(text) => {
                                let trimmed = text.trim();

                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                    // println!("Parsed json from user {}: {:?}", id, json);
                                    // println!("Type field is: {:?}", json["type"]);

                                    if json["type"] == "image" {
                                        println!("Image from the user: {}", id);
                                    }

                                    let clients = clients.lock().await;
                                    for (cid, client_write) in clients.iter() {
                                        if *cid == id {
                                            continue;
                                        }

                                        let mut w = client_write.lock().await;
                                        w.send(Message::Text(text.clone())).await;
                                        println!("Image sent")
                                    }
                                    continue;
                                 }

                                if trimmed == "/who" {
                                    let map = names.lock().await;
                                    let client = clients.lock().await;
                                    
                                    // Collect labels for all users except the current user
                                    let mut labels = Vec::new();
                                    for (cid, _) in client.iter() {
                                        if *cid == id {
                                            continue; // Skip the current user
                                        }
                                        let label = map
                                            .get(cid)
                                            .cloned()
                                            .unwrap_or_else(|| format!("user{}", cid));
                                        labels.push(label);
                                    }
                                    drop(client);
                                    drop(map);

                                    let reply = if labels.is_empty() {
                                        "No one".to_string()
                                    } else {
                                        format!("Online: {}", labels.join(", "))
                                    };

                                    let mut w = write.lock().await;
                                    w.send(Message::Text(reply)).await;
                                    continue;
                                }

                                if trimmed == "/count" {
                                    let client = clients.lock().await;
                                    let count = client.len();
                                    drop(client);
                                    let mut w = write.lock().await;

                                    w.send(Message::Text(format!("Total active clients: {}", count))).await;
                                    continue;
                                }

                                if trimmed.starts_with("/name ") {
                                    let new_name = trimmed["/name ".len()..].trim();
                                    let mut rooms_map = rooms.lock().await;
                                    rooms_map.insert(id, "lobby".to_string());
                                    println!("user {} joined lobby", id);

                                    drop(rooms_map);

                                    if !new_name.is_empty() {
                                        let mut map = names.lock().await;
                                        map.insert(id, new_name.to_string());
                                    }

                                    // Send lobby history when joining
                                    let history_lock = room_history.lock().await;
                                    if let Some(messages) = history_lock.get("lobby") {
                                        let recent = &messages[messages.len().saturating_sub(50)..];
                                        for msg in recent {
                                            let mut w = write.lock().await;
                                            w.send(Message::Text(msg.clone())).await.unwrap();
                                        }
                                    }
                                    drop(history_lock);

                                    let mut w = write.lock().await;
                                    w.send(Message::Text(format!("name set to {}", new_name)))
                                        .await;
                                    
                                    continue;
                                }

                                if trimmed.starts_with("/dm ") {
                                    let text = trimmed["/dm ".len()..].trim();
                                    let first_char = text.chars().next();
                                    let mut parts = text.splitn(2, ' ');
                                    let target_name = parts.next().unwrap_or("").trim();
                                    let dm_message = parts.next().unwrap_or("").trim();

                                    let guard = clients.lock().await;
                                    let maps = names.lock().await;

                                    let target_cid_option = maps.iter()
                                        .find_map(|(&cid, client_name)| {
                                            if client_name == target_name {
                                                Some(cid)
                                            } else {
                                                None
                                            }
                                        });

                                    
                                    if let Some(target_cid) = target_cid_option {
                                        for (cid, client_write) in guard.iter() {
                                            if *cid == target_cid {
                                                let mut w = client_write.lock().await;

                                                println!("user {} target_cid {}", id, target_cid);

                                                let message_content = format!("(DM from {}) {}", maps.get(&id).unwrap_or(&format!("user{}", id)), dm_message);
                                                w.send(Message::Text(message_content)).await;
                                                break;
                                            }
                                        }
                                    } else {
                                        println!("no user found");
                                    }
                                    
                                    drop(guard);
                                    drop(maps);
                                    continue;
                                }

                                if trimmed == "/ping" {
                                    let mut w = write.lock().await;
                                    w.send(Message::Text(format!("pong"))).await;
                                    continue;
                                }

                                if trimmed.starts_with("/join ") {
                                    let room_name = trimmed["/join ".len()..].trim();
                                    if !room_name.is_empty() {
                                        let mut rooms_map = rooms.lock().await;
                                        rooms_map.insert(id, room_name.to_string());
                                        println!("user {} joined {} ", id, room_name);
                                        
                                        // Send confirmation message to update UI
                                        {
                                            let mut w = write.lock().await;
                                            w.send(Message::Text(format!("Joined room: {}", room_name))).await.unwrap();
                                        }
                                         
                                        let history_lock = room_history.lock().await;
                                        
                                        if let Some(messages) = history_lock.get(room_name) {
                                            let recent = &messages[messages.len().saturating_sub(50)..];
                                            for msg in recent {
                                                let mut w = write.lock().await;
                                                w.send(Message::Text(msg.clone())).await.unwrap();
                                            }
                                        }
                                    }
                                    continue;
                                }

                                if trimmed == "/get_rooms" {
                                    let rooms_lock = rooms.lock().await;
                                    let mut all_rooms = Vec::new();
                                    
                                    for room in rooms_lock.values() {
                                        all_rooms.push(room.clone());
                                    }

                                    let mut w = write.lock().await;
                                    w.send(Message::Text(format!("Active rooms: {:?}", all_rooms))).await.unwrap();
                                    continue;
                                }

                                let map = names.lock().await;
                                let label = map 
                                    .get(&id)
                                    .cloned()
                                    .unwrap_or_else(|| format!("user{}", id));
                                drop(map);

                                let guard = clients.lock().await;
                                let room_lock = rooms.lock().await;
                                let my_room = room_lock.get(&id).cloned().unwrap_or_else(|| "lobby".to_string());

                                // Always save message to history for the room (even if user is alone)
                                let formatted_msg = format!("{}: {}", label, trimmed);
                                {
                                    let mut history_lock = room_history.lock().await;
                                    let room_msgs = history_lock.entry(my_room.clone()).or_insert_with(Vec::new);
                                    room_msgs.push(formatted_msg.clone());
                                    if room_msgs.len() > 100 {
                                        room_msgs.remove(0);
                                    }
                                }

                                // Send message to other users in the same room
                                for (cid, client_write) in guard.iter() {
                                    if *cid == id {
                                        continue;
                                    }

                                    let other_room = room_lock.get(cid).cloned().unwrap_or_else(|| "lobby".to_string());

                                    if other_room == my_room {
                                        let mut w = client_write.lock().await;
                                        if let Err(e) = w.send(Message::Text(format!("{}: {}", label, trimmed))).await {
                                            println!("send error: {:?}", e);
                                        }
                                    }
                                }
                                drop(guard);
                                drop(room_lock);
                            },

                            other => {
                                println!("got other msg: {}", other)
                            }
                        }
                    }
                    Err( _) => println!("No msg"),
                }
            }
            let mut guard = clients.lock().await;
            if let Some(pos) = guard.iter().position(|(cid, _)| *cid == id) {
                guard.remove(pos);
            }
            println!("Client {} left, total clients = {}", id, guard.len());
        });
        println!("New client connected from {}", addrs);
    }
    println!("listner is serving on {}", listener.local_addr().unwrap());

}