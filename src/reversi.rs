use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use actix_web::{
    Error, HttpRequest, HttpResponse, get, rt,
    web::{self, Data},
};
use actix_ws::{AggregatedMessage, CloseCode, CloseReason};
use futures_util::{StreamExt as _, lock::Mutex};
use tokio::{
    sync::mpsc::{Receiver, Sender, channel},
    time,
};

pub struct Lobbies {
    /// queued up users for various games
    pub queued: HashMap<String, Connection>,
    pub next_id: AtomicU64,
}

impl Lobbies {
    pub fn new() -> Self {
        Self {
            queued: HashMap::new(),
            next_id: AtomicU64::new(0),
        }
    }
}

#[derive(Clone)]
pub struct LobbyWrapper {
    pub lobbies: Arc<Mutex<Lobbies>>,
}

pub struct Connection {
    pub id: u64,
    pub to_client: Sender<String>,
    pub from_client: Receiver<String>,
}

impl LobbyWrapper {
    pub async fn get_or_queue(
        &self,
        stream: Connection,
        game_name: String,
    ) -> Option<(Connection, Connection)> {
        let mut lock = self.lobbies.lock().await;
        match lock.queued.remove(&game_name) {
            Some(opponent) => {
                println!("removing {}", game_name);
                Some((opponent, stream))
            },
            None => {
                println!("inserting {}", game_name);
                lock.queued.insert(game_name, stream);
                dbg!(&lock.queued.keys());
                None
            }
        }
    }
    pub async fn remove_if_queued(&self, con_id: u64, game_name: &String) {
        let mut lock = self.lobbies.lock().await;
        match lock.queued.get(game_name) {
            Some(c) => {
                if c.id == con_id {
                    println!("{} disconnected before matched", game_name);
                    lock.queued.remove(game_name);
                }
            }
            None => {}
        }
    }
    pub async fn get_id(&self) -> u64 {
        let lock = self.lobbies.lock().await;
        lock.next_id.fetch_add(1, Ordering::SeqCst)
    }
    pub fn new() -> Self {
        Self {
            lobbies: Arc::new(Mutex::new(Lobbies::new())),
        }
    }
}

#[get("/rooms/{game}")]
pub async fn game_matchmaking(
    req: HttpRequest,
    stream: web::Payload,
    game_name: web::Path<String>,
    lobbies: Data<LobbyWrapper>,
) -> Result<HttpResponse, Error> {
    let (res, mut session, stream) = actix_ws::handle(&req, stream)?;

    let mut stream = stream
        .aggregate_continuations()
        // aggregate continuation frames up to 1MiB
        .max_continuation_size(2_usize.pow(20));

    let (to_client, mut to_client_receiver) = channel::<String>(100);
    let (from_client, from_client_receiver) = channel::<String>(100);
    let id = lobbies.get_id().await;

    let conn = Connection {
        id,
        to_client,
        from_client: from_client_receiver,
    };

    let lobbies2 = lobbies.clone();
    let game_name2 = game_name.clone();

    // start task but don't wait for it
    rt::spawn(async move {
        let mut ping_timer = time::interval(Duration::from_secs(5));
        ping_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                // send a ping packet every couple seconds so cloudflare doesn't kill it
                _tick = ping_timer.tick() => {
                    session.ping(b"").await.unwrap();
                }

                maybe_msg = stream.next() => {
                match maybe_msg {
                    Some(Ok(AggregatedMessage::Text(text))) => {
                        let _ = from_client.send(text.to_string()).await;

                        // session.text(text).await.unwrap();
                    }

                    Some(Ok(AggregatedMessage::Binary(bin))) => {
                        session.binary(bin).await.unwrap();
                    }

                    Some(Ok(AggregatedMessage::Ping(msg))) => {
                        session.pong(&msg).await.unwrap();
                    }

                    Some(Ok(AggregatedMessage::Pong(_))) => {}

                    Some(Ok(AggregatedMessage::Close(r))) => {
                        // if let Some(ref rea) = r {
                        //     println!("ws closed with {:?}", rea);
                        // }
                        lobbies2.remove_if_queued(id, &game_name2).await;
                        session.close(r).await.unwrap();
                        break;
                    }

                    Some(Err(err)) => {
                        println!("ws error: {}", err);
                    }

                    None => break, // websocket closed
                }
            }

            maybe_msg = to_client_receiver.recv() => {
                match maybe_msg {
                    Some(msg) => {
                        session.text(msg).await.unwrap();
                    }
                    None => {
                        session.close(Some(CloseReason::from(
                            (CloseCode::Away, "Other player disconnected.")
                        ))).await.unwrap();
                        break;
                    }, // sender dropped
                }
            }

            }
        }
    });

    if let Some((mut player1, mut player2)) =
        lobbies.get_or_queue(conn, game_name.into_inner()).await
    {
        println!("matchmade");
        rt::spawn(async move {
            player1.to_client.send("1".to_string()).await.unwrap();
            player2.to_client.send("2".to_string()).await.unwrap();
            loop {
                tokio::select! {

                maybe_msg = player1.from_client.recv() => {
                    match maybe_msg {
                        Some(msg) => {
                            player2.to_client.send(msg).await.unwrap();
                        }
                        None => break, // sender dropped
                    }
                }

                maybe_msg = player2.from_client.recv() => {
                    match maybe_msg {
                        Some(msg) => {
                            player1.to_client.send(msg).await.unwrap();
                        }
                        None => break, // sender dropped
                    }
                }

                }
            }
        });
    }

    // respond immediately with response connected to WS session
    Ok(res)
}
