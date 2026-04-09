use actix_web::{
    App, HttpResponse, HttpServer, Responder, get,
    web::{self, Data},
};
use game_garden_server::{
    reversi::{LobbyWrapper, game_matchmaking},
    routes::get_api_routes,
};

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let lobby = LobbyWrapper::new();
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(lobby.clone()))
            .service(hello)
            .service(game_matchmaking)
            // .route("/rooms/reversi", web::get().to(game_matchmaking))
            .service(get_api_routes())
    })
    .bind(("127.0.0.1", 8021))?
    .run()
    .await
}
