use actix_web::{App, HttpServer, web};
use game_garden_server::{reversi::reversi, routes::get_api_routes};

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    HttpServer::new(move || {
        App::new()
        .route("/rooms/reversi", web::get().to(reversi))
            .service(get_api_routes())
    })
    .bind(("127.0.0.1", 8020))?
    .run()
    .await
}