use actix_web::{App, HttpResponse, HttpServer, Responder, get, web};
use game_garden_server::{reversi::reversi, routes::get_api_routes};

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(move || {
        App::new()
            .service(hello)
            .route("/rooms/reversi", web::get().to(reversi))
            .service(get_api_routes())
    })
    .bind(("127.0.0.1", 8021))?
    .run()
    .await
}
