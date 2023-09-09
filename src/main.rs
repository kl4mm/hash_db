use hash_db::serverv2::server;

#[tokio::main]
async fn main() {
    server::run().await
}
