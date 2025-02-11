use mini_redis::clients::Client;

#[tokio::main]
async fn main() {
    let mut client = Client::connect("localhost:6379").await.unwrap();

    client.set("ninja", "subzero".into()).await.unwrap();

    // getting the value immediately works
    let val = client.get("ninja").await.unwrap().unwrap();
    assert_eq!(val, "subzero");
}