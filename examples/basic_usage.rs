use std::path::PathBuf;
use tegdb::Engine;

fn main() {
    let path = PathBuf::from("test.db");
    let mut engine = Engine::new(path.clone());

    // Set a value
    let key = b"key";
    let value = b"value";
    engine.set(key, value.to_vec());

    // Get a value
    let get_value = engine.get(key);
    println!("Got value: {}", String::from_utf8_lossy(&get_value));

    // Delete a value
    engine.del(key);

    // Scan for values
    let values = engine.scan(b"a".to_vec()..b"z".to_vec());
    for (key, value) in values {
        println!(
            "Got key: {}, value: {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    // Execute SQL queries
    let select_query = "SELECT value FROM key";
    let insert_query = "INSERT INTO key VALUES (key, value)";
    let update_query = "UPDATE key SET value = value";
    let delete_query = "DELETE FROM key";

    match engine.execute_sql(select_query) {
        Ok(result) => println!("{}", result),
        Err(err) => println!("Error: {}", err),
    }

    match engine.execute_sql(insert_query) {
        Ok(result) => println!("{}", result),
        Err(err) => println!("Error: {}", err),
    }

    match engine.execute_sql(update_query) {
        Ok(result) => println!("{}", result),
        Err(err) => println!("Error: {}", err),
    }

    match engine.execute_sql(delete_query) {
        Ok(result) => println!("{}", result),
        Err(err) => println!("Error: {}", err),
    }

    // Clean up
    drop(engine);
}
