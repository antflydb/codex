use antfly_embedded::{Database, OpenOptions};
use std::sync::{Arc, Mutex};

#[test]
fn many_databases_in_one_process() {
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let threads: usize = std::env::var("STRESS_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let errors = Arc::clone(&errors);
            std::thread::Builder::new()
                .stack_size(antfly_embedded::MIN_THREAD_STACK_SIZE)
                .spawn(move || {
                    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
                    let db = match Database::create(dir.path().join("s.aflite"), &OpenOptions::new()) {
                        Ok(db) => db,
                        Err(e) => { errors.lock().unwrap_or_else(|p| p.into_inner()).push(format!("t{t} open: {e}")); return; }
                    };
                    for i in 0..200 {
                        let body = format!(r#"{{"inserts":{{"k:{t}:{i}":{{"search_text":"raft snapshot number {i}"}}}},"sync_level":"write"}}"#);
                        if let Err(e) = db.batch_json(body.as_bytes()) {
                            errors.lock().unwrap_or_else(|p| p.into_inner()).push(format!("t{t} batch {i}: {e}"));
                        }
                        if i % 50 == 0 {
                            if let Err(e) = db.search_json(br#"{"full_text_search":{"match":{"field":"search_text","text":"raft"}},"limit":5}"#) {
                                errors.lock().unwrap_or_else(|p| p.into_inner()).push(format!("t{t} search {i}: {e}"));
                            }
                        }
                    }
                    if let Err(e) = db.close() {
                        errors.lock().unwrap_or_else(|p| p.into_inner()).push(format!("t{t} close: {e}"));
                    }
                    drop(dir);
                })
                .unwrap_or_else(|e| panic!("{e}"))
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }
    let errors = errors.lock().unwrap_or_else(|p| p.into_inner());
    eprintln!(
        "ERRORS {}: {:?}",
        errors.len(),
        errors.iter().take(10).collect::<Vec<_>>()
    );
}
