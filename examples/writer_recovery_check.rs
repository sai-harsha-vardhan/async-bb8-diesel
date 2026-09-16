//! Manual local verification for `ConnectionManager::require_writer`.
//!
//! Run against a real Postgres primary + streaming standby:
//!
//! ```sh
//! cargo run --example writer_recovery_check -- \
//!   "postgresql://postgres@127.0.0.1:55432/postgres" \
//!   "postgresql://postgres@127.0.0.1:55433/postgres"
//! ```

use async_bb8_diesel::{AsyncSimpleConnection, ConnectionManager};
use bb8::ManageConnection;
use diesel::pg::PgConnection;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let writer_url = args.next().expect("expected writer url as first arg");
    let reader_url = args.next().expect("expected reader url as second arg");

    // Sanity: plain (non-writer-only) manager should validate both instances.
    for (label, url) in [("writer", &writer_url), ("reader", &reader_url)] {
        let manager = ConnectionManager::<PgConnection>::new(url.clone());
        let mut conn = manager.connect().await.expect("connect failed");
        manager
            .is_valid(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("plain is_valid unexpectedly failed for {}: {}", label, e));
        println!("[ok] plain is_valid passes for {} ({})", label, url);
    }

    // require_writer(): writer instance must pass, reader instance must fail.
    let writer_manager =
        ConnectionManager::<PgConnection>::new(writer_url.clone()).require_writer();
    let mut writer_conn = writer_manager.connect().await.expect("connect failed");
    writer_manager
        .is_valid(&mut writer_conn)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "require_writer() is_valid unexpectedly failed for writer: {}",
                e
            )
        });
    println!(
        "[ok] require_writer() is_valid passes for the writer ({})",
        writer_url
    );

    let reader_manager =
        ConnectionManager::<PgConnection>::new(reader_url.clone()).require_writer();
    let mut reader_conn = reader_manager.connect().await.expect("connect failed");
    match reader_manager.is_valid(&mut reader_conn).await {
        Ok(()) => panic!("require_writer() is_valid unexpectedly succeeded for the reader"),
        Err(e) => println!(
            "[ok] require_writer() is_valid correctly rejects the reader ({}): {}",
            reader_url, e
        ),
    }

    // Also confirm a full bb8 pool checkout evicts+refuses a reader-backed
    // writer manager (test_on_check_out is on by default).
    let reader_pool = bb8::Pool::builder()
        .connection_timeout(std::time::Duration::from_secs(2))
        .build(ConnectionManager::<PgConnection>::new(reader_url.clone()).require_writer())
        .await
        .expect("pool build should still succeed (validity is checked on checkout)");
    let pool_result = reader_pool.get().await;
    assert!(
        pool_result.is_err(),
        "expected pool checkout against a reader to fail validity"
    );
    println!("[ok] bb8 pool checkout against the reader fails as expected");

    let pool = bb8::Pool::builder()
        .build(ConnectionManager::<PgConnection>::new(writer_url.clone()).require_writer())
        .await
        .expect("pool build failed");
    let conn = pool
        .get()
        .await
        .expect("pool checkout against the writer should succeed");
    conn.batch_execute_async("SELECT 1")
        .await
        .expect("query failed");
    println!("[ok] bb8 pool checkout against the writer succeeds and can run a query");

    println!("\nAll checks passed.");
}
