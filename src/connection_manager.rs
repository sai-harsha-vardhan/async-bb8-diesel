//! An async-safe connection pool for Diesel.

use crate::{Connection, ConnectionError};
use diesel::r2d2::{self, ManageConnection, R2D2Connection};
use std::sync::{Arc, Mutex};

/// A connection manager which implements [`bb8::ManageConnection`] to
/// integrate with bb8.
///
/// ```no_run
/// use async_bb8_diesel::AsyncRunQueryDsl;
/// use diesel::prelude::*;
/// use diesel::pg::PgConnection;
///
/// table! {
///     users (id) {
///         id -> Integer,
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     use users::dsl;
///
///     // Creates a Diesel-specific connection manager for bb8.
///     let mgr = async_bb8_diesel::ConnectionManager::<PgConnection>::new("localhost:1234");
///     let pool = bb8::Pool::builder().build(mgr).await.unwrap();
///
///     diesel::insert_into(dsl::users)
///         .values(dsl::id.eq(1337))
///         .execute_async(&*pool.get().await.unwrap())
///         .await
///         .unwrap();
/// }
/// ```
#[derive(Clone)]
pub struct ConnectionManager<T> {
    inner: Arc<Mutex<r2d2::ConnectionManager<T>>>,
    require_writer: bool,
}

impl<T: Send + 'static> ConnectionManager<T> {
    pub fn new<S: Into<String>>(database_url: S) -> Self {
        Self {
            inner: Arc::new(Mutex::new(r2d2::ConnectionManager::new(database_url))),
            require_writer: false,
        }
    }

    /// Marks connections produced by this manager as requiring the writable
    /// primary instance.
    ///
    /// When enabled (and the `postgres` feature is active), `is_valid` runs
    /// `SELECT pg_is_in_recovery()` instead of the default liveness check,
    /// and rejects the connection whenever it reports being a read replica.
    /// `bb8` treats a rejected connection as invalid, evicting it from the
    /// pool and establishing a new one in its place. This guards against a
    /// "writer" pool silently continuing to serve a former primary that has
    /// failed over to a standby (the DNS/endpoint now points elsewhere, but
    /// already-pooled connections would otherwise keep working - just against
    /// the wrong, read-only, instance).
    pub fn require_writer(mut self) -> Self {
        self.require_writer = true;
        self
    }

    async fn run_blocking<R, F>(&self, f: F) -> R
    where
        R: Send + 'static,
        F: Send + 'static + FnOnce(&r2d2::ConnectionManager<T>) -> R,
    {
        let cloned = self.inner.clone();
        tokio::task::spawn_blocking(move || f(&*cloned.lock().unwrap()))
            .await
            // Intentionally panic if the inner closure panics.
            .unwrap()
    }
}

#[cfg(feature = "postgres")]
mod recovery {
    use diesel::{
        connection::LoadConnection, pg::Pg, sql_types::Bool, Connection, QueryableByName,
        RunQueryDsl,
    };

    /// Reports whether a connection currently points at a read-only replica
    /// (e.g. a Postgres streaming standby) rather than the writable primary.
    ///
    /// Blanket-implemented for any Postgres-backed Diesel connection,
    /// including wrapper connections (e.g. instrumented ones) that still
    /// implement Diesel's `Connection`/`LoadConnection` traits against the
    /// `Pg` backend.
    pub trait RecoveryCheck {
        fn is_in_recovery(&mut self) -> Result<bool, diesel::result::Error>;
    }

    #[derive(QueryableByName)]
    struct IsInRecoveryRow {
        #[diesel(sql_type = Bool)]
        pg_is_in_recovery: bool,
    }

    impl<T> RecoveryCheck for T
    where
        T: Connection<Backend = Pg> + LoadConnection,
    {
        fn is_in_recovery(&mut self) -> Result<bool, diesel::result::Error> {
            diesel::sql_query("SELECT pg_is_in_recovery()")
                .get_result::<IsInRecoveryRow>(self)
                .map(|row| row.pg_is_in_recovery)
        }
    }
}

#[cfg(feature = "postgres")]
pub use recovery::RecoveryCheck;

#[cfg(feature = "postgres")]
impl<T> bb8::ManageConnection for ConnectionManager<T>
where
    T: R2D2Connection + recovery::RecoveryCheck + Send + 'static,
{
    type Connection = Connection<T>;
    type Error = ConnectionError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.run_blocking(|m| m.connect())
            .await
            .map(Connection::new)
            .map_err(ConnectionError::Connection)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let c = Connection(conn.0.clone());
        let require_writer = self.require_writer;
        self.run_blocking(move |m| {
            if require_writer {
                let in_recovery = recovery::RecoveryCheck::is_in_recovery(&mut *c.inner())?;
                if in_recovery {
                    return Err(ConnectionError::Query(
                        diesel::result::Error::QueryBuilderError(
                            "connection is a read-only replica; refusing to treat it as the writer"
                                .into(),
                        ),
                    ));
                }
            } else {
                m.is_valid(&mut *c.inner())?;
            }
            Ok(())
        })
        .await
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        // Diesel returns this value internally. We have no way of calling the
        // inner method without blocking as this method is not async, but `bb8`
        // indicates that this method is not mandatory.
        false
    }
}

#[cfg(not(feature = "postgres"))]
impl<T> bb8::ManageConnection for ConnectionManager<T>
where
    T: R2D2Connection + Send + 'static,
{
    type Connection = Connection<T>;
    type Error = ConnectionError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.run_blocking(|m| m.connect())
            .await
            .map(Connection::new)
            .map_err(ConnectionError::Connection)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let c = Connection(conn.0.clone());
        self.run_blocking(move |m| {
            m.is_valid(&mut *c.inner())?;
            Ok(())
        })
        .await
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        // Diesel returns this value internally. We have no way of calling the
        // inner method without blocking as this method is not async, but `bb8`
        // indicates that this method is not mandatory.
        false
    }
}
