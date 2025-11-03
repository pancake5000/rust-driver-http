use anyhow::Context;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::policies::load_balancing;
use scylla::policies::retry::DefaultRetryPolicy;
use scylla::statement::{Consistency, prepared::PreparedStatement};
use std::sync::Arc;
use std::time::Duration;

pub struct AppState {
    pub session: Arc<Session>,
    pub prepared_insert: PreparedStatement,
}

impl AppState {
    pub async fn cleanup(&self) -> anyhow::Result<()> {
        println!("Cleaning up demo keyspace...");
        self.session
            .query_unpaged("DROP KEYSPACE IF EXISTS demo;", ())
            .await?;
        println!("Cleanup completed.");
        Ok(())
    }

    pub async fn init() -> anyhow::Result<Self> {
        // Build SessionBuilder and optionally enable TLS via tls::load_tls()
        let mut builder = SessionBuilder::new()
            .known_node("127.0.0.2:9042")
            .known_node("127.0.0.3:9042")
            .known_node("127.0.0.4:9042");

        // Try to load TLS config (returns Option<Arc<ClientConfig>>)
        if let Some(tls_config) = crate::tls::load_tls_config().context("loading TLS config")? {
            builder = builder.tls_context(Some(tls_config));
            println!("TLS enabled for Scylla connections.");
        } else {
            println!("TLS not enabled. Connecting without TLS.");
        }

        let profile = ExecutionProfile::builder()
            .consistency(Consistency::LocalOne)
            .request_timeout(Some(Duration::from_secs(42)))
            .load_balancing_policy(Arc::new(load_balancing::DefaultPolicy::default()))
            .retry_policy(Arc::new(DefaultRetryPolicy::new()))
            .build();
        let profile_handle = profile.into_handle();
        builder = builder.default_execution_profile_handle(profile_handle);
        let session: Session = builder.build().await.context("failed to build session")?;

        // Ensure keyspace and table exist
        let ks_cql = "CREATE KEYSPACE IF NOT EXISTS demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} AND tablets = {'enabled': false};";
        session.query_unpaged(ks_cql, ()).await?;
        let tbl_cql =
            "CREATE TABLE IF NOT EXISTS demo.items (id uuid PRIMARY KEY, name text, value bigint);";
        session.query_unpaged(tbl_cql, ()).await?;

        // Prepare statements
        let prepared_insert = session
            .prepare("INSERT INTO demo.items (id, name, value) VALUES (?, ?, ?)")
            .await?;

        Ok(AppState {
            session: Arc::new(session),
            prepared_insert,
        })
    }
}
