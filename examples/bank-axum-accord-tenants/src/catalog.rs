//! The tenant catalog and registry.
//!
//! **Catalog** — a `Tenant` aggregate in a *system* consensus group
//! (`GroupId::DEFAULT`, its own `system.db` on every host). Creating a tenant is an
//! ordinary replicated write whose aggregate id is the slug, so a duplicate slug is
//! rejected cluster-wide by the version condition — no host-local lock, no
//! coordination outside Accord. Every host runs an (ephemeral, replay-from-zero)
//! subscription on the catalog, which is how it learns about tenants other hosts
//! created, at runtime, with no config push.
//!
//! **Registry** — per process: which tenants exist (a mirror of the catalog), which
//! are *open* here (its SQLite file + consensus group + executor), and an LRU cap on
//! how many are open at once. A tenant is opened when it is created, when a request
//! names it, or when a peer's frame for its group arrives while it is evicted (the
//! transport's unrouted hook) — but **only** if the catalog knows it: the wire can
//! never create a database.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use evento::metadata::Event;
use evento::migrator::{Migrate, Plan};
use evento::subscription::{Context, Subscription, SubscriptionBuilder};
use evento::{Executor, WriteError};
use evento_accord::{
    AccordExecutor, DataStore, ExecutorDataStore, GroupHost, GroupId, Journal, NodeConfig, NodeId,
    StaticTopology, Topology,
};
use evento_sql::{Sql, SqlJournal};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Sqlite, SqlitePool};

/// The catalog aggregate. Its id is the tenant's slug.
#[evento::aggregate(name = "tenants/Tenant")]
pub enum Tenant {
    /// A tenant was created (by an application user, at runtime).
    TenantCreated { name: String },
    /// A tenant was suspended: its group is closed everywhere and requests are refused.
    TenantSuspended { reason: String },
}

/// A tenant's executor: Accord-coordinated writes, reads off its own SQLite file.
pub type TenantExecutor = AccordExecutor<Sql<Sqlite>>;

/// What the registry knows about a tenant (mirrored from the catalog).
#[derive(Clone, Debug)]
pub struct TenantInfo {
    pub slug: String,
    pub name: String,
    pub group: GroupId,
    pub suspended: bool,
}

/// Why a tenant could not be created.
#[derive(Debug)]
pub enum CreateError {
    /// Not `[a-z0-9][a-z0-9-]{0,31}`.
    InvalidSlug,
    /// A tenant with this slug already exists (rejected by consensus).
    Taken,
    /// The slug hashes to a group id another tenant already uses (one in 2⁶⁴).
    GroupCollision,
    Other(anyhow::Error),
}

struct OpenTenant {
    executor: TenantExecutor,
    pool: SqlitePool,
    last_used: Instant,
}

/// Per-process tenant state; see the module docs.
pub struct TenantRegistry {
    host: Arc<GroupHost>,
    ids: Vec<NodeId>,
    data_dir: PathBuf,
    /// Most tenants open at once on this host (LRU beyond it).
    cap: usize,
    system: TenantExecutor,
    tenants: RwLock<HashMap<String, TenantInfo>>,
    by_group: RwLock<HashMap<GroupId, String>>,
    open: Mutex<HashMap<GroupId, OpenTenant>>,
    /// Serializes opening one group (see `ensure_open`).
    open_locks: Mutex<HashMap<GroupId, Arc<tokio::sync::Mutex<()>>>>,
}

/// Tenants created within this many seconds are opened eagerly by every host when
/// their `TenantCreated` arrives, so the first write finds its replicas ready. Older
/// ones (the catalog replay at boot) are only *learned*, and open on first use.
const FRESH_SECS: u64 = 60;

impl TenantRegistry {
    /// Opens the system group (the catalog) on this host and returns the registry.
    pub async fn new(
        host: Arc<GroupHost>,
        ids: Vec<NodeId>,
        data_dir: PathBuf,
        cap: usize,
    ) -> anyhow::Result<Arc<Self>> {
        let (sql, pool) = open_db(&data_dir.join("system.db")).await?;
        let node = host
            .open(
                GroupId::DEFAULT,
                topology(host.id(), &ids),
                Arc::new(ExecutorDataStore::new(sql.clone())) as Arc<dyn DataStore>,
                Arc::new(SqlJournal::<Sqlite>::new(pool)) as Arc<dyn Journal>,
                node_config(),
            )
            .await?;
        Ok(Arc::new(Self {
            host,
            ids,
            data_dir,
            cap: cap.max(1),
            system: AccordExecutor::new(node, sql),
            tenants: RwLock::new(HashMap::new()),
            by_group: RwLock::new(HashMap::new()),
            open: Mutex::new(HashMap::new()),
            open_locks: Mutex::new(HashMap::new()),
        }))
    }

    /// The group a slug maps to: FNV-1a over the slug, never `GroupId::DEFAULT`
    /// (reserved for the catalog). Identical on every host, so no host needs to be
    /// told a tenant's group id — only that the tenant exists.
    pub fn group_of(slug: &str) -> GroupId {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in slug.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        GroupId(hash.max(1))
    }

    pub fn host(&self) -> &Arc<GroupHost> {
        &self.host
    }

    pub fn system(&self) -> &TenantExecutor {
        &self.system
    }

    /// Every known tenant, by slug.
    pub fn tenants(&self) -> Vec<TenantInfo> {
        let mut all: Vec<TenantInfo> = self
            .tenants
            .read()
            .expect("tenants poisoned")
            .values()
            .cloned()
            .collect();
        all.sort_by(|a, b| a.slug.cmp(&b.slug));
        all
    }

    pub fn info(&self, slug: &str) -> Option<TenantInfo> {
        self.tenants
            .read()
            .expect("tenants poisoned")
            .get(slug)
            .cloned()
    }

    /// How many tenants are open (SQLite file + consensus group) on this host.
    pub fn open_count(&self) -> usize {
        self.open.lock().expect("open poisoned").len()
    }

    /// Records a tenant the catalog announced.
    fn learn(&self, info: TenantInfo) {
        self.by_group
            .write()
            .expect("by_group poisoned")
            .insert(info.group, info.slug.clone());
        self.tenants
            .write()
            .expect("tenants poisoned")
            .insert(info.slug.clone(), info);
    }

    fn mark_suspended(&self, slug: &str) {
        let group = {
            let mut tenants = self.tenants.write().expect("tenants poisoned");
            let Some(info) = tenants.get_mut(slug) else {
                return;
            };
            info.suspended = true;
            info.group
        };
        self.close(group);
    }

    /// Creates a tenant through the catalog group. `Taken` is decided by consensus:
    /// the slug is the aggregate id and the write declares a brand-new stream.
    pub async fn create(&self, slug: &str, name: &str) -> Result<TenantInfo, CreateError> {
        if !valid_slug(slug) {
            return Err(CreateError::InvalidSlug);
        }
        let group = Self::group_of(slug);
        if let Some(other) = self.by_group.read().expect("by_group poisoned").get(&group) {
            if other != slug {
                return Err(CreateError::GroupCollision);
            }
        }
        match evento::append(slug)
            .event(&TenantCreated {
                name: name.to_owned(),
            })
            .commit(&self.system)
            .await
        {
            Ok(_) => {}
            Err(WriteError::InvalidOriginalVersion) => return Err(CreateError::Taken),
            Err(err) => return Err(CreateError::Other(err.into())),
        }
        let info = TenantInfo {
            slug: slug.to_owned(),
            name: name.to_owned(),
            group,
            suspended: false,
        };
        // Read-your-writes: the catalog subscription will deliver this too, but the
        // creating host opens the tenant right away so the redirect lands on it.
        self.learn(info.clone());
        self.ensure_open(&info).await.map_err(CreateError::Other)?;
        Ok(info)
    }

    /// Suspends a tenant: closes its group everywhere (each host sees the event).
    pub async fn suspend(&self, slug: &str, reason: &str) -> anyhow::Result<()> {
        let Some(info) = self.info(slug) else {
            anyhow::bail!("unknown tenant {slug}");
        };
        if info.suspended {
            return Ok(());
        }
        // The catalog stream is at version 1 (created); a stale version here means a
        // concurrent change — fine to surface as an error for this demo.
        evento::append(slug)
            .original_version(1)
            .event(&TenantSuspended {
                reason: reason.to_owned(),
            })
            .commit(&self.system)
            .await?;
        self.mark_suspended(slug);
        Ok(())
    }

    /// What the registry knows about `slug` — or, if nothing yet, what the **catalog
    /// stream** says. The catalog subscription learns tenants a little after they
    /// commit (subscriptions wait for the stability watermark), but the catalog group
    /// applies the `TenantCreated` to this host's `system.db` within milliseconds of
    /// the commit, so a request for a tenant created on another node a moment ago
    /// finds it here instead of a 404.
    async fn info_or_lookup(&self, slug: &str) -> anyhow::Result<Option<TenantInfo>> {
        if let Some(info) = self.info(slug) {
            return Ok(Some(info));
        }
        if !valid_slug(slug) {
            return Ok(None);
        }
        let mut info: Option<TenantInfo> = None;
        for event in evento::read::<Tenant>(slug).decode(&self.system).await? {
            match event {
                TenantEvent::TenantCreated(TenantCreated { name }) => {
                    info = Some(TenantInfo {
                        slug: slug.to_owned(),
                        name,
                        group: Self::group_of(slug),
                        suspended: false,
                    });
                }
                TenantEvent::TenantSuspended(_) => {
                    if let Some(info) = info.as_mut() {
                        info.suspended = true;
                    }
                }
            }
        }
        if let Some(info) = &info {
            self.learn(info.clone());
        }
        Ok(info)
    }

    /// The executor for `slug`, opening the tenant on this host if needed. `None` if
    /// the catalog has no such tenant; an error if it is suspended.
    pub async fn executor(&self, slug: &str) -> anyhow::Result<Option<TenantExecutor>> {
        let Some(info) = self.info_or_lookup(slug).await? else {
            return Ok(None);
        };
        if info.suspended {
            anyhow::bail!("tenant {slug} is suspended");
        }
        Ok(Some(self.ensure_open(&info).await?))
    }

    /// Opens the tenant's SQLite file (migrating it) and its consensus group on this
    /// host, or returns the already-open executor. Evicts the least recently used
    /// tenants beyond the cap.
    pub async fn ensure_open(&self, info: &TenantInfo) -> anyhow::Result<TenantExecutor> {
        if let Some(executor) = self.touch(info.group) {
            return Ok(executor);
        }
        // One open per group at a time (a request and the catalog subscription may
        // race to open the same tenant, and must not both migrate a fresh file);
        // re-check under the lock.
        let open_lock = Arc::clone(
            self.open_locks
                .lock()
                .expect("open_locks poisoned")
                .entry(info.group)
                .or_default(),
        );
        let _opening = open_lock.lock().await;
        if let Some(executor) = self.touch(info.group) {
            return Ok(executor);
        }
        let path = self
            .data_dir
            .join("tenants")
            .join(format!("{}.db", info.slug));
        let (sql, pool) = open_db(&path).await?;
        let node = self
            .host
            .open(
                info.group,
                topology(self.host.id(), &self.ids),
                Arc::new(ExecutorDataStore::new(sql.clone())) as Arc<dyn DataStore>,
                Arc::new(SqlJournal::<Sqlite>::new(pool.clone())) as Arc<dyn Journal>,
                node_config(),
            )
            .await?;
        let executor = AccordExecutor::new(node, sql);

        // No std lock guard may live across an await (the futures must stay `Send`),
        // so the bookkeeping is one synchronous block and the awaits come after it.
        let evicted = {
            let mut open = self.open.lock().expect("open poisoned");
            open.insert(
                info.group,
                OpenTenant {
                    executor: executor.clone(),
                    pool,
                    last_used: Instant::now(),
                },
            );
            // LRU eviction beyond the cap.
            let mut evicted = Vec::new();
            while open.len() > self.cap {
                let Some((&group, _)) = open.iter().min_by_key(|(_, t)| t.last_used) else {
                    break;
                };
                if let Some(t) = open.remove(&group) {
                    evicted.push((group, t.pool));
                }
            }
            evicted
        };
        tracing::info!(tenant = %info.slug, group = info.group.0, path = %path.display(), "tenant opened");
        for (group, pool) in evicted {
            self.host.close(group);
            tokio::spawn(async move { pool.close().await });
            tracing::info!(group = group.0, "tenant evicted (LRU)");
        }
        Ok(executor)
    }

    /// The executor of an open tenant, marking it recently used.
    fn touch(&self, group: GroupId) -> Option<TenantExecutor> {
        let mut open = self.open.lock().expect("open poisoned");
        let tenant = open.get_mut(&group)?;
        tenant.last_used = Instant::now();
        Some(tenant.executor.clone())
    }

    /// Closes a tenant's group on this host (it is re-opened on demand).
    fn close(&self, group: GroupId) {
        let removed = self.open.lock().expect("open poisoned").remove(&group);
        if let Some(t) = removed {
            self.host.close(group);
            tokio::spawn(async move { t.pool.close().await });
        }
    }

    /// The transport's unrouted-group hook: a peer sent frames for a group this host
    /// has not registered. Re-open it **only if the catalog knows it** (an evicted
    /// tenant); anything else is ignored — the wire cannot create tenants. Runs on
    /// the connection's read task, so the open is spawned.
    pub fn reopen(self: &Arc<Self>, group: GroupId) {
        let slug = self
            .by_group
            .read()
            .expect("by_group poisoned")
            .get(&group)
            .cloned();
        let Some(slug) = slug else {
            tracing::debug!(group = group.0, "frames for an unknown group ignored");
            return;
        };
        let Some(info) = self.info(&slug) else {
            return;
        };
        if info.suspended {
            return;
        }
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = registry.ensure_open(&info).await {
                tracing::warn!(tenant = %info.slug, error = %err, "re-open failed");
            }
        });
    }

    /// Starts the catalog subscription: replays the whole catalog (learning every
    /// tenant) and then follows it live. Ephemeral — no cursor row, replays on every
    /// boot by design, since the registry is in-memory.
    pub async fn start_catalog_subscription(self: &Arc<Self>) -> anyhow::Result<Subscription> {
        SubscriptionBuilder::<TenantExecutor>::new("tenant-catalog")
            .handler(on_tenant_created())
            .handler(on_tenant_suspended())
            .data(Arc::clone(self))
            .ephemeral()
            .continue_on_error()
            .start(&self.system)
            .await
    }
}

#[evento::subscription]
async fn on_tenant_created<E: Executor>(
    ctx: &Context<'_, E>,
    event: Event<TenantCreated>,
) -> anyhow::Result<()> {
    let registry: Arc<TenantRegistry> = ctx.extract();
    let slug = event.aggregate_id.clone();
    let info = TenantInfo {
        group: TenantRegistry::group_of(&slug),
        slug,
        name: event.data.name.clone(),
        suspended: false,
    };
    registry.learn(info.clone());
    if now_secs().saturating_sub(event.timestamp) <= FRESH_SECS {
        registry.ensure_open(&info).await?;
    }
    Ok(())
}

#[evento::subscription]
async fn on_tenant_suspended<E: Executor>(
    ctx: &Context<'_, E>,
    event: Event<TenantSuspended>,
) -> anyhow::Result<()> {
    let registry: Arc<TenantRegistry> = ctx.extract();
    registry.mark_suspended(&event.aggregate_id);
    Ok(())
}

/// Every group reads with the **linearizable read barrier** on: a request may land
/// on any node right after a write committed on another, and without the barrier a
/// local read can miss that write (serializable but stale — see `OPERATIONS.md` §9).
/// It costs one consensus round per single-aggregate read; a deployment that can
/// pin a tenant's requests to one node can turn it off.
fn node_config() -> NodeConfig {
    NodeConfig {
        linearizable_reads: true,
        ..NodeConfig::default()
    }
}

fn topology(id: NodeId, ids: &[NodeId]) -> Arc<dyn Topology> {
    Arc::new(StaticTopology::new(id, ids.to_vec()))
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn valid_slug(slug: &str) -> bool {
    let bytes = slug.as_bytes();
    (1..=32).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase() | bytes[0].is_ascii_digit()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// Opens (creating and migrating) one SQLite file holding a store's events,
/// projections **and** consensus journal — the migrator's `accord` feature registers
/// the journal tables alongside the event schema. Pool bounds matter at scale:
/// sqlx-sqlite runs one OS thread per open connection, so keep `max_connections`
/// small and let idle connections close. `synchronous(Full)` because this file is
/// also the consensus journal (an acked write must survive power loss).
async fn open_db(path: &Path) -> anyhow::Result<(Sql<Sqlite>, SqlitePool)> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(3)
        .min_connections(0)
        .idle_timeout(Some(Duration::from_secs(30)))
        .connect_with(options)
        .await?;
    let mut conn = pool.acquire().await?;
    evento_sql_migrator::new::<Sqlite>()?
        .run(&mut *conn, &Plan::apply_all())
        .await?;
    drop(conn);
    // One `Sql` for both the data-store bridge and the executor: two `.into()`s from
    // the same pool would not share the write-wake channel subscriptions rely on.
    let sql: Sql<Sqlite> = pool.clone().into();
    Ok((sql, pool))
}
