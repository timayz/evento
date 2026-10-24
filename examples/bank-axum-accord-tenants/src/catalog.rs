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
//! are *open* here (its SQLite files + consensus group + executor), and an LRU cap on
//! how many are open at once. A tenant is opened when it is created, when a request
//! names it, or when a peer's frame for its group arrives while it is evicted (the
//! transport's unrouted hook) — but **only** if the catalog knows it: the wire can
//! never create a database. Where a group's files live, and how a group comes back
//! when one of them was lost, is [`crate::storage`]'s business.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use evento::cursor::{Args, Value};
use evento::metadata::Event;
use evento::subscription::{Context, Subscription, SubscriptionBuilder};
use evento::{EventFilter, Executor, WriteError};
use evento_accord::{
    AccordExecutor, GroupHost, GroupId, NodeConfig, NodeId, StaticTopology, Topology,
};
use evento_sql::Sql;
use sqlx::Sqlite;

use crate::storage::{GroupDbs, RejoinFailed, Storage};

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
    dbs: GroupDbs,
    last_used: Instant,
}

/// Per-process tenant state; see the module docs.
pub struct TenantRegistry {
    host: Arc<GroupHost>,
    ids: Vec<NodeId>,
    storage: Storage,
    /// Most tenants open at once on this host (LRU beyond it).
    cap: usize,
    system: TenantExecutor,
    system_dbs: GroupDbs,
    /// Set once the catalog was told to rejoin (see `rejoin`).
    catalog_rejoin: tokio::sync::watch::Sender<bool>,
    tenants: RwLock<HashMap<String, TenantInfo>>,
    by_group: RwLock<HashMap<GroupId, String>>,
    open: Mutex<HashMap<GroupId, OpenTenant>>,
    /// Serializes opening and closing one group (see `ensure_open`, `close`).
    open_locks: Mutex<HashMap<GroupId, Arc<tokio::sync::Mutex<()>>>>,
    /// How far `refresh_catalog` has read the catalog stream.
    catalog_cursor: Mutex<Option<Value>>,
    /// Serializes `refresh_catalog` (several unrouted frames can trigger it at once).
    refresh_lock: tokio::sync::Mutex<()>,
}

/// Tenants created within this many seconds are opened eagerly by every host when
/// their `TenantCreated` arrives, so the first write finds its replicas ready. Older
/// ones (the catalog replay at boot) are only *learned*, and open on first use.
const FRESH_SECS: u64 = 60;

/// The catalog group's name: its files are `system.db` and `system.journal.db`.
const CATALOG: &str = "system";

/// A tenant group's name: its files are `tenants/{slug}.db` and
/// `tenants/{slug}.journal.db`.
fn tenant_name(slug: &str) -> String {
    format!("tenants/{slug}")
}

impl TenantRegistry {
    /// Opens the system group (the catalog) on this host and returns the registry.
    /// A catalog that must rejoin waits for a peer that can vouch for it,
    /// retrying until one does.
    pub async fn new(
        host: Arc<GroupHost>,
        ids: Vec<NodeId>,
        data_dir: PathBuf,
        cap: usize,
    ) -> anyhow::Result<Arc<Self>> {
        let storage = Storage::open(data_dir.clone(), || groups_on_disk(&data_dir)).await?;
        let (node, system_dbs) = loop {
            match storage
                .open_group(
                    &host,
                    GroupId::DEFAULT,
                    CATALOG,
                    topology(host.id(), &ids),
                    node_config(),
                )
                .await
            {
                Ok(opened) => break opened,
                Err(err) if err.downcast_ref::<RejoinFailed>().is_some() => {
                    tracing::warn!(
                        error = format!("{err:#}"),
                        "the catalog could not rejoin yet: retrying"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(err) => {
                    storage.close().await;
                    return Err(err);
                }
            }
        };
        Ok(Arc::new(Self {
            host,
            ids,
            storage,
            cap: cap.max(1),
            system: AccordExecutor::new(node, system_dbs.sql.clone()),
            system_dbs,
            catalog_rejoin: tokio::sync::watch::Sender::new(false),
            tenants: RwLock::new(HashMap::new()),
            by_group: RwLock::new(HashMap::new()),
            open: Mutex::new(HashMap::new()),
            open_locks: Mutex::new(HashMap::new()),
            catalog_cursor: Mutex::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
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

    async fn mark_suspended(&self, slug: &str) {
        let group = {
            let mut tenants = self.tenants.write().expect("tenants poisoned");
            let Some(info) = tenants.get_mut(slug) else {
                return;
            };
            info.suspended = true;
            info.group
        };
        self.close(group).await;
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
        self.mark_suspended(slug).await;
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

    /// Opens the tenant's SQLite files (migrating them) and its consensus group on
    /// this host, or returns the already-open executor. Evicts the least recently
    /// used tenants beyond the cap.
    pub async fn ensure_open(&self, info: &TenantInfo) -> anyhow::Result<TenantExecutor> {
        if let Some(executor) = self.touch(info.group) {
            return Ok(executor);
        }
        let executor = self.open_tenant(info).await?;
        // Not under this tenant's lock: an eviction takes the victim's, and two
        // tenants opening at once must not each wait for the other's.
        self.evict_over_cap().await;
        Ok(executor)
    }

    /// [`ensure_open`](Self::ensure_open)'s open, under the tenant's lock.
    async fn open_tenant(&self, info: &TenantInfo) -> anyhow::Result<TenantExecutor> {
        // One open per group at a time (a request and the catalog subscription may
        // race to open the same tenant, and must not both migrate a fresh file);
        // re-check under the lock.
        let open_lock = self.open_lock(info.group);
        let _opening = open_lock.lock().await;
        if let Some(executor) = self.touch(info.group) {
            return Ok(executor);
        }
        let (node, dbs) = self
            .storage
            .open_group(
                &self.host,
                info.group,
                &tenant_name(&info.slug),
                topology(self.host.id(), &self.ids),
                node_config(),
            )
            .await?;
        let executor = AccordExecutor::new(node, dbs.sql.clone());
        self.open.lock().expect("open poisoned").insert(
            info.group,
            OpenTenant {
                executor: executor.clone(),
                dbs,
                last_used: Instant::now(),
            },
        );
        tracing::info!(tenant = %info.slug, group = info.group.0, "tenant opened");
        Ok(executor)
    }

    /// Closes the least recently used tenants while more than the cap are open,
    /// each under its own lock, so a request for it waits for the close to
    /// finish and then opens it afresh — rather than pick up the node being
    /// closed.
    async fn evict_over_cap(&self) {
        while let Some(group) = self.coldest_over_cap() {
            let open_lock = self.open_lock(group);
            let _evicting = open_lock.lock().await;
            // Re-checked under its lock: another eviction or a suspension may
            // have closed it while this one waited.
            let tenant = {
                let mut open = self.open.lock().expect("open poisoned");
                if open.len() <= self.cap {
                    return;
                }
                open.remove(&group)
            };
            if let Some(tenant) = tenant {
                self.host.close(group).await;
                tenant.dbs.close().await;
                tracing::info!(group = group.0, "tenant evicted (LRU)");
            }
        }
    }

    /// The least recently used open tenant, if more than the cap are open.
    fn coldest_over_cap(&self) -> Option<GroupId> {
        let open = self.open.lock().expect("open poisoned");
        if open.len() <= self.cap {
            return None;
        }
        open.iter()
            .min_by_key(|(_, tenant)| tenant.last_used)
            .map(|(&group, _)| group)
    }

    /// The lock serializing the opening and closing of `group`.
    fn open_lock(&self, group: GroupId) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(
            self.open_locks
                .lock()
                .expect("open_locks poisoned")
                .entry(group)
                .or_default(),
        )
    }

    /// The executor of an open tenant, marking it recently used.
    fn touch(&self, group: GroupId) -> Option<TenantExecutor> {
        let mut open = self.open.lock().expect("open poisoned");
        let tenant = open.get_mut(&group)?;
        tenant.last_used = Instant::now();
        Some(tenant.executor.clone())
    }

    /// Closes a tenant's group on this host (it is re-opened on demand), then
    /// its databases: `GroupHost::close` returns once nothing writes to them.
    async fn close(&self, group: GroupId) {
        let open_lock = self.open_lock(group);
        let _closing = open_lock.lock().await;
        self.close_locked(group).await;
    }

    /// [`close`](Self::close)'s work, under the tenant's lock.
    async fn close_locked(&self, group: GroupId) {
        let removed = self.open.lock().expect("open poisoned").remove(&group);
        if let Some(tenant) = removed {
            self.host.close(group).await;
            tenant.dbs.close().await;
        }
    }

    /// The host's rejoin hook (`GroupHost::with_rejoin_handler`). A peer told
    /// this host's replica of `group` to rejoin: it was away while its peers
    /// compacted what it never applied. A tenant is closed, marked in the
    /// ledger, and opened again, which rebuilds its event file from a peer
    /// over its kept journal. The catalog cannot be swapped under a running
    /// registry, so it is only marked: the next start rebuilds it, and
    /// [`catalog_rejoin_required`](Self::catalog_rejoin_required) resolves.
    pub fn rejoin(self: &Arc<Self>, group: GroupId) {
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = registry.rejoin_now(group).await {
                tracing::warn!(group = group.0, error = format!("{err:#}"), "rejoin failed");
            }
        });
    }

    async fn rejoin_now(&self, group: GroupId) -> anyhow::Result<()> {
        if group == GroupId::DEFAULT {
            self.storage.set_hosted(group, CATALOG, true).await?;
            self.catalog_rejoin.send_replace(true);
            tracing::error!("the catalog was told to rejoin: start this host again to rebuild it");
            return Ok(());
        }
        let Some(slug) = self.slug_of(group) else {
            return Ok(());
        };
        {
            // Marked under the tenant's lock, so no open in between finds it
            // closed and opens it plainly.
            let open_lock = self.open_lock(group);
            let _closing = open_lock.lock().await;
            self.close_locked(group).await;
            self.storage
                .set_hosted(group, &tenant_name(&slug), true)
                .await?;
        }
        if let Some(info) = self.info(&slug).filter(|info| !info.suspended) {
            self.ensure_open(&info).await?;
        }
        Ok(())
    }

    /// Resolves once the catalog was told to rejoin (see [`rejoin`](Self::rejoin)).
    /// The host should then stop and start again.
    pub async fn catalog_rejoin_required(&self) {
        let mut required = self.catalog_rejoin.subscribe();
        let _ = required.wait_for(|&required| required).await;
    }

    /// Whether the catalog was told to rejoin.
    pub fn catalog_rejoin_pending(&self) -> bool {
        *self.catalog_rejoin.borrow()
    }

    /// Closes every group open here, the tenants and then the catalog, and
    /// their databases. Nothing is served after that. Stop the catalog
    /// subscription first.
    pub async fn shutdown(&self) {
        let groups: Vec<GroupId> = self
            .open
            .lock()
            .expect("open poisoned")
            .keys()
            .copied()
            .collect();
        for group in groups {
            self.close(group).await;
        }
        self.host.close(GroupId::DEFAULT).await;
        self.system_dbs.close().await;
        self.storage.close().await;
    }

    fn slug_of(&self, group: GroupId) -> Option<String> {
        self.by_group
            .read()
            .expect("by_group poisoned")
            .get(&group)
            .cloned()
    }

    /// Reads the catalog stream past where this registry last looked and learns
    /// every tenant it finds. The catalog *subscription* only sees an event once it
    /// is below the stability watermark (about a second), but the catalog group
    /// applies it to this host's `system.db` within milliseconds of the commit — so
    /// this is how a host finds out about a tenant another host created a moment
    /// ago, when that tenant's first write is already knocking (`reopen`). Returns
    /// how many catalog events were read.
    pub async fn refresh_catalog(&self) -> anyhow::Result<usize> {
        let _serial = self.refresh_lock.lock().await;
        let filters: Arc<[EventFilter]> = Arc::from(vec![
            EventFilter::by_event::<TenantCreated>(),
            EventFilter::by_event::<TenantSuspended>(),
        ]);
        const PAGE: u16 = 200;
        let mut read = 0;
        loop {
            let after = self.catalog_cursor.lock().expect("cursor poisoned").clone();
            let page = self
                .system
                .read(
                    Some(filters.clone()),
                    None,
                    Args::forward(PAGE, after),
                    None,
                )
                .await?;
            let full = page.edges.len() >= usize::from(PAGE);
            for edge in page.edges {
                let slug = edge.node.aggregate_id.clone();
                match TenantEvent::try_from(&edge.node) {
                    Ok(TenantEvent::TenantCreated(TenantCreated { name })) => {
                        self.learn(TenantInfo {
                            group: Self::group_of(&slug),
                            slug,
                            name,
                            suspended: false,
                        })
                    }
                    Ok(TenantEvent::TenantSuspended(_)) => self.mark_suspended(&slug).await,
                    Err(err) => tracing::warn!(%slug, error = %err, "undecodable catalog event"),
                }
                *self.catalog_cursor.lock().expect("cursor poisoned") = Some(edge.cursor);
                read += 1;
            }
            if !full {
                return Ok(read);
            }
        }
    }

    /// The transport's unrouted-group hook: a peer sent frames for a group this host
    /// has not registered. Open it **only if the catalog knows it** — an evicted
    /// tenant, or one created on another host so recently that this host's catalog
    /// subscription has not delivered it yet (so the catalog is re-read first).
    /// Anything the catalog does not know is ignored: the wire cannot create
    /// tenants. Runs on the connection's read task, so all the work is spawned.
    pub fn reopen(self: &Arc<Self>, group: GroupId) {
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            if registry.slug_of(group).is_none() {
                if let Err(err) = registry.refresh_catalog().await {
                    tracing::warn!(group = group.0, error = %err, "catalog refresh failed");
                    return;
                }
            }
            let Some(slug) = registry.slug_of(group) else {
                tracing::debug!(group = group.0, "frames for an unknown group ignored");
                return;
            };
            let Some(info) = registry.info(&slug) else {
                return;
            };
            if info.suspended {
                return;
            }
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
    registry.mark_suspended(&event.aggregate_id).await;
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

/// The groups whose files are in `data_dir`, by id and name: the seed of a new
/// host ledger (see [`crate::storage`]).
fn groups_on_disk(data_dir: &Path) -> anyhow::Result<Vec<(GroupId, String)>> {
    let mut found = Vec::new();
    if data_dir.join(format!("{CATALOG}.db")).exists()
        || data_dir.join(format!("{CATALOG}.journal.db")).exists()
    {
        found.push((GroupId::DEFAULT, CATALOG.to_owned()));
    }
    let tenants = data_dir.join("tenants");
    if !tenants.exists() {
        return Ok(found);
    }
    let mut slugs = std::collections::BTreeSet::new();
    for entry in std::fs::read_dir(&tenants)? {
        let file = entry?.file_name();
        let Some(file) = file.to_str() else {
            continue;
        };
        let slug = file
            .strip_suffix(".journal.db")
            .or_else(|| file.strip_suffix(".db"));
        if let Some(slug) = slug.filter(|slug| valid_slug(slug)) {
            slugs.insert(slug.to_owned());
        }
    }
    found.extend(
        slugs
            .into_iter()
            .map(|slug| (TenantRegistry::group_of(&slug), tenant_name(&slug))),
    );
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    use bank::{AccountType, Command, OpenAccount};
    use evento_accord::{MuxTransport, SweepConfig, SweepScheduler};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::SqlitePool;
    use tokio::net::TcpListener;

    struct Host {
        registry: Arc<TenantRegistry>,
        host: Arc<GroupHost>,
        /// The registry the transport's unrouted hook reaches, if one runs.
        slot: Arc<RwLock<Option<Arc<TenantRegistry>>>>,
        ids: Vec<NodeId>,
        cap: usize,
        dir: PathBuf,
        _tasks: Vec<tokio::task::JoinHandle<()>>,
    }

    impl Host {
        /// Shuts the registry down, as the process would on exit. The
        /// transport keeps running: this host's frames are unrouted until
        /// [`start`](Self::start).
        async fn stop(&self) {
            *self.slot.write().unwrap() = None;
            self.registry.shutdown().await;
        }

        /// Starts a registry again over the same data directory.
        async fn start(&mut self) {
            let registry = TenantRegistry::new(
                Arc::clone(&self.host),
                self.ids.clone(),
                self.dir.clone(),
                self.cap,
            )
            .await
            .unwrap();
            *self.slot.write().unwrap() = Some(Arc::clone(&registry));
            self.registry = registry;
        }
    }

    /// `n` hosts over localhost TCP, each with its catalog group open and **no**
    /// catalog subscription — so the unrouted hook is the only way a host learns
    /// about a tenant another host created.
    async fn hosts(n: u64) -> Vec<Host> {
        hosts_with_cap(n, 8).await
    }

    /// [`hosts`] keeping at most `cap` tenants open per host.
    async fn hosts_with_cap(n: u64, cap: usize) -> Vec<Host> {
        let ids: Vec<NodeId> = (0..n).map(NodeId).collect();
        let mut listeners = Vec::new();
        let mut peers: HashMap<NodeId, SocketAddr> = HashMap::new();
        for &id in &ids {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            peers.insert(id, listener.local_addr().unwrap());
            listeners.push((id, listener));
        }
        let mut hosts = Vec::new();
        for (id, listener) in listeners {
            let slot: Arc<RwLock<Option<Arc<TenantRegistry>>>> = Arc::new(RwLock::new(None));
            let hook = Arc::clone(&slot);
            let mux = Arc::new(MuxTransport::new(id, peers.clone()).with_unrouted_handler(
                move |group| {
                    let registry = hook.read().unwrap().clone();
                    if let Some(registry) = registry {
                        registry.reopen(group);
                    }
                },
            ));
            let mut tasks = vec![mux.serve(listener)];
            let scheduler = SweepScheduler::new(SweepConfig::default());
            tasks.push(scheduler.start());
            let host = Arc::new(GroupHost::new(mux, scheduler));
            let dir = std::env::temp_dir().join(format!(
                "tenants-catalog-test-{}-{}",
                ulid::Ulid::generate(),
                id.0
            ));
            let registry = TenantRegistry::new(Arc::clone(&host), ids.clone(), dir.clone(), cap)
                .await
                .unwrap();
            *slot.write().unwrap() = Some(Arc::clone(&registry));
            hosts.push(Host {
                registry,
                host,
                slot,
                ids: ids.clone(),
                cap,
                dir,
                _tasks: tasks,
            });
        }
        hosts
    }

    /// Opens an account holding 100 in `slug`, through `registry`.
    async fn open_account(registry: &TenantRegistry, slug: &str) -> String {
        let executor = registry.executor(slug).await.unwrap().unwrap();
        Command(executor)
            .open_account(OpenAccount {
                owner_id: "o".into(),
                owner_name: "Ann".into(),
                account_type: AccountType::Checking,
                currency: "EUR".into(),
                initial_balance: 100,
            })
            .await
            .unwrap()
    }

    /// Waits until `registry` reads `account` of `slug`, holding 100. Opening
    /// the tenant may take a rejoin, which waits for a peer's report.
    async fn sees_account(registry: &TenantRegistry, slug: &str, account: &str) {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            match registry.executor(slug).await {
                Ok(Some(executor)) => {
                    if let Ok(Some(view)) = bank::load_account_details(&executor, account, "").await
                    {
                        assert_eq!(view.balance, 100);
                        return;
                    }
                }
                Ok(None) => {}
                Err(err) => tracing::debug!(error = format!("{err:#}"), "open failed"),
            }
            assert!(
                Instant::now() < deadline,
                "{slug}'s account never showed up"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Waits until every host has `slug` open.
    async fn open_everywhere(hosts: &[Host], slug: &str) {
        let group = TenantRegistry::group_of(slug);
        let deadline = Instant::now() + Duration::from_secs(30);
        while !hosts.iter().all(|h| h.registry.host().is_open(group)) {
            assert!(Instant::now() < deadline, "{slug} never opened everywhere");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// A tenant created on the first host, with an account every host reads.
    async fn tenant_with_an_account(hosts: &[Host], slug: &str) -> (GroupId, String) {
        let info = hosts[0].registry.create(slug, slug).await.unwrap();
        let account = open_account(&hosts[0].registry, slug).await;
        open_everywhere(hosts, slug).await;
        for h in hosts {
            sees_account(&h.registry, slug, &account).await;
        }
        (info.group, account)
    }

    /// A connection to the SQLite file at `path`, which must exist.
    async fn sqlite(path: &Path) -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::new().filename(path))
            .await
            .unwrap_or_else(|err| panic!("{}: {err}", path.display()))
    }

    /// How many rows `table` holds in the SQLite file at `path`.
    async fn rows(path: &Path, table: &str) -> i64 {
        let pool = sqlite(path).await;
        let count = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {table}"
        )))
        .fetch_one(&pool)
        .await
        .unwrap_or_else(|err| panic!("{table} in {}: {err}", path.display()));
        pool.close().await;
        count
    }

    /// How many journal rows the SQLite file at `path` holds.
    async fn journal_rows(path: &Path) -> i64 {
        let mut total = 0;
        for table in [
            "accord_commands",
            "accord_meta",
            "accord_metadata_log",
            "accord_acceptors",
        ] {
            total += rows(path, table).await;
        }
        total
    }

    /// The host ledger in `dir`: each group, and whether a rejoin is pending.
    async fn ledger(dir: &Path) -> HashMap<GroupId, bool> {
        let pool = sqlite(&dir.join("host.db")).await;
        let rows: Vec<(i64, i64)> = sqlx::query_as("SELECT group_id, rejoining FROM hosted_groups")
            .fetch_all(&pool)
            .await
            .unwrap();
        pool.close().await;
        rows.into_iter()
            .map(|(group, rejoining)| (GroupId(group as u64), rejoining != 0))
            .collect()
    }

    /// Deletes a SQLite file and its `-wal` and `-shm` files.
    fn remove_db(path: &Path) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    /// The amnesia fence of `group`'s node on `registry`'s host.
    fn fence(registry: &TenantRegistry, group: GroupId) -> Option<evento_accord::AmnesiaFence> {
        registry
            .host()
            .get(group)
            .expect("the group is open")
            .amnesia_fence()
    }

    fn cleanup(hosts: &[Host]) {
        for h in hosts {
            let _ = std::fs::remove_dir_all(&h.dir);
        }
    }

    /// Every group keeps two files: its events, and its journal. The ledger
    /// lists each group a host opened, and a brand-new group opens without a
    /// fence everywhere (a fenced one would wait for a peer to vouch for it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn each_group_keeps_its_journal_in_a_file_of_its_own() {
        let hosts = hosts(3).await;
        let (group, _) = tenant_with_an_account(&hosts, "acme").await;

        for h in &hosts {
            for name in ["system", "tenants/acme"] {
                let events = h.dir.join(format!("{name}.db"));
                let journal = h.dir.join(format!("{name}.journal.db"));
                assert!(journal.exists(), "no {}", journal.display());
                assert!(journal_rows(&journal).await > 0, "{name}: empty journal");
                assert_eq!(journal_rows(&events).await, 0, "{name}: journal rows");
            }
            assert!(rows(&h.dir.join("tenants/acme.db"), "event").await > 0);
            assert_eq!(
                ledger(&h.dir).await,
                HashMap::from([(GroupId::DEFAULT, false), (group, false)])
            );
            assert_eq!(h.registry.system().node().amnesia_fence(), None);
            assert_eq!(fence(&h.registry, group), None);
        }
        cleanup(&hosts);
    }

    /// A tenant whose journal file was lost comes back under the host's node
    /// id: its events are dropped and it rejoins from a peer, fenced.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_lost_journal_file_rejoins_under_the_same_node_id() {
        let mut hosts = hosts(3).await;
        let (group, account) = tenant_with_an_account(&hosts, "acme").await;

        hosts[2].stop().await;
        remove_db(&hosts[2].dir.join("tenants/acme.journal.db"));
        hosts[2].start().await;
        sees_account(&hosts[2].registry, "acme", &account).await;

        let fence = fence(&hosts[2].registry, group).expect("no amnesia fence");
        assert!(fence.joined);
        assert_eq!(ledger(&hosts[2].dir).await.get(&group), Some(&false));
        cleanup(&hosts);
    }

    /// A journal file emptied in place leaves events without the votes that
    /// went with them: the plain open refuses them (`LostJournal`), and the
    /// tenant rejoins, fenced.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_emptied_journal_file_is_refused_and_rejoins() {
        let mut hosts = hosts(3).await;
        let (group, account) = tenant_with_an_account(&hosts, "acme").await;

        hosts[2].stop().await;
        let journal = hosts[2].dir.join("tenants/acme.journal.db");
        remove_db(&journal);
        std::fs::File::create(&journal).unwrap();
        hosts[2].start().await;
        sees_account(&hosts[2].registry, "acme", &account).await;

        let fence = fence(&hosts[2].registry, group).expect("no amnesia fence");
        assert!(fence.joined);
        cleanup(&hosts);
    }

    /// A tenant whose event file was lost is rebuilt from a peer over its kept
    /// journal: it forgot nothing it voted, so it is not fenced.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_lost_event_file_is_rebuilt_from_a_peer() {
        let mut hosts = hosts(3).await;
        let (group, account) = tenant_with_an_account(&hosts, "acme").await;
        let journal = hosts[2].dir.join("tenants/acme.journal.db");
        assert!(journal.exists(), "no {}", journal.display());

        hosts[2].stop().await;
        remove_db(&hosts[2].dir.join("tenants/acme.db"));
        hosts[2].start().await;
        sees_account(&hosts[2].registry, "acme", &account).await;

        assert_eq!(fence(&hosts[2].registry, group), None);
        assert_eq!(ledger(&hosts[2].dir).await.get(&group), Some(&false));
        cleanup(&hosts);
    }

    /// With both of a tenant's files gone, the ledger still says the host had
    /// it: it rejoins, fenced, rather than start afresh and vote at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_group_whose_files_were_both_lost_rejoins_from_the_ledger() {
        let mut hosts = hosts(3).await;
        let (group, account) = tenant_with_an_account(&hosts, "acme").await;

        hosts[2].stop().await;
        remove_db(&hosts[2].dir.join("tenants/acme.db"));
        remove_db(&hosts[2].dir.join("tenants/acme.journal.db"));
        hosts[2].start().await;
        sees_account(&hosts[2].registry, "acme", &account).await;

        let fence = fence(&hosts[2].registry, group).expect("no amnesia fence");
        assert!(fence.joined);
        cleanup(&hosts);
    }

    /// The catalog's journal is a group's journal like any other: lost, the
    /// catalog rejoins when the host starts, and still knows every tenant.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_lost_catalog_journal_rejoins_at_start() {
        let mut hosts = hosts(3).await;
        let (_, account) = tenant_with_an_account(&hosts, "acme").await;

        hosts[2].stop().await;
        remove_db(&hosts[2].dir.join("system.journal.db"));
        hosts[2].start().await;

        let fence = hosts[2]
            .registry
            .system()
            .node()
            .amnesia_fence()
            .expect("no amnesia fence");
        assert!(fence.joined);
        sees_account(&hosts[2].registry, "acme", &account).await;
        cleanup(&hosts);
    }

    /// A crash in the middle of a rejoin leaves files that look whole: a
    /// fresh journal file and an empty event file. The ledger's mark sends
    /// the next open through the rejoin again, fenced, not afresh.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_rejoin_cut_short_by_a_crash_resumes() {
        let mut hosts = hosts(3).await;
        let (group, account) = tenant_with_an_account(&hosts, "acme").await;

        hosts[2].stop().await;
        for name in ["tenants/acme.db", "tenants/acme.journal.db"] {
            let path = hosts[2].dir.join(name);
            remove_db(&path);
            std::fs::File::create(&path).unwrap();
        }
        let pool = sqlite(&hosts[2].dir.join("host.db")).await;
        sqlx::query("UPDATE hosted_groups SET rejoining = 1 WHERE group_id = ?")
            .bind(group.0 as i64)
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        hosts[2].start().await;
        sees_account(&hosts[2].registry, "acme", &account).await;

        let fence = fence(&hosts[2].registry, group).expect("no amnesia fence");
        assert!(fence.joined);
        assert_eq!(ledger(&hosts[2].dir).await.get(&group), Some(&false));
        cleanup(&hosts);
    }

    /// A data directory from before the split — each journal inside its
    /// group's event file, and no ledger — is moved to the new layout on
    /// start, journal and all: no group comes back fenced.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_single_file_group_moves_its_journal_into_a_file_of_its_own() {
        let mut hosts = hosts(3).await;
        let (group, account) = tenant_with_an_account(&hosts, "acme").await;

        hosts[2].stop().await;
        let dir = hosts[2].dir.clone();
        for name in ["system", "tenants/acme"] {
            let events = dir.join(format!("{name}.db"));
            let journal = dir.join(format!("{name}.journal.db"));
            let pool = sqlite(&events).await;
            let mut conn = pool.acquire().await.unwrap();
            sqlx::query("ATTACH DATABASE ? AS j")
                .bind(journal.to_string_lossy().into_owned())
                .execute(&mut *conn)
                .await
                .unwrap();
            for table in [
                "accord_commands",
                "accord_meta",
                "accord_metadata_log",
                "accord_acceptors",
            ] {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO main.{table} SELECT * FROM j.{table}"
                )))
                .execute(&mut *conn)
                .await
                .unwrap();
            }
            sqlx::query("DETACH DATABASE j")
                .execute(&mut *conn)
                .await
                .unwrap();
            drop(conn);
            pool.close().await;
            assert!(journal_rows(&events).await > 0);
            remove_db(&journal);
        }
        remove_db(&dir.join("host.db"));
        hosts[2].start().await;
        sees_account(&hosts[2].registry, "acme", &account).await;

        for name in ["system", "tenants/acme"] {
            assert!(journal_rows(&dir.join(format!("{name}.journal.db"))).await > 0);
            assert_eq!(journal_rows(&dir.join(format!("{name}.db"))).await, 0);
        }
        assert_eq!(hosts[2].registry.system().node().amnesia_fence(), None);
        assert_eq!(fence(&hosts[2].registry, group), None);
        assert_eq!(
            ledger(&dir).await,
            HashMap::from([(GroupId::DEFAULT, false), (group, false)])
        );
        cleanup(&hosts);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_peer_learns_a_just_created_tenant_from_its_first_frames() {
        let hosts = hosts(2).await;
        let (a, b) = (&hosts[0].registry, &hosts[1].registry);

        // Create on A and write immediately. B has never heard of "acme": its
        // catalog subscription is not running, so only A's consensus frames —
        // parked, reported to the hook, which re-reads the catalog — can tell it.
        let info = a.create("acme", "Acme").await.unwrap();
        assert!(b.info("acme").is_none(), "B knows nothing yet");
        let executor = a.executor("acme").await.unwrap().unwrap();
        let account = Command(executor)
            .open_account(OpenAccount {
                owner_id: "o".into(),
                owner_name: "Ann".into(),
                account_type: AccountType::Checking,
                currency: "EUR".into(),
                initial_balance: 100,
            })
            .await
            .unwrap();

        // B opens the tenant from the hook and converges on the write.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if b.host().is_open(info.group) {
                if let Ok(Some(executor)) = b.executor("acme").await {
                    if let Ok(Some(view)) =
                        bank::load_account_details(&executor, &account, "").await
                    {
                        assert_eq!(view.balance, 100);
                        break;
                    }
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "B never opened acme / saw the account"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(hosts[1].dir.join("tenants").join("acme.db").exists());

        // Frames for a group the catalog does not know never create anything.
        b.reopen(GroupId(0xdead_beef));
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(b.open_count(), 1);
        let mut files: Vec<String> = std::fs::read_dir(hosts[1].dir.join("tenants"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".db"))
            .collect();
        files.sort();
        assert_eq!(files, ["acme.db", "acme.journal.db"]);

        for h in &hosts {
            let _ = std::fs::remove_dir_all(&h.dir);
        }
    }

    /// Beyond the cap the coldest tenant is closed — its group, then its
    /// database — and a later request opens it afresh and finds its data.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_evicted_tenant_reopens_with_its_data() {
        let hosts = hosts_with_cap(1, 1).await;
        let registry = &hosts[0].registry;
        let one = registry.create("one", "One").await.unwrap();
        let executor = registry.executor("one").await.unwrap().unwrap();
        let account = Command(executor)
            .open_account(OpenAccount {
                owner_id: "o".into(),
                owner_name: "Ann".into(),
                account_type: AccountType::Checking,
                currency: "EUR".into(),
                initial_balance: 100,
            })
            .await
            .unwrap();

        let two = registry.create("two", "Two").await.unwrap();
        assert!(registry.host().is_open(two.group));
        assert!(!registry.host().is_open(one.group), "one was not evicted");
        assert_eq!(registry.open_count(), 1);

        let executor = registry.executor("one").await.unwrap().unwrap();
        let view = bank::load_account_details(&executor, &account, "")
            .await
            .unwrap()
            .expect("the account survived the eviction");
        assert_eq!(view.balance, 100);
        assert!(registry.host().is_open(one.group));
        assert!(!registry.host().is_open(two.group), "two was not evicted");

        for h in &hosts {
            let _ = std::fs::remove_dir_all(&h.dir);
        }
    }
}
