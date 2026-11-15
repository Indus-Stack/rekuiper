use super::model::{CatalogError, CatalogGeneration};
use crate::contracts::Generation;
use parking_lot::RwLock;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

/// Transactional SQLite-backed store of immutable catalog generations.
///
/// Concurrent readers observe a single active [`Arc<CatalogGeneration>`] snapshot,
/// while historical generations remain persisted for inspection and rollback.
pub struct CatalogStore {
    pool: SqlitePool,
    active: RwLock<Option<Arc<CatalogGeneration>>>,
}

impl CatalogStore {
    /// Opens or initializes a catalog store at `path` (or `:memory:`).
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, CatalogError> {
        let path_str = path.as_ref().to_string_lossy();
        let url = if path_str == ":memory:" {
            "sqlite::memory:".to_owned()
        } else {
            format!("sqlite://{path_str}")
        };
        let opts = SqliteConnectOptions::from_str(&url)
            .map_err(|e| CatalogError::Storage(e.to_string()))?
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .map_err(|e| CatalogError::Storage(e.to_string()))?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS catalog_generations (
                generation INTEGER PRIMARY KEY,
                created_wall_millis INTEGER NOT NULL,
                snapshot_json TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .map_err(|e| CatalogError::Storage(e.to_string()))?;

        let row = sqlx::query(
            "SELECT snapshot_json FROM catalog_generations ORDER BY generation DESC LIMIT 1",
        )
        .fetch_optional(&pool)
        .await
        .map_err(|e| CatalogError::Storage(e.to_string()))?;

        let active = match row {
            Some(r) => {
                let raw: String = r.get("snapshot_json");
                let gen_val: CatalogGeneration =
                    serde_json::from_str(&raw).map_err(|e| CatalogError::Storage(e.to_string()))?;
                Some(Arc::new(gen_val))
            }
            None => None,
        };

        Ok(Self {
            pool,
            active: RwLock::new(active),
        })
    }

    /// Returns the currently active immutable catalog generation snapshot.
    pub fn current_generation(&self) -> Option<Arc<CatalogGeneration>> {
        self.active.read().clone()
    }

    /// Validates and atomically commits a candidate catalog generation.
    ///
    /// Retains all previous generations in SQLite; if validation or transaction
    /// commit fails, the prior active generation remains completely unchanged.
    pub async fn commit_candidate(
        &self,
        expected_parent: Option<Generation>,
        candidate: CatalogGeneration,
    ) -> Result<Arc<CatalogGeneration>, CatalogError> {
        candidate.validate()?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CatalogError::Storage(e.to_string()))?;

        let latest_row = sqlx::query(
            "SELECT generation FROM catalog_generations ORDER BY generation DESC LIMIT 1",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| CatalogError::Storage(e.to_string()))?;

        let actual_parent = latest_row.map(|r| {
            let g: i64 = r.get("generation");
            Generation(g as u64)
        });

        if actual_parent != expected_parent {
            return Err(CatalogError::GenerationConflict {
                expected: expected_parent,
                actual: actual_parent,
            });
        }

        if let Some(parent) = actual_parent {
            if candidate.generation.0 <= parent.0 {
                return Err(CatalogError::InvalidObject(format!(
                    "candidate generation {} must exceed parent {}",
                    candidate.generation.0, parent.0
                )));
            }
        } else if candidate.generation.0 == 0 {
            return Err(CatalogError::InvalidObject(
                "initial catalog generation must be >= 1".to_owned(),
            ));
        }

        let generation_i64 = generation_to_sqlite(candidate.generation)?;
        let json =
            serde_json::to_string(&candidate).map_err(|e| CatalogError::Storage(e.to_string()))?;

        sqlx::query(
            "INSERT INTO catalog_generations (generation, created_wall_millis, snapshot_json)
             VALUES (?1, ?2, ?3)",
        )
        .bind(generation_i64)
        .bind(candidate.created_wall_millis)
        .bind(json)
        .execute(&mut *tx)
        .await
        .map_err(|e| CatalogError::Storage(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| CatalogError::Storage(e.to_string()))?;

        let arc = Arc::new(candidate);
        *self.active.write() = Some(arc.clone());
        Ok(arc)
    }

    /// Reads any persisted historical generation for inspection or rollback.
    pub async fn load_generation(
        &self,
        generation: Generation,
    ) -> Result<Arc<CatalogGeneration>, CatalogError> {
        let generation_i64 = generation_to_sqlite(generation)?;
        let row =
            sqlx::query("SELECT snapshot_json FROM catalog_generations WHERE generation = ?1")
                .bind(generation_i64)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| CatalogError::Storage(e.to_string()))?;

        let Some(row) = row else {
            return Err(CatalogError::GenerationNotFound(generation));
        };
        let raw: String = row.get("snapshot_json");
        let parsed: CatalogGeneration =
            serde_json::from_str(&raw).map_err(|e| CatalogError::Storage(e.to_string()))?;
        Ok(Arc::new(parsed))
    }

    /// Rolls back the active in-memory pointer to a previously committed generation
    /// and records a new monotonic generation copying that historical snapshot.
    pub async fn rollback_to(
        &self,
        target_generation: Generation,
        new_generation: Generation,
        wall_millis: i64,
    ) -> Result<Arc<CatalogGeneration>, CatalogError> {
        let historical = self.load_generation(target_generation).await?;
        let current = self.current_generation().map(|g| g.generation);
        let candidate = CatalogGeneration {
            generation: new_generation,
            created_wall_millis: wall_millis,
            entries: historical.entries.clone(),
            edges: historical.edges.clone(),
        };
        self.commit_candidate(current, candidate).await
    }
}

fn generation_to_sqlite(generation: Generation) -> Result<i64, CatalogError> {
    i64::try_from(generation.0).map_err(|_| {
        CatalogError::InvalidObject(format!(
            "catalog generation {} exceeds SQLite integer range (max {})",
            generation.0,
            i64::MAX
        ))
    })
}
