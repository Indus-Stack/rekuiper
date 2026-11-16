use crate::contracts::{ContentChecksum, Generation};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Kind of versioned catalog object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogObjectKind {
    Rule,
    Stream,
    Table,
    View,
    Schema,
    Connection,
    Extension,
    Action,
}

impl CatalogObjectKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rule => "rule",
            Self::Stream => "stream",
            Self::Table => "table",
            Self::View => "view",
            Self::Schema => "schema",
            Self::Connection => "connection",
            Self::Extension => "extension",
            Self::Action => "action",
        }
    }
}

impl fmt::Display for CatalogObjectKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Stable object identity independent of mutable display name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CatalogObjectId(String);

impl CatalogObjectId {
    pub fn new(raw: impl Into<String>) -> Result<Self, CatalogError> {
        let s = raw.into();
        if s.trim().is_empty() || s.len() > 128 {
            return Err(CatalogError::InvalidObject(
                "catalog object id must be 1..=128 non-empty chars".to_owned(),
            ));
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CatalogObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Pinned reference to a specific object version in a catalog generation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CatalogObjectRef {
    pub kind: CatalogObjectKind,
    pub id: CatalogObjectId,
    pub version: u64,
}

/// Versioned entry inside a catalog generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub id: CatalogObjectId,
    pub kind: CatalogObjectKind,
    pub display_name: String,
    pub version: u64,
    pub spec: serde_json::Value,
    pub checksum: ContentChecksum,
}

impl CatalogEntry {
    pub fn new(
        id: CatalogObjectId,
        kind: CatalogObjectKind,
        display_name: impl Into<String>,
        version: u64,
        spec: serde_json::Value,
    ) -> Result<Self, CatalogError> {
        let display_name = display_name.into();
        if display_name.trim().is_empty() {
            return Err(CatalogError::InvalidObject(
                "display_name must not be empty".to_owned(),
            ));
        }
        if version == 0 {
            return Err(CatalogError::InvalidObject(
                "catalog entry version must be >= 1".to_owned(),
            ));
        }
        let encoded =
            serde_json::to_vec(&spec).map_err(|e| CatalogError::InvalidObject(e.to_string()))?;
        let checksum = ContentChecksum::sha256_bytes(&encoded);
        Ok(Self {
            id,
            kind,
            display_name,
            version,
            spec,
            checksum,
        })
    }

    pub fn object_ref(&self) -> CatalogObjectRef {
        CatalogObjectRef {
            kind: self.kind,
            id: self.id.clone(),
            version: self.version,
        }
    }
}

/// Directed dependency edge (`from` depends on `to`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CatalogEdge {
    pub from: CatalogObjectRef,
    pub to: CatalogObjectRef,
}

/// Immutable snapshot of all catalog objects and dependency edges at one generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogGeneration {
    pub generation: Generation,
    pub created_wall_millis: i64,
    pub entries: BTreeMap<CatalogObjectId, CatalogEntry>,
    pub edges: Vec<CatalogEdge>,
}

impl CatalogGeneration {
    pub fn validate(&self) -> Result<(), CatalogError> {
        let mut seen_names: BTreeSet<(CatalogObjectKind, &str)> = BTreeSet::new();
        for (key_id, entry) in &self.entries {
            if key_id != &entry.id {
                return Err(CatalogError::InvalidObject(format!(
                    "map key {key_id} does not match entry id {}",
                    entry.id
                )));
            }
            if !seen_names.insert((entry.kind, entry.display_name.as_str())) {
                return Err(CatalogError::DuplicateDisplayName {
                    kind: entry.kind,
                    display_name: entry.display_name.clone(),
                });
            }
        }

        let mut adj: BTreeMap<&CatalogObjectId, Vec<&CatalogObjectId>> = BTreeMap::new();
        for edge in &self.edges {
            let Some(from_entry) = self.entries.get(&edge.from.id) else {
                return Err(CatalogError::MissingDependency {
                    from: edge.from.id.clone(),
                    missing: edge.from.clone(),
                });
            };
            if from_entry.kind != edge.from.kind || from_entry.version != edge.from.version {
                return Err(CatalogError::MissingDependency {
                    from: edge.from.id.clone(),
                    missing: edge.from.clone(),
                });
            }

            let Some(to_entry) = self.entries.get(&edge.to.id) else {
                return Err(CatalogError::MissingDependency {
                    from: edge.from.id.clone(),
                    missing: edge.to.clone(),
                });
            };
            if to_entry.kind != edge.to.kind || to_entry.version != edge.to.version {
                return Err(CatalogError::MissingDependency {
                    from: edge.from.id.clone(),
                    missing: edge.to.clone(),
                });
            }

            adj.entry(&edge.from.id).or_default().push(&edge.to.id);
        }

        // Detect dependency cycles using 3-color DFS.
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum Visit {
            Visiting,
            Done,
        }
        let mut state: BTreeMap<&CatalogObjectId, Visit> = BTreeMap::new();

        fn dfs<'a>(
            node: &'a CatalogObjectId,
            adj: &BTreeMap<&'a CatalogObjectId, Vec<&'a CatalogObjectId>>,
            state: &mut BTreeMap<&'a CatalogObjectId, Visit>,
        ) -> Result<(), CatalogError> {
            match state.get(node).copied() {
                Some(Visit::Visiting) => return Err(CatalogError::DependencyCycle(node.clone())),
                Some(Visit::Done) => return Ok(()),
                None => {}
            }
            state.insert(node, Visit::Visiting);
            if let Some(neighbors) = adj.get(node) {
                for next in neighbors {
                    dfs(next, adj, state)?;
                }
            }
            state.insert(node, Visit::Done);
            Ok(())
        }

        for id in self.entries.keys() {
            if !state.contains_key(id) {
                dfs(id, &adj, &mut state)?;
            }
        }

        Ok(())
    }

    pub fn by_display_name(
        &self,
        kind: CatalogObjectKind,
        display_name: &str,
    ) -> Option<&CatalogEntry> {
        self.entries
            .values()
            .find(|e| e.kind == kind && e.display_name == display_name)
    }
}

/// Errors returned by catalog validation and transactional storage.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("invalid catalog object: {0}")]
    InvalidObject(String),
    #[error("duplicate display name '{display_name}' for kind {kind}")]
    DuplicateDisplayName {
        kind: CatalogObjectKind,
        display_name: String,
    },
    #[error("missing dependency for {from}: {missing:?}")]
    MissingDependency {
        from: CatalogObjectId,
        missing: CatalogObjectRef,
    },
    #[error("dependency cycle detected at object {0}")]
    DependencyCycle(CatalogObjectId),
    #[error("generation conflict: expected current {expected:?}, actual {actual:?}")]
    GenerationConflict {
        expected: Option<Generation>,
        actual: Option<Generation>,
    },
    #[error("catalog generation {0:?} not found")]
    GenerationNotFound(Generation),
    #[error("catalog storage error: {0}")]
    Storage(String),
}
