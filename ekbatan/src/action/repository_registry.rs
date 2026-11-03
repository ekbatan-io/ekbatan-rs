//! Java's `RepositoryRegistry`: which repository writes each model and entity type.

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::action::plan::{AnyChanges, PersistableChanges};
use crate::{Error, Persistable, Repository, ShardIdentifier, Transaction};

/// Which repository writes each type an action can stage: Java's `RepositoryRegistry`.
#[derive(Clone, Default)]
pub struct RepositoryRegistry {
    repositories: HashMap<TypeId, Arc<dyn AnyRepository>>,
}

impl RepositoryRegistry {
    pub fn builder() -> RepositoryRegistryBuilder {
        RepositoryRegistryBuilder::default()
    }

    /// The repository of `E`, as registered.
    pub fn repository<E: Persistable>(&self) -> Option<&dyn AnyRepository> {
        self.repositories
            .get(&TypeId::of::<E>())
            .map(|repository| &**repository)
    }

    pub(crate) fn by_type(&self, type_id: TypeId) -> Option<&dyn AnyRepository> {
        self.repositories.get(&type_id).map(|r| &**r)
    }
}

#[derive(Default)]
pub struct RepositoryRegistryBuilder {
    repositories: HashMap<TypeId, Arc<dyn AnyRepository>>,
}

impl RepositoryRegistryBuilder {
    /// Registers `repository` as the writer of its persistable type: Java's
    /// `withModelRepository(Wallet.class, walletRepository)` (and `withEntityRepository`).
    pub fn with_repository<R: Repository + 'static>(mut self, repository: Arc<R>) -> Self {
        self.repositories
            .insert(TypeId::of::<R::Persistable>(), Arc::new(Erased(repository)));
        self
    }

    pub fn build(self) -> RepositoryRegistry {
        RepositoryRegistry {
            repositories: self.repositories,
        }
    }
}

/// One type's changes, split by shard.
pub type ChangesByShard = Vec<(ShardIdentifier, Box<dyn AnyChanges>)>;

/// A repository, whatever its type: what the action executor calls.
pub trait AnyRepository: Send + Sync {
    /// Splits one type's staged changes by the shard each row belongs to, in the order the
    /// shards are first met.
    fn group_by_shard(&self, changes: &dyn AnyChanges) -> Result<ChangesByShard, Error>;

    /// Writes one shard's changes of this type in `tx`: Java's `addAllNoResult` for the
    /// additions, then `updateAllNoResult` for the updates.
    fn persist<'a>(
        &'a self,
        tx: &'a mut Transaction,
        changes: &'a dyn AnyChanges,
    ) -> BoxFuture<'a, Result<(), Error>>;
}

struct Erased<R>(Arc<R>);

impl<R: Repository + 'static> AnyRepository for Erased<R> {
    fn group_by_shard(&self, changes: &dyn AnyChanges) -> Result<ChangesByShard, Error> {
        let changes = downcast::<R::Persistable>(changes)?;
        let mut by_shard = Vec::new();
        for persistable in &changes.additions {
            let shard = self.0.effective_shard(persistable.id())?;
            slot(&mut by_shard, shard).add(persistable.clone())?;
        }
        for persistable in &changes.updates {
            let shard = self.0.effective_shard(persistable.id())?;
            slot(&mut by_shard, shard).update(persistable.clone())?;
        }
        Ok(by_shard
            .into_iter()
            .map(|(shard, changes)| (shard, Box::new(changes) as Box<dyn AnyChanges>))
            .collect())
    }

    fn persist<'a>(
        &'a self,
        tx: &'a mut Transaction,
        changes: &'a dyn AnyChanges,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let changes = downcast::<R::Persistable>(changes)?;
            if !changes.additions.is_empty() {
                self.0
                    .add_all_no_result(Some(&mut *tx), &changes.additions)
                    .await?;
            }
            if !changes.updates.is_empty() {
                self.0
                    .update_all_no_result(Some(&mut *tx), &changes.updates)
                    .await?;
            }
            Ok(())
        })
    }
}

fn downcast<E: Persistable>(changes: &dyn AnyChanges) -> Result<&PersistableChanges<E>, Error> {
    changes.as_any().downcast_ref().ok_or_else(|| {
        Error::Unexpected(format!(
            "changes of {} given to the repository of {}",
            changes.model_name(),
            E::model_name()
        ))
    })
}

/// The changes of `shard` in `by_shard`, added at the end the first time it is met.
fn slot<E: Persistable>(
    by_shard: &mut Vec<(ShardIdentifier, PersistableChanges<E>)>,
    shard: ShardIdentifier,
) -> &mut PersistableChanges<E> {
    let index = match by_shard.iter().position(|(s, _)| *s == shard) {
        Some(index) => index,
        None => {
            by_shard.push((shard, PersistableChanges::default()));
            by_shard.len() - 1
        }
    };
    &mut by_shard[index].1
}
