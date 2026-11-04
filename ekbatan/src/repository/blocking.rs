//! `Repository`: every repository's selectors and ready-made methods, waited on.
//! `aio::AsyncRepository` is the async twin; both are given to every `AbstractRepository`.

use sea_query::Condition;

use crate::aio::AsyncRepository;
use crate::blocking::{Db, Dbs, Transaction, block_on};
use crate::repository::IdOf;
use crate::{AbstractRepository, Error, ShardKey};

/// Every repository's selectors and ready-made methods, with Java's SQL: Java's
/// `AbstractRepository` methods. Implemented for every `AbstractRepository`; the async twin is
/// `AsyncRepository`. A module that imports both traits must say which one it means.
pub trait Repository: AbstractRepository {
    // ---- The database selectors, for code that is not async (the async ones are on
    // ---- `AsyncRepository`). Each takes a shard key: `DefaultShard`, a `ShardIdentifier`, or
    // ---- an id, which covers Java's overloads.

    /// The primary: for writes, and reads that must be current. Java's `db(...)`.
    fn db(&self, key: impl ShardKey) -> Result<Db<'_>, Error> {
        AsyncRepository::db(self, key).map(Db::new)
    }

    /// Every shard's primary; only the default shard's under `NoSharding`. Java's `dbs()`.
    fn dbs(&self) -> Dbs<'_> {
        Dbs::new(AsyncRepository::dbs(self))
    }

    /// The replica: for reads that may lag. Java's `readonlyDb(...)`.
    fn readonly_db(&self, key: impl ShardKey) -> Result<Db<'_>, Error> {
        AsyncRepository::readonly_db(self, key).map(Db::new)
    }

    /// Every shard's replica. Java's `readonlyDbs()`.
    fn readonly_dbs(&self) -> Dbs<'_> {
        Dbs::new(AsyncRepository::readonly_dbs(self))
    }

    /// The caller's transaction, refused if it is on another shard than the key.
    fn tx_db<'a>(&'a self, tx: &'a mut Transaction, key: impl ShardKey) -> Result<Db<'a>, Error> {
        AsyncRepository::tx_db(self, tx.as_async(), key).map(Db::new)
    }

    /// The caller's transaction if given one, else the primary.
    fn tx_db_else_db<'a>(
        &'a self,
        tx: Option<&'a mut Transaction>,
        key: impl ShardKey,
    ) -> Result<Db<'a>, Error> {
        AsyncRepository::tx_db_else_db(self, tx.map(Transaction::as_async), key).map(Db::new)
    }

    fn add(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Persistable,
    ) -> Result<Self::Persistable, Error> {
        block_on(AsyncRepository::add(
            self,
            tx.map(Transaction::as_async),
            model,
        ))
    }

    fn add_no_result(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Persistable,
    ) -> Result<(), Error> {
        block_on(AsyncRepository::add_no_result(
            self,
            tx.map(Transaction::as_async),
            model,
        ))
    }

    fn add_all(
        &self,
        tx: Option<&mut Transaction>,
        models: &[Self::Persistable],
    ) -> Result<Vec<Self::Persistable>, Error> {
        block_on(AsyncRepository::add_all(
            self,
            tx.map(Transaction::as_async),
            models,
        ))
    }

    fn add_all_no_result(
        &self,
        tx: Option<&mut Transaction>,
        models: &[Self::Persistable],
    ) -> Result<(), Error> {
        block_on(AsyncRepository::add_all_no_result(
            self,
            tx.map(Transaction::as_async),
            models,
        ))
    }

    fn update(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Persistable,
    ) -> Result<Self::Persistable, Error> {
        block_on(AsyncRepository::update(
            self,
            tx.map(Transaction::as_async),
            model,
        ))
    }

    fn update_no_result(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Persistable,
    ) -> Result<(), Error> {
        block_on(AsyncRepository::update_no_result(
            self,
            tx.map(Transaction::as_async),
            model,
        ))
    }

    fn update_all(
        &self,
        tx: Option<&mut Transaction>,
        models: &[Self::Persistable],
    ) -> Result<Vec<Self::Persistable>, Error> {
        block_on(AsyncRepository::update_all(
            self,
            tx.map(Transaction::as_async),
            models,
        ))
    }

    fn update_all_no_result(
        &self,
        tx: Option<&mut Transaction>,
        models: &[Self::Persistable],
    ) -> Result<(), Error> {
        block_on(AsyncRepository::update_all_no_result(
            self,
            tx.map(Transaction::as_async),
            models,
        ))
    }

    fn find_by_id(&self, id: &IdOf<Self>) -> Result<Option<Self::Persistable>, Error> {
        block_on(AsyncRepository::find_by_id(self, id))
    }

    fn get_by_id(&self, id: &IdOf<Self>) -> Result<Self::Persistable, Error> {
        block_on(AsyncRepository::get_by_id(self, id))
    }

    fn find_all_by_ids(&self, ids: &[IdOf<Self>]) -> Result<Vec<Self::Persistable>, Error> {
        block_on(AsyncRepository::find_all_by_ids(self, ids))
    }

    fn find_all(&self) -> Result<Vec<Self::Persistable>, Error> {
        block_on(AsyncRepository::find_all(self))
    }

    fn exists_by_id(&self, id: &IdOf<Self>) -> Result<bool, Error> {
        block_on(AsyncRepository::exists_by_id(self, id))
    }

    fn count(&self) -> Result<i64, Error> {
        block_on(AsyncRepository::count(self))
    }

    fn count_where(&self, condition: Condition) -> Result<i64, Error> {
        block_on(AsyncRepository::count_where(self, condition))
    }

    fn find_one_where(&self, condition: Condition) -> Result<Option<Self::Persistable>, Error> {
        block_on(AsyncRepository::find_one_where(self, condition))
    }

    fn find_all_where(&self, condition: Condition) -> Result<Vec<Self::Persistable>, Error> {
        block_on(AsyncRepository::find_all_where(self, condition))
    }

    fn exists_where(&self, condition: Condition) -> Result<bool, Error> {
        block_on(AsyncRepository::exists_where(self, condition))
    }
}

impl<R: AbstractRepository + ?Sized> Repository for R {}
