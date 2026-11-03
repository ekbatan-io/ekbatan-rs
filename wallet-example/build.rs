//! Generates `schema.rs` (the tables, as `ekbatan-schemagen` reads them from a database with
//! the migrations applied), as Java's build runs jOOQ's codegen before compiling. Needs Docker.
//! Runs again when a migration changes.

fn main() {
    ekbatan_schemagen::Schemagen::new(ekbatan::Dialect::Postgres)
        .migrations("migrations/postgres")
        .write_for_build()
        .expect("schema generated from the migrations");
}
