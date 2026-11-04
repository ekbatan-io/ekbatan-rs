//! Generates `schema.rs` (the tables, read by `ekbatan-schemagen` from a throwaway PostgreSQL
//! with `migrations/` applied), as Java's build runs jOOQ's codegen. Needs Docker. Runs again
//! when a migration changes.

fn main() {
    ekbatan_schemagen::Schemagen::new(ekbatan::Dialect::Postgres)
        .migrations("migrations")
        .write_for_build()
        .expect("schema generated from the migrations");
}
