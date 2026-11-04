//! Generates `schema.rs` (the tables, read by `ekbatan-schemagen` from a throwaway MySQL
//! with `migrations/` applied), as Java's build runs jOOQ's codegen. Needs Docker. Runs again
//! when a migration changes.

fn main() {
    ekbatan_schemagen::Schemagen::new(ekbatan::Dialect::MySql)
        .migrations("migrations")
        .write_for_build()
        .expect("schema generated from the migrations");
}
