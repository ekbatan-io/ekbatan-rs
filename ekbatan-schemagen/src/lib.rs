//! Table definitions generated from the database your migrations produce: Java's jOOQ codegen
//! step (a container, Flyway into it, jOOQ reads it), for the same reason - the schema is the
//! database's, not a hand-copy of it, so the two can't drift.
//!
//! Run from the app's `build.rs`, so a build always matches its migrations, as Gradle's does:
//!
//! ```ignore
//! // build.rs
//! fn main() {
//!     ekbatan_schemagen::Schemagen::new(ekbatan::Dialect::Postgres)
//!         .migrations("migrations/postgres")
//!         .write_for_build()
//!         .expect("schema generated");
//! }
//!
//! // src/schema.rs
//! ekbatan::include_schema!();
//! ```
//!
//! What it does:
//!
//! 1. starts a throwaway database in Docker (Testcontainers): PostgreSQL 17, MySQL 8.4 or
//!    MariaDB 11.8, or the image tag given;
//! 2. runs the migrations into it with ekbatan's `Migrator`, the same code an app runs;
//! 3. reads every table of the default schema from `information_schema` (ekbatan's own,
//!    `scheduled_tasks` and the migration history, are left out);
//! 4. maps each column to a Rust type (forced types first, as Java's `forcedTypes`);
//! 5. writes, per table, the sea-query `Iden` enum (jOOQ's table and fields), the `Table`
//!    impl when it has `id`, `version` and `state`, and the row struct (jOOQ's `Record`).

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use ekbatan::migration::{MigrationLocation, Migrator};
use ekbatan::{DataSourceConfig, Dialect, Error};
use heck::{ToSnakeCase, ToUpperCamelCase};
use regex::Regex;
use sqlx::Connection;
use testcontainers_modules::mariadb::Mariadb;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

/// Tables that are ekbatan's or a tool's, never the app's.
const FRAMEWORK_TABLES: [&str; 3] = [
    "_sqlx_migrations",
    "scheduled_tasks",
    "flyway_schema_history",
];

/// A column's Rust type chosen by the project: Java's jOOQ `ForcedType`. Matches on
/// `table.column` (`include_expression`) and on the database type as `information_schema`
/// gives it, such as `char(36)` or `numeric` (`include_types`); both must match. The type must
/// read with sqlx and convert into a sea-query `Value`.
#[derive(Clone, Debug)]
pub struct ForcedType {
    pub rust_type: String,
    pub include_expression: Regex,
    pub include_types: Regex,
}

impl ForcedType {
    pub fn new(rust_type: &str) -> Self {
        ForcedType {
            rust_type: rust_type.to_owned(),
            include_expression: Regex::new(".*").expect("a valid regex"),
            include_types: Regex::new(".*").expect("a valid regex"),
        }
    }

    /// A regex for `table.column`, matched whole (case-insensitive).
    pub fn include_expression(mut self, regex: &str) -> Self {
        self.include_expression = whole(regex);
        self
    }

    /// A regex for the database type, matched whole (case-insensitive).
    pub fn include_types(mut self, regex: &str) -> Self {
        self.include_types = whole(regex);
        self
    }

    fn matches(&self, table: &str, column: &str, db_type: &str) -> bool {
        self.include_expression
            .is_match(&format!("{table}.{column}"))
            && self.include_types.is_match(db_type)
    }
}

fn whole(regex: &str) -> Regex {
    Regex::new(&format!("(?i)^(?:{regex})$")).expect("a valid regex")
}

/// The generator's settings.
#[derive(Clone, Debug)]
pub struct Schemagen {
    dialect: Dialect,
    locations: Vec<MigrationLocation>,
    image_tag: Option<String>,
    forced_types: Vec<ForcedType>,
    includes: Option<Regex>,
    excludes: Option<Regex>,
}

impl Schemagen {
    pub fn new(dialect: Dialect) -> Self {
        Schemagen {
            dialect,
            locations: Vec::new(),
            image_tag: None,
            forced_types: Vec::new(),
            includes: None,
            excludes: None,
        }
    }

    /// A migration folder (or embedded files), relative to the crate. Several are applied
    /// together, as `Migrator::locations`.
    pub fn migrations(mut self, location: impl Into<MigrationLocation>) -> Self {
        self.locations.push(location.into());
        self
    }

    /// The database image's tag: `17-alpine`, `8.4`, `11.8` by default.
    pub fn image_tag(mut self, tag: &str) -> Self {
        self.image_tag = Some(tag.to_owned());
        self
    }

    pub fn forced_type(mut self, forced_type: ForcedType) -> Self {
        self.forced_types.push(forced_type);
        self
    }

    /// Only tables whose name matches (whole, case-insensitive).
    pub fn includes(mut self, regex: &str) -> Self {
        self.includes = Some(whole(regex));
        self
    }

    /// Not tables whose name matches (whole, case-insensitive).
    pub fn excludes(mut self, regex: &str) -> Self {
        self.excludes = Some(whole(regex));
        self
    }

    /// From `build.rs`: writes `$OUT_DIR/schema.rs` (for `ekbatan::include_schema!()`) and
    /// tells cargo to run again when a migration folder changes.
    pub fn write_for_build(&self) -> Result<PathBuf, Error> {
        for location in &self.locations {
            if let MigrationLocation::Dir(dir) = location {
                println!("cargo:rerun-if-changed={}", dir.display());
            }
        }
        let out_dir = std::env::var("OUT_DIR")
            .map_err(|_| Error::Config("OUT_DIR is not set: call this from build.rs".to_owned()))?;
        let path = Path::new(&out_dir).join("schema.rs");
        self.write_to(&path)?;
        Ok(path)
    }

    /// Writes the generated module to `path`.
    pub fn write_to(&self, path: &Path) -> Result<(), Error> {
        let code = self.generate()?;
        std::fs::write(path, code)
            .map_err(|e| Error::Config(format!("can't write {}: {e}", path.display())))
    }

    /// The generated module, as text. Blocks: it runs its own async runtime.
    pub fn generate(&self) -> Result<String, Error> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::Config(format!("can't start a runtime: {e}")))?
            .block_on(self.generate_async())
    }

    /// The generated module, as text.
    pub async fn generate_async(&self) -> Result<String, Error> {
        if self.locations.is_empty() {
            return Err(Error::Config(
                "schemagen needs a migration location".to_owned(),
            ));
        }
        let database = TestDatabase::start(self.dialect, self.image_tag.as_deref()).await?;
        Migrator::builder()
            .with_data_source(database.config())
            .locations(self.locations.clone())
            .migrate()
            .await?;
        let columns = database.columns().await?;
        let tables = self.tables(columns)?;
        Ok(render(self.dialect, &tables))
    }

    fn tables(&self, columns: Vec<ColumnRow>) -> Result<Vec<GenTable>, Error> {
        let mut tables: Vec<GenTable> = Vec::new();
        for row in columns {
            if FRAMEWORK_TABLES.contains(&row.table.as_str())
                || self
                    .includes
                    .as_ref()
                    .is_some_and(|r| !r.is_match(&row.table))
                || self
                    .excludes
                    .as_ref()
                    .is_some_and(|r| r.is_match(&row.table))
            {
                continue;
            }
            let rust_type = self.rust_type(&row)?;
            let column = GenColumn {
                name: row.column.clone(),
                rust_type,
                nullable: row.nullable,
            };
            match tables.iter_mut().find(|t| t.name == row.table) {
                Some(table) => table.columns.push(column),
                None => tables.push(GenTable {
                    name: row.table,
                    columns: vec![column],
                }),
            }
        }
        Ok(tables)
    }

    /// The Rust type of a column: a forced type, Java's UUID-in-`CHAR(36)` rule, or the type
    /// table below.
    fn rust_type(&self, row: &ColumnRow) -> Result<RustType, Error> {
        if let Some(forced) = self
            .forced_types
            .iter()
            .find(|f| f.matches(&row.table, &row.column, &row.db_type))
        {
            return Ok(RustType::Custom(forced.rust_type.clone()));
        }
        let db_type = row.db_type.to_lowercase();
        let base = db_type.split('(').next().unwrap_or("").trim();
        let unsigned = db_type.contains("unsigned");
        let ty = match self.dialect {
            Dialect::Postgres => match base {
                "uuid" => RustType::Uuid,
                "bigint" => RustType::Copy("i64"),
                "integer" => RustType::Copy("i32"),
                "smallint" => RustType::Copy("i16"),
                "boolean" => RustType::Copy("bool"),
                "character varying" | "character" | "text" => RustType::Text,
                "numeric" => RustType::Copy("::rust_decimal::Decimal"),
                "timestamp without time zone" => RustType::Copy("::chrono::NaiveDateTime"),
                "timestamp with time zone" => RustType::Copy("::chrono::DateTime<::chrono::Utc>"),
                "date" => RustType::Copy("::chrono::NaiveDate"),
                "time without time zone" => RustType::Copy("::chrono::NaiveTime"),
                "double precision" => RustType::Copy("f64"),
                "real" => RustType::Copy("f32"),
                "bytea" => RustType::Bytes,
                _ => return Err(unsupported(row)),
            },
            Dialect::MySql | Dialect::MariaDb => match base {
                // Java's MySQL build: CHAR(36) is a UUID on `id` and `*_id` columns only.
                "char" if db_type.starts_with("char(36)") && is_id_column(&row.column) => {
                    RustType::Uuid
                }
                "uuid" => RustType::Uuid,
                "bigint" if unsigned => RustType::Copy("u64"),
                "bigint" => RustType::Copy("i64"),
                "int" | "integer" if unsigned => RustType::Copy("u32"),
                "int" | "integer" => RustType::Copy("i32"),
                "smallint" if unsigned => RustType::Copy("u16"),
                "smallint" => RustType::Copy("i16"),
                "tinyint" if db_type.starts_with("tinyint(1)") => RustType::Copy("bool"),
                "tinyint" if unsigned => RustType::Copy("u8"),
                "tinyint" => RustType::Copy("i8"),
                "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" => {
                    RustType::Text
                }
                "decimal" => RustType::Copy("::rust_decimal::Decimal"),
                "datetime" | "timestamp" => RustType::Copy("::chrono::NaiveDateTime"),
                "date" => RustType::Copy("::chrono::NaiveDate"),
                "time" => RustType::Copy("::chrono::NaiveTime"),
                "double" => RustType::Copy("f64"),
                "float" => RustType::Copy("f32"),
                "blob" | "tinyblob" | "mediumblob" | "longblob" | "binary" | "varbinary" => {
                    RustType::Bytes
                }
                _ => return Err(unsupported(row)),
            },
        };
        Ok(ty)
    }
}

fn is_id_column(column: &str) -> bool {
    let column = column.to_lowercase();
    column == "id" || column.ends_with("_id")
}

fn unsupported(row: &ColumnRow) -> Error {
    Error::Config(format!(
        "{}.{} has type {}, which schemagen has no Rust type for: add a ForcedType for it",
        row.table, row.column, row.db_type
    ))
}

/// One column as `information_schema` describes it.
struct ColumnRow {
    table: String,
    column: String,
    /// PostgreSQL's `data_type`; MySQL's and MariaDB's `column_type` (`char(36)`,
    /// `bigint(20) unsigned`, ...).
    db_type: String,
    nullable: bool,
}

#[derive(Clone, Debug)]
enum RustType {
    Uuid,
    Text,
    Bytes,
    Copy(&'static str),
    Custom(String),
}

impl RustType {
    fn name(&self) -> String {
        match self {
            RustType::Uuid => "::ekbatan::DbUuid".to_owned(),
            RustType::Text => "::std::string::String".to_owned(),
            RustType::Bytes => "::std::vec::Vec<u8>".to_owned(),
            RustType::Copy(name) => (*name).to_owned(),
            RustType::Custom(name) => name.clone(),
        }
    }

    /// The expression turning `self.<field>` into a sea-query `Value`.
    fn value(&self, field: &str, nullable: bool) -> String {
        match (self, nullable) {
            (RustType::Uuid, false) => format!("self.{field}.0.into()"),
            (RustType::Uuid, true) => format!("self.{field}.map(|v| v.0).into()"),
            (RustType::Copy(_), _) => format!("self.{field}.into()"),
            _ => format!("self.{field}.clone().into()"),
        }
    }
}

struct GenColumn {
    name: String,
    rust_type: RustType,
    nullable: bool,
}

struct GenTable {
    name: String,
    columns: Vec<GenColumn>,
}

const KEYWORDS: [&str; 38] = [
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern",
    "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub",
    "ref", "return", "self", "static", "struct", "super", "trait", "true", "type", "unsafe", "use",
    "where", "while", "gen",
];

fn field_name(column: &str) -> String {
    let name = column.to_snake_case();
    if KEYWORDS.contains(&name.as_str()) {
        format!("r#{name}")
    } else {
        name
    }
}

fn variant_name(column: &str) -> String {
    let name = column.to_upper_camel_case();
    if name == "Table" {
        "TableColumn".to_owned()
    } else {
        name
    }
}

fn render(dialect: Dialect, tables: &[GenTable]) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "// Generated by ekbatan-schemagen from {dialect:?} with the migrations applied. Do not \
         edit:\n// change a migration and build again.\n"
    );
    for table in tables {
        render_table(&mut out, table);
    }
    out
}

fn render_table(out: &mut String, table: &GenTable) {
    let enum_name = table.name.to_upper_camel_case();
    let record = format!("{enum_name}Record");
    let variants: Vec<(String, &GenColumn)> = table
        .columns
        .iter()
        .map(|c| (variant_name(&c.name), c))
        .collect();
    let _ = writeln!(
        out,
        "/// The `{}` table and its columns, for building SQL: jOOQ's table and fields.",
        table.name
    );
    let _ = writeln!(
        out,
        "#[derive(::sea_query::Iden, Clone, Copy, Debug, PartialEq, Eq)]"
    );
    let _ = writeln!(out, "pub enum {enum_name} {{");
    let _ = writeln!(out, "    #[iden = \"{}\"]\n    Table,", table.name);
    for (variant, column) in &variants {
        let _ = writeln!(out, "    #[iden = \"{}\"]\n    {variant},", column.name);
    }
    let _ = writeln!(out, "}}\n");

    let has = |name: &str| variants.iter().find(|(_, c)| c.name == name);
    if let (Some(id), Some(version), Some(state)) = (has("id"), has("version"), has("state")) {
        let _ = writeln!(out, "impl ::ekbatan::Table for {enum_name} {{");
        let _ = writeln!(out, "    const TABLE: Self = {enum_name}::Table;");
        let _ = writeln!(out, "    const ID: Self = {enum_name}::{};", id.0);
        let _ = writeln!(out, "    const VERSION: Self = {enum_name}::{};", version.0);
        let _ = writeln!(out, "    const STATE: Self = {enum_name}::{};", state.0);
        let _ = writeln!(out, "    const COLUMNS: &'static [Self] = &[");
        for (variant, _) in &variants {
            let _ = writeln!(out, "        {enum_name}::{variant},");
        }
        let _ = writeln!(out, "    ];\n}}\n");
    }

    let _ = writeln!(out, "/// One `{}` row: jOOQ's record.", table.name);
    let _ = writeln!(out, "#[derive(Clone, Debug, PartialEq, ::sqlx::FromRow)]");
    let _ = writeln!(out, "pub struct {record} {{");
    for column in &table.columns {
        let field = field_name(&column.name);
        if field.trim_start_matches("r#") != column.name {
            let _ = writeln!(out, "    #[sqlx(rename = \"{}\")]", column.name);
        }
        let ty = column.rust_type.name();
        let ty = if column.nullable {
            format!("::core::option::Option<{ty}>")
        } else {
            ty
        };
        let _ = writeln!(out, "    pub {field}: {ty},");
    }
    let _ = writeln!(out, "}}\n");

    if has("id").is_some() && has("version").is_some() && has("state").is_some() {
        let _ = writeln!(out, "impl ::ekbatan::TableRecord for {record} {{");
        let _ = writeln!(out, "    type Table = {enum_name};\n");
        let _ = writeln!(
            out,
            "    fn values(&self) -> ::std::vec::Vec<({enum_name}, ::sea_query::Value)> {{"
        );
        let _ = writeln!(out, "        ::std::vec![");
        for (variant, column) in &variants {
            let field = field_name(&column.name);
            let value = column.rust_type.value(&field, column.nullable);
            let _ = writeln!(out, "            ({enum_name}::{variant}, {value}),");
        }
        let _ = writeln!(out, "        ]\n    }}\n}}\n");
    }
}

/// The throwaway database, in a container that stops when this is dropped.
struct TestDatabase {
    dialect: Dialect,
    url: String,
    username: &'static str,
    password: Option<&'static str>,
    _postgres: Option<ContainerAsync<Postgres>>,
    _mysql: Option<ContainerAsync<Mysql>>,
    _mariadb: Option<ContainerAsync<Mariadb>>,
}

impl TestDatabase {
    async fn start(dialect: Dialect, tag: Option<&str>) -> Result<Self, Error> {
        let failed = |e: &dyn std::fmt::Display| {
            Error::Config(format!(
                "can't start the database (is Docker running?): {e}"
            ))
        };
        let mut database = TestDatabase {
            dialect,
            url: String::new(),
            username: "root",
            password: None,
            _postgres: None,
            _mysql: None,
            _mariadb: None,
        };
        match dialect {
            Dialect::Postgres => {
                let container = Postgres::default()
                    .with_tag(tag.unwrap_or("17-alpine"))
                    .start()
                    .await
                    .map_err(|e| failed(&e))?;
                let port = container
                    .get_host_port_ipv4(5432)
                    .await
                    .map_err(|e| failed(&e))?;
                database.url = format!("postgres://127.0.0.1:{port}/postgres");
                database.username = "postgres";
                database.password = Some("postgres");
                database._postgres = Some(container);
            }
            Dialect::MySql => {
                let container = Mysql::default()
                    .with_tag(tag.unwrap_or("8.4"))
                    .start()
                    .await
                    .map_err(|e| failed(&e))?;
                let port = container
                    .get_host_port_ipv4(3306)
                    .await
                    .map_err(|e| failed(&e))?;
                database.url = format!("mysql://127.0.0.1:{port}/test");
                database._mysql = Some(container);
            }
            Dialect::MariaDb => {
                let container = Mariadb::default()
                    .with_tag(tag.unwrap_or("11.8"))
                    .start()
                    .await
                    .map_err(|e| failed(&e))?;
                let port = container
                    .get_host_port_ipv4(3306)
                    .await
                    .map_err(|e| failed(&e))?;
                database.url = format!("mysql://127.0.0.1:{port}/test");
                database._mariadb = Some(container);
            }
        }
        Ok(database)
    }

    fn config(&self) -> DataSourceConfig {
        DataSourceConfig {
            username: Some(self.username.to_owned()),
            password: self.password.map(str::to_owned),
            ..DataSourceConfig::new(self.dialect, self.url.clone(), 1)
        }
    }

    fn login_url(&self) -> String {
        let login = match self.password {
            Some(password) => format!("{}:{password}@", self.username),
            None => format!("{}@", self.username),
        };
        self.url.replacen("://", &format!("://{login}"), 1)
    }

    /// Every column of every base table in the default schema, in table and column order.
    async fn columns(&self) -> Result<Vec<ColumnRow>, Error> {
        let rows: Vec<(String, String, String, String)> = match self.dialect {
            Dialect::Postgres => {
                let mut connection = sqlx::PgConnection::connect(&self.login_url()).await?;
                sqlx::query_as(
                    "SELECT c.table_name::text, c.column_name::text, c.data_type::text, \
                     c.is_nullable::text \
                     FROM information_schema.columns c \
                     JOIN information_schema.tables t \
                       ON t.table_schema = c.table_schema AND t.table_name = c.table_name \
                     WHERE c.table_schema = current_schema() AND t.table_type = 'BASE TABLE' \
                     ORDER BY c.table_name, c.ordinal_position",
                )
                .fetch_all(&mut connection)
                .await?
            }
            Dialect::MySql | Dialect::MariaDb => {
                let mut connection = sqlx::MySqlConnection::connect(&self.login_url()).await?;
                sqlx::query_as(
                    "SELECT CAST(c.TABLE_NAME AS CHAR), CAST(c.COLUMN_NAME AS CHAR), \
                     CAST(c.COLUMN_TYPE AS CHAR), CAST(c.IS_NULLABLE AS CHAR) \
                     FROM information_schema.COLUMNS c \
                     JOIN information_schema.TABLES t \
                       ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME \
                     WHERE c.TABLE_SCHEMA = DATABASE() AND t.TABLE_TYPE = 'BASE TABLE' \
                     ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION",
                )
                .fetch_all(&mut connection)
                .await?
            }
        };
        Ok(rows
            .into_iter()
            .map(|(table, column, db_type, nullable)| ColumnRow {
                table,
                column,
                db_type,
                nullable: nullable == "YES",
            })
            .collect())
    }
}
