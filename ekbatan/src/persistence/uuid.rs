use sqlx::error::BoxDynError;
use sqlx::mysql::{MySql, MySqlTypeInfo, MySqlValueRef};
use sqlx::postgres::{PgTypeInfo, PgValueRef, Postgres};
use sqlx::{Decode, Type};
use uuid::Uuid;

/// A UUID column, read the way each database keeps it: Java's `UuidStringConverter`.
/// PostgreSQL has a `uuid` type; MySQL keeps `CHAR(36)` text; MariaDB's own `UUID` type arrives
/// as text. Writing needs no type of its own: `AsyncDb` sends UUIDs to MySQL and MariaDB as text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DbUuid(pub Uuid);

impl Type<Postgres> for DbUuid {
    fn type_info() -> PgTypeInfo {
        <Uuid as Type<Postgres>>::type_info()
    }
}

impl<'r> Decode<'r, Postgres> for DbUuid {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(DbUuid(<Uuid as Decode<Postgres>>::decode(value)?))
    }
}

impl Type<MySql> for DbUuid {
    fn type_info() -> MySqlTypeInfo {
        <str as Type<MySql>>::type_info()
    }

    fn compatible(ty: &MySqlTypeInfo) -> bool {
        <str as Type<MySql>>::compatible(ty) || <[u8] as Type<MySql>>::compatible(ty)
    }
}

impl<'r> Decode<'r, MySql> for DbUuid {
    fn decode(value: MySqlValueRef<'r>) -> Result<Self, BoxDynError> {
        let bytes = <&[u8] as Decode<MySql>>::decode(value)?;
        let uuid = match bytes.len() {
            16 => Uuid::from_slice(bytes)?,
            _ => Uuid::try_parse_ascii(bytes)?,
        };
        Ok(DbUuid(uuid))
    }
}
