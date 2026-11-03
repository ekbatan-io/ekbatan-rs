//! `Id` and `ShardedId` in JSON as Java's Jackson writes them: through their two getters,
//! `getId()` and `getValue()`, which both return the UUID - `{"id":"...","value":"..."}`.
//! Read back from that object (either key), or from a bare UUID string.

use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use uuid::Uuid;

use crate::{Id, ShardedId};

fn write<S: Serializer>(name: &'static str, value: Uuid, serializer: S) -> Result<S::Ok, S::Error> {
    let text = value.hyphenated().to_string();
    let mut object = serializer.serialize_struct(name, 2)?;
    object.serialize_field("id", &text)?;
    object.serialize_field("value", &text)?;
    object.end()
}

struct UuidOrObject;

impl<'de> Visitor<'de> for UuidOrObject {
    type Value = Uuid;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(r#"a UUID, or {"id": UUID, "value": UUID}"#)
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<Uuid, E> {
        Uuid::parse_str(text).map_err(E::custom)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Uuid, A::Error> {
        let mut found: Option<Uuid> = None;
        while let Some(key) = map.next_key::<String>()? {
            let value: Uuid = map.next_value()?;
            if key == "id" || key == "value" {
                if let Some(previous) = found
                    && previous != value
                {
                    return Err(de::Error::custom("\"id\" and \"value\" differ"));
                }
                found = Some(value);
            }
        }
        found.ok_or_else(|| de::Error::missing_field("value"))
    }
}

impl<M> Serialize for ShardedId<M> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        write("ShardedId", self.uuid(), serializer)
    }
}

impl<'de, M> Deserialize<'de> for ShardedId<M> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer
            .deserialize_any(UuidOrObject)
            .map(ShardedId::from_uuid)
    }
}

impl<M> Serialize for Id<M> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        write("Id", self.value(), serializer)
    }
}

impl<'de, M> Deserialize<'de> for Id<M> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(UuidOrObject).map(Id::of)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Id, ShardIdentifier, ShardedId};

    struct Wallet;

    #[test]
    fn ids_are_written_as_java_writes_them_and_read_back() {
        let id = ShardedId::<Wallet>::generate(ShardIdentifier::new(1, 0));
        let json = serde_json::to_string(&id).unwrap();
        let text = id.uuid().to_string();
        assert_eq!(json, format!(r#"{{"id":"{text}","value":"{text}"}}"#));
        let back: ShardedId<Wallet> = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
        let bare: ShardedId<Wallet> = serde_json::from_str(&format!("\"{text}\"")).unwrap();
        assert_eq!(bare, id);
        let plain = Id::<Wallet>::random();
        let back: Id<Wallet> =
            serde_json::from_str(&serde_json::to_string(&plain).unwrap()).unwrap();
        assert_eq!(back, plain);
    }
}
