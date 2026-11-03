use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

/// Which database a row lives on: Java's `ShardIdentifier`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardIdentifier {
    pub group: u8,
    pub member: u8,
}

impl ShardIdentifier {
    pub const DEFAULT: ShardIdentifier = ShardIdentifier::new(0, 0);

    /// For constants: a bad member stops the build.
    pub const fn new(group: u8, member: u8) -> ShardIdentifier {
        assert!(member <= 63, "member must be 0 or 63 or between them");
        ShardIdentifier { group, member }
    }
}

impl fmt::Display for ShardIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.group, self.member)
    }
}

const MEMBER_BITS: u32 = 6;
const SHARD_BITS: u32 = 8 + MEMBER_BITS;
const RANDOM_BITS: u32 = 62 - SHARD_BITS;

/// A UUID that carries its shard, in Java's `ShardedUUID` layout, so both read the same ids:
/// high 64 bits `[48-bit millis][version 7][12 random bits]`, low 64 bits
/// `[variant][8-bit group][6-bit member][48 random bits]`.
pub struct ShardedId<M> {
    value: Uuid,
    model: PhantomData<fn() -> M>,
}

impl<M> ShardedId<M> {
    pub fn generate(shard: ShardIdentifier) -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_millis() as u64;
        let (random_a, random_b) = Uuid::new_v4().as_u64_pair();
        let high = ((millis & 0xFFFF_FFFF_FFFF) << 16) | 0x7000 | (random_a & 0xFFF);
        let shard_bits = (u64::from(shard.group) << MEMBER_BITS) | u64::from(shard.member);
        let low = (1 << 63) | (shard_bits << RANDOM_BITS) | (random_b & ((1 << RANDOM_BITS) - 1));
        ShardedId::from_uuid(Uuid::from_u64_pair(high, low))
    }

    pub fn from_uuid(value: Uuid) -> Self {
        ShardedId {
            value,
            model: PhantomData,
        }
    }

    pub fn uuid(&self) -> Uuid {
        self.value
    }

    pub fn shard(&self) -> ShardIdentifier {
        let (_, low) = self.value.as_u64_pair();
        let bits = (low >> RANDOM_BITS) & ((1 << SHARD_BITS) - 1);
        ShardIdentifier {
            group: (bits >> MEMBER_BITS) as u8,
            member: (bits & ((1 << MEMBER_BITS) - 1)) as u8,
        }
    }
}

impl<M> Clone for ShardedId<M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M> Copy for ShardedId<M> {}

impl<M> PartialEq for ShardedId<M> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<M> Eq for ShardedId<M> {}

impl<M> Hash for ShardedId<M> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<M> fmt::Debug for ShardedId<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (shard {})", self.value, self.shard())
    }
}

impl<M> fmt::Display for ShardedId<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.value, f)
    }
}

/// Anything that names a shard. One selector takes any of them, which covers Java's overloads:
/// `db(DefaultShard)` is `db()`, `db(shard)` is `db(shard)`, `db(&id)` is `db(id)`.
pub trait ShardKey {
    fn shard(&self, default: ShardIdentifier) -> ShardIdentifier;
}

/// The registry's default shard: Java's no-argument `db()`.
pub struct DefaultShard;

impl ShardKey for DefaultShard {
    fn shard(&self, default: ShardIdentifier) -> ShardIdentifier {
        default
    }
}

impl ShardKey for ShardIdentifier {
    fn shard(&self, _: ShardIdentifier) -> ShardIdentifier {
        *self
    }
}

impl<M> ShardKey for ShardedId<M> {
    fn shard(&self, _: ShardIdentifier) -> ShardIdentifier {
        ShardedId::shard(self)
    }
}

impl<T: ShardKey + ?Sized> ShardKey for &T {
    fn shard(&self, default: ShardIdentifier) -> ShardIdentifier {
        (**self).shard(default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_keeps_its_shard_in_javas_layout() {
        for shard in [
            ShardIdentifier::new(0, 0),
            ShardIdentifier::new(1, 0),
            ShardIdentifier::new(255, 63),
        ] {
            let id = ShardedId::<()>::generate(shard);
            assert_eq!(id.shard(), shard);
            assert_eq!(id.uuid().get_version_num(), 7);
            assert_eq!(id.uuid().get_variant(), uuid::Variant::RFC4122);
        }
    }

    #[test]
    fn reads_the_shard_of_an_id_built_by_hand_the_way_java_builds_it() {
        // group 1, member 2, every random bit zero: low = variant | (1 << 6 | 2) << 48.
        let low = (1u64 << 63) | (((1u64 << 6) | 2) << 48);
        let id = ShardedId::<()>::from_uuid(Uuid::from_u64_pair(0x7000, low));
        assert_eq!(id.shard(), ShardIdentifier::new(1, 2));
    }
}
