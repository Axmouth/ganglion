//! Copy-on-write sharing for the large collections in a coordination snapshot.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A value shared between snapshot copies until one of them changes it.
///
/// The state machine hands a full snapshot to every applied entry's reply and
/// to every watcher. Sharing the collections makes those copies cheap, and an
/// entry that changes one collection copies only that one. Mutable access
/// copies the value first when another snapshot still shares it.
///
/// Serializes exactly like the inner value, so stored snapshots and logs keep
/// their format.
#[derive(Debug, Default, Eq)]
pub struct Shared<T>(Arc<T>);

impl<T> Shared<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(value))
    }

    /// Whether both share the same allocation, which implies equal contents.
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.0, &other.0)
    }
}

impl<T: Clone> Shared<T> {
    /// The inner value, copied only when another snapshot still shares it.
    pub fn into_inner(self) -> T {
        Arc::unwrap_or_clone(self.0)
    }
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> Deref for Shared<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: Clone> DerefMut for Shared<T> {
    fn deref_mut(&mut self) -> &mut T {
        Arc::make_mut(&mut self.0)
    }
}

impl<T> From<T> for Shared<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Clone + IntoIterator> IntoIterator for Shared<T> {
    type Item = T::Item;
    type IntoIter = T::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.into_inner().into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Shared<T>
where
    &'a T: IntoIterator,
{
    type Item = <&'a T as IntoIterator>::Item;
    type IntoIter = <&'a T as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        (&*self.0).into_iter()
    }
}

impl<'a, T: Clone> IntoIterator for &'a mut Shared<T>
where
    &'a mut T: IntoIterator,
{
    type Item = <&'a mut T as IntoIterator>::Item;
    type IntoIter = <&'a mut T as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        Arc::make_mut(&mut self.0).into_iter()
    }
}

impl<A, T: FromIterator<A>> FromIterator<A> for Shared<T> {
    fn from_iter<I: IntoIterator<Item = A>>(iter: I) -> Self {
        Self::new(T::from_iter(iter))
    }
}

impl<T: PartialEq> PartialEq for Shared<T> {
    fn eq(&self, other: &Self) -> bool {
        Self::ptr_eq(self, other) || *self.0 == *other.0
    }
}

impl<T: Serialize> Serialize for Shared<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Shared<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(Self::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn copies_on_write_only_while_shared() {
        let mut first = Shared::new(BTreeMap::from([("a".to_string(), "1".to_string())]));
        let second = first.clone();
        assert!(Shared::ptr_eq(&first, &second));

        first.insert("b".into(), "2".into());
        assert!(!Shared::ptr_eq(&first, &second));
        assert_eq!(second.len(), 1);
        assert_eq!(first.len(), 2);

        let before = Arc::as_ptr(&first.0);
        first.insert("c".into(), "3".into());
        assert_eq!(Arc::as_ptr(&first.0), before);
    }

    #[test]
    fn serializes_like_the_inner_value() {
        let map = BTreeMap::from([("key".to_string(), "value".to_string())]);
        let shared = Shared::new(map.clone());
        assert_eq!(serde_json::to_string(&shared).unwrap(), serde_json::to_string(&map).unwrap());
        let back: Shared<BTreeMap<String, String>> = serde_json::from_str(r#"{"key":"value"}"#).unwrap();
        assert_eq!(*back, map);
    }
}
