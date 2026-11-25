// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::collections::btree_map;
use std::iter::Peekable;

/// Marks iterators over key-value pairs sorted by key, such as [`btree_map::BTreeMap`].
trait Sorted {}

/// Defines helper methods on iterators over `(key, value)` pairs sorted by key.
pub trait SortedByKeyIterator<K, V>: Iterator + Sized {
    /// Compares the keys of two sorted key-value iterators and returns the diff.
    fn diff_by_key<U, W>(self, other: U) -> DiffByKeyIterator<Self, U>
    where U: SortedByKeyIterator<K, W> {
        DiffByKeyIterator {
            left: self.peekable(),
            right: other.peekable(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum KeyDiff<K, V, W> {
    Added(K, W),
    Unchanged(K, V, W),
    Removed(K, V),
}

pub struct DiffByKeyIterator<T: Iterator, U: Iterator> {
    left: Peekable<T>,
    right: Peekable<U>,
}

impl<T, U, K, V, W> Iterator for DiffByKeyIterator<T, U>
where
    T: Iterator<Item = (K, V)>,
    U: Iterator<Item = (K, W)>,
    K: Ord,
{
    type Item = KeyDiff<K, V, W>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.left.peek(), self.right.peek()) {
            (Some((left_key, _)), Some((right_key, _))) => match left_key.cmp(right_key) {
                Ordering::Less => {
                    let (left_key, left_value) = self
                        .left
                        .next()
                        .expect("The left iterator should not be empty.");
                    Some(KeyDiff::Removed(left_key, left_value))
                }
                Ordering::Equal => {
                    let (left_key, left_value) = self
                        .left
                        .next()
                        .expect("The left iterator should not be empty.");
                    let (_, right_value) = self
                        .right
                        .next()
                        .expect("The right iterator should not be empty.");
                    Some(KeyDiff::Unchanged(left_key, left_value, right_value))
                }
                Ordering::Greater => {
                    let (right_key, right_value) = self
                        .right
                        .next()
                        .expect("The right iterator should not be empty.");
                    Some(KeyDiff::Added(right_key, right_value))
                }
            },
            (Some(_), None) => {
                let (left_key, left_value) = self
                    .left
                    .next()
                    .expect("The left iterator should not be empty.");
                Some(KeyDiff::Removed(left_key, left_value))
            }
            (None, Some(_)) => {
                let (right_key, right_value) = self
                    .right
                    .next()
                    .expect("The right iterator should not be empty.");
                Some(KeyDiff::Added(right_key, right_value))
            }
            (None, None) => None,
        }
    }
}

impl<T, K, V> SortedByKeyIterator<K, V> for T where T: Iterator<Item = (K, V)> + Sorted {}

impl<K, V> Sorted for btree_map::IntoIter<K, V> {}
impl<K, V> Sorted for btree_map::Iter<'_, K, V> {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn test_diff_by_key() {
        {
            let left: BTreeMap<u64, u64> = Vec::new().into_iter().collect();
            let right: BTreeMap<u64, u64> = Vec::new().into_iter().collect();
            let key_diff: Vec<_> = left.iter().diff_by_key(right.iter()).collect();
            assert_eq!(key_diff, Vec::new());
        }
        {
            let left: BTreeMap<_, _> = vec![(1, 1)].into_iter().collect();
            let right: BTreeMap<_, &'static str> = Vec::new().into_iter().collect();
            let key_diff: Vec<_> = left.iter().diff_by_key(right.iter()).collect();
            assert_eq!(key_diff, vec![KeyDiff::Removed(&1, &1)]);
        }
        {
            let left: BTreeMap<_, usize> = Vec::new().into_iter().collect();
            let right: BTreeMap<_, _> = vec![(1, "a")].into_iter().collect();
            let key_diff: Vec<_> = left.iter().diff_by_key(right.iter()).collect();
            assert_eq!(key_diff, vec![KeyDiff::Added(&1, &"a")]);
        }
        {
            let left: BTreeMap<_, _> = vec![(1, 11)].into_iter().collect();
            let right: BTreeMap<_, _> = vec![(1, "a")].into_iter().collect();
            let key_diff: Vec<_> = left.iter().diff_by_key(right.iter()).collect();
            assert_eq!(key_diff, vec![KeyDiff::Unchanged(&1, &11, &"a")]);
        }
        {
            let left: BTreeMap<_, _> = vec![(1, 1), (3, 3), (5, 5), (7, 7)].into_iter().collect();
            let right: BTreeMap<_, _> = vec![(2, "b"), (4, "d"), (5, "e"), (6, "f")]
                .into_iter()
                .collect();
            let key_diff: Vec<_> = left.iter().diff_by_key(right.iter()).collect();
            assert_eq!(
                key_diff,
                vec![
                    KeyDiff::Removed(&1, &1),
                    KeyDiff::Added(&2, &"b"),
                    KeyDiff::Removed(&3, &3),
                    KeyDiff::Added(&4, &"d"),
                    KeyDiff::Unchanged(&5, &5, &"e"),
                    KeyDiff::Added(&6, &"f"),
                    KeyDiff::Removed(&7, &7),
                ]
            );
        }
    }
}
