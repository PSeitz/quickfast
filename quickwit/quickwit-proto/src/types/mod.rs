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

use std::borrow::Borrow;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
pub use ulid::Ulid;

mod doc_mapping_uid;
mod doc_uid;
mod index_uid;
mod split_id;

pub use doc_mapping_uid::DocMappingUid;
pub use doc_uid::{DocUid, DocUidGenerator};
pub use index_uid::IndexUid;
pub use split_id::SplitId;

/// The size of an ULID in bytes. Use `ULID_LEN` for the length of Base32 encoded ULID strings.
pub(crate) const ULID_SIZE: usize = 16;

pub type IndexId = String;

pub type SourceId = String;

pub type SubrequestId = u32;

pub type AvailabilityZone = Arc<str>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(Arc<str>);

impl NodeId {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(node_id: &str) -> Self {
        Self(Arc::from(node_id))
    }

    pub fn from_arc_str(node_id: Arc<str>) -> Self {
        Self(node_id)
    }
}

impl AsRef<NodeIdRef> for NodeId {
    fn as_ref(&self) -> &NodeIdRef {
        self.deref()
    }
}

impl Borrow<str> for NodeId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Borrow<NodeIdRef> for NodeId {
    fn borrow(&self) -> &NodeIdRef {
        self.deref()
    }
}

impl Deref for NodeId {
    type Target = NodeIdRef;

    fn deref(&self) -> &Self::Target {
        NodeIdRef::from_str(&self.0)
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<NodeId> for Arc<str> {
    fn from(node_id: NodeId) -> Self {
        node_id.0
    }
}

impl PartialEq<&str> for NodeId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeIdRef(str);

impl NodeIdRef {
    /// Transparently reinterprets the string slice as a strongly-typed [`NodeIdRef`].
    pub const fn from_str(node_id: &str) -> &Self {
        let ptr: *const str = node_id;
        // SAFETY: `NodeIdRef` is `#[repr(transparent)]` around a single `str` field, so a `*const
        // str` can be safely reinterpreted as a `*const NodeIdRef`
        unsafe { &*(ptr as *const Self) }
    }

    /// Transparently reinterprets the static string slice as a strongly-typed [`NodeIdRef`].
    pub const fn from_static(node_id: &'static str) -> &'static Self {
        Self::from_str(node_id)
    }

    /// Provides access to the underlying value as a string slice.
    pub const fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for NodeIdRef {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for NodeIdRef {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Display for NodeIdRef {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl PartialEq<NodeIdRef> for NodeId {
    fn eq(&self, other: &NodeIdRef) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<&'_ NodeIdRef> for NodeId {
    fn eq(&self, other: &&NodeIdRef) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<NodeId> for NodeIdRef {
    fn eq(&self, other: &NodeId) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<NodeId> for &'_ NodeIdRef {
    fn eq(&self, other: &NodeId) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<NodeId> for String {
    fn eq(&self, other: &NodeId) -> bool {
        self.as_str() == other.as_str()
    }
}

impl ToOwned for NodeIdRef {
    type Owned = NodeId;

    fn to_owned(&self) -> Self::Owned {
        NodeId(Arc::from(&self.0))
    }
}

#[cfg(feature = "postgres")]
impl From<&NodeId> for sea_query::Value {
    fn from(node_id: &NodeId) -> Self {
        node_id.to_string().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id() {
        let node_id = NodeId::from_str("test-node");
        assert_eq!(node_id.as_str(), "test-node");
        assert_eq!(node_id, NodeIdRef::from_str("test-node"));
    }

    #[test]
    fn test_node_serde() {
        #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
        struct Node {
            node_id: NodeId,
        }
        let node = Node {
            node_id: NodeId::from_str("test-node"),
        };
        let serialized = serde_json::to_string(&node).unwrap();
        assert_eq!(serialized, r#"{"node_id":"test-node"}"#);

        let deserialized = serde_json::from_str::<Node>(&serialized).unwrap();
        assert_eq!(deserialized, node);
    }
}
