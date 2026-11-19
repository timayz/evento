//! A node's part of the work spread over the nodes up.
//!
//! Background work that any node can do (a sweep over a read model, a mail
//! to send) is done once when each piece falls to one node: the node at
//! place `index` among `of` takes the pieces whose id hashes to it
//! ([`Share::owns`]). The places come from the nodes heard from lately
//! ([`Cluster::live`](crate::Cluster::live)), so a node that stops has its
//! part taken over once it has been silent long enough, and gets it back
//! when it returns.
//!
//! Nothing here is agreed through consensus: two nodes that disagree for a
//! moment on who is up do one piece twice, or leave it for the next look.
//! The work itself makes sure neither harms it (a claim written through the
//! cluster, a handler that bears being run twice).

use crate::clock::NodeId;
use crate::host::LiveNode;

/// A node's place among the nodes a piece of work is spread over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Share {
    /// This node's place among them, from 0.
    pub index: u64,
    /// How many there are, at least 1.
    pub of: u64,
}

impl Share {
    /// A node on its own: everything is its.
    pub const ALONE: Self = Self { index: 0, of: 1 };

    /// `me`'s place among the nodes of `live`: the work every node can do,
    /// whatever it serves.
    pub fn among(me: NodeId, live: &[LiveNode]) -> Self {
        Self::place(me, live, |_| true)
    }

    /// `me`'s place among the nodes of `live` whose subscriptions follow the
    /// events written under `key` (`None`: without one): the work a
    /// subscription is told about, which must fall to a node that sees its
    /// event. `me` always counts, so it owns its part of what it alone saw;
    /// a node that has not said what it serves yet counts for no key until
    /// it does.
    pub fn among_serving(me: NodeId, live: &[LiveNode], key: Option<&str>) -> Self {
        Self::place(me, live, |node| node.serves(key))
    }

    fn place(me: NodeId, live: &[LiveNode], counts: impl Fn(&LiveNode) -> bool) -> Self {
        let mut index = 0;
        let mut of = 0;
        for node in live {
            if node.id != me && !counts(node) {
                continue;
            }
            if node.id == me {
                index = of;
            }
            of += 1;
        }
        Self {
            index,
            of: of.max(1),
        }
    }

    /// Whether the work `id` names belongs to this node.
    pub fn owns(&self, id: &str) -> bool {
        u64::from(shard_of(id)) % self.of == self.index
    }
}

/// Where the work `id` names falls among the nodes (FNV-1a of the id): the
/// same on every node and in every build, which the standard hasher does not
/// promise. A read model that hands work out keeps it in a column, so a
/// query takes a node's part with `shard % of = index`.
pub fn shard_of(id: &str) -> u32 {
    id.bytes().fold(0x811c_9dc5, |hash: u32, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live(nodes: &[(u64, Option<&[Option<&str>]>)]) -> Vec<LiveNode> {
        nodes
            .iter()
            .map(|(id, keys)| LiveNode {
                id: NodeId(*id),
                label: None,
                keys: keys.map(|keys| keys.iter().map(|key| key.map(str::to_owned)).collect()),
            })
            .collect()
    }

    #[test]
    fn the_hash_is_fnv_1a() {
        // The reference vectors: what every build must agree on.
        assert_eq!(shard_of(""), 0x811c_9dc5);
        assert_eq!(shard_of("a"), 0xe40c_292c);
        assert_eq!(shard_of("foobar"), 0xbf9c_f968);
    }

    #[test]
    fn every_piece_belongs_to_one_node() {
        let ids: Vec<String> = (0..300).map(|n| format!("link-{n}")).collect();
        for of in 1..=5 {
            let mut held = 0;
            for index in 0..of {
                let share = Share { index, of };
                let own = ids.iter().filter(|id| share.owns(id)).count();
                // Each node holds its part, near enough.
                assert!(own >= 300 / of as usize / 2, "{own} of 300 on {index}/{of}");
                held += own;
            }
            assert_eq!(held, ids.len());
        }
    }

    #[test]
    fn a_place_among_every_node_goes_by_id_order() {
        let live = live(&[(0, None), (2, None), (3, None)]);
        assert_eq!(Share::among(NodeId(2), &live), Share { index: 1, of: 3 });
        assert_eq!(Share::among(NodeId(3), &live), Share { index: 2, of: 3 });
        // A node not listed is alone, rather than without a place.
        assert_eq!(Share::among(NodeId(9), &[]), Share::ALONE);
    }

    #[test]
    fn a_place_among_the_nodes_of_a_key_counts_those_that_serve_it() {
        let live = live(&[
            (0, Some(&[Some("eu")])),
            (1, Some(&[Some("us"), Some("eu")])),
            (2, Some(&[None])),
            (3, None),
        ]);
        let eu = Some("eu");
        assert_eq!(
            Share::among_serving(NodeId(0), &live, eu),
            Share { index: 0, of: 2 }
        );
        assert_eq!(
            Share::among_serving(NodeId(1), &live, eu),
            Share { index: 1, of: 2 }
        );
        assert_eq!(
            Share::among_serving(NodeId(1), &live, Some("us")),
            Share::ALONE
        );
        // The events written without a key have nodes of their own.
        assert_eq!(Share::among_serving(NodeId(2), &live, None), Share::ALONE);
        // A node counts itself among the nodes of a key it does not serve,
        // and one that has not said what it serves counts for none.
        assert_eq!(
            Share::among_serving(NodeId(2), &live, eu),
            Share { index: 2, of: 3 }
        );
        assert_eq!(
            Share::among_serving(NodeId(3), &live, eu),
            Share { index: 2, of: 3 }
        );
    }
}
