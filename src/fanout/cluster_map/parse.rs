//! Parsing of `CLUSTER NODES` reply lines. Pure, so it is tested without a server.

use super::node::{NodeId, NodeLocation, SocketAddress};
use smallvec::SmallVec;
use std::net::{IpAddr, Ipv4Addr};
use valkey_module::VALKEYMODULE_NODE_ID_LEN;

/// Inline-capacity buffer for a master's slot ranges: a settled cluster has
/// one contiguous range per shard, so four covers all but heavily fragmented
/// topologies without a heap allocation per line.
pub(super) type SlotRangeBuf = SmallVec<[(u16, u16); 4]>;

/// A single parsed line of `CLUSTER NODES` output.
pub(super) struct ParsedNodeLine {
    pub(super) id: NodeId,
    /// `None` when the node has no usable address (dropped for remote nodes).
    pub(super) socket_address: Option<SocketAddress>,
    pub(super) is_master: bool,
    pub(super) is_local: bool,
    /// Not flagged `fail`/`fail?`, and (for a remote node) its link is `connected`.
    pub(super) is_healthy: bool,
    /// The master is importing at least one slot (`[slot-<-id]`).
    pub(super) imports_slots: bool,
    /// True when this replica has a resolvable master id.
    pub(super) has_master: bool,
    pub(super) master_id: NodeId,
    /// Owned slot ranges (masters only); import/export markers are excluded.
    pub(super) slot_ranges: SlotRangeBuf,
}

impl ParsedNodeLine {
    pub(super) fn location(&self) -> NodeLocation {
        if self.is_local {
            NodeLocation::Local
        } else {
            NodeLocation::Remote
        }
    }
}

/// Parse one `CLUSTER NODES` line:
/// `<id> <ip:port@cport[,aux]> <flags> <master> <ping> <pong> <epoch> <link> [slots...]`
///
/// Returns `None` when the line is too malformed to trust (too few fields,
/// unparseable node id, or no single clear role).
pub(super) fn parse_node_line(line: &str, my_node_id: &NodeId) -> Option<ParsedNodeLine> {
    // Walk the fields without collecting them: only the first four and the
    // trailing slot specs carry information.
    let mut fields = line.split_ascii_whitespace();
    let id_field = fields.next()?;
    let addr_field = fields.next()?;
    let flags_field = fields.next()?;
    let master_field = fields.next()?;
    // Skip ping-sent/pong-recv/config-epoch and require link-state, so a line
    // with fewer than 8 fields is rejected just as the indexed form did.
    let mut fields = fields.skip(3);
    let link_state = fields.next()?;

    if id_field.len() != VALKEYMODULE_NODE_ID_LEN as usize {
        return None;
    }
    let id = NodeId::from_bytes(id_field.as_bytes());

    let mut is_master = false;
    let mut is_replica = false;
    let mut is_myself = false;
    let mut noaddr = false;
    let mut failed = false;
    for flag in flags_field.split(',') {
        match flag {
            "master" => is_master = true,
            "slave" | "replica" => is_replica = true,
            "myself" => is_myself = true,
            "noaddr" => noaddr = true,
            "fail" | "fail?" => failed = true,
            _ => {}
        }
    }
    // Exactly one role must be declared (rejects handshake/unknown lines).
    if is_master == is_replica {
        return None;
    }
    let is_local = is_myself || &id == my_node_id;
    // The local node is reachable by definition; a remote one only over a live link.
    let is_healthy = !failed && (is_local || link_state == "connected");

    let socket_address = parse_node_address(addr_field, noaddr, is_local);

    let has_master = is_replica && master_field.len() == VALKEYMODULE_NODE_ID_LEN as usize;
    let master_id = if has_master {
        NodeId::from_bytes(master_field.as_bytes())
    } else {
        NodeId::default()
    };

    let mut slot_ranges = SlotRangeBuf::new();
    let mut imports_slots = false;
    if is_master {
        for spec in fields {
            // Importing/migrating markers like "[12000-<-<id>]" settle no ownership.
            if spec.starts_with('[') {
                imports_slots |= spec.contains("-<-");
                continue;
            }
            if let Some((a, b)) = spec.split_once('-') {
                if let (Ok(a), Ok(b)) = (a.parse::<u16>(), b.parse::<u16>()) {
                    slot_ranges.push((a, b));
                }
            } else if let Ok(n) = spec.parse::<u16>() {
                slot_ranges.push((n, n));
            }
        }
    }

    Some(ParsedNodeLine {
        id,
        socket_address,
        is_master,
        is_local,
        is_healthy,
        imports_slots,
        has_master,
        master_id,
        slot_ranges,
    })
}

/// Parse the `ip:port@cport[,aux...]` address field.
///
/// Returns `None` for a remote node with no usable address (it can't be
/// targeted). The local node keeps a placeholder address instead, since a
/// node never dials itself but must still appear in its own map.
fn parse_node_address(field: &str, noaddr: bool, is_local: bool) -> Option<SocketAddress> {
    // Drop the optional ",hostname"/aux fields, then the "@cport" bus suffix.
    let addr = field.split(',').next().unwrap_or(field);
    let host_port = addr.split('@').next().unwrap_or(addr);
    // Split at the last ':' so IPv6 addresses (which contain ':') parse.
    // Port 0 (or one that does not parse) can never be dialled.
    let (ip_str, port) = match host_port.rsplit_once(':') {
        Some((ip, port)) => (ip, port.parse::<u16>().unwrap_or(0)),
        None => (host_port, 0),
    };

    let local_placeholder = || {
        is_local.then_some(SocketAddress {
            primary_endpoint: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port,
        })
    };

    if noaddr || ip_str.is_empty() || port == 0 {
        return local_placeholder();
    }
    match ip_str.parse::<IpAddr>() {
        Ok(primary_endpoint) => Some(SocketAddress {
            primary_endpoint,
            port,
        }),
        Err(_) => local_placeholder(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const M1: &str = "1111111111111111111111111111111111111111";
    const M2: &str = "2222222222222222222222222222222222222222";

    fn id(s: &str) -> NodeId {
        NodeId::from_bytes(s.as_bytes())
    }

    #[test]
    fn test_ipv6_and_hostname_address() {
        let line = format!("{M1} ::1:7001@17001,host.example.com master - 0 0 1 connected 0-16383");
        let rec = parse_node_line(&line, &id(M2)).unwrap();
        let addr = rec.socket_address.unwrap();
        assert_eq!(addr.primary_endpoint, "::1".parse::<IpAddr>().unwrap());
        assert_eq!(addr.port, 7001);
        assert!(rec.is_master);
        assert!(!rec.is_local);
    }

    #[test]
    fn test_parse_node_line_rejects_malformed() {
        // Too few fields.
        assert!(parse_node_line("too short line here", &id(M1)).is_none());
        // Node id of the wrong length.
        assert!(
            parse_node_line(
                "short 127.0.0.1:7001@17001 master - 0 0 1 connected",
                &id(M1)
            )
            .is_none()
        );
        // No clear role flag.
        let line = format!("{M1} 127.0.0.1:7001@17001 handshake - 0 0 1 connected");
        assert!(parse_node_line(&line, &id(M2)).is_none());
    }
}
