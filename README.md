# routing-sims
Misc scripts to simulate different routing schemes.

Of the schemes that I've been testing, the one I've been calling "tzdmc" is currently the best performing, both in terms of routing table size and average stretch, for the real-world graphs I have tested it on.
The goal is to have something that perform well in practice, with failure modes that revert to something well understood (in this case, the usual shortest-path routing that current schemes employ).

## scripts/tzdmc.py

This is modeled after the Thorup/Zwick compact universal compact routing scheme. More generally, it's in the class of schemes based on Cowen's first universal compact routing scheme.
It's an attempt to remove the need for a "routing domain" with a full view of the network graph. This is to make the scheme more suitable to dynamic networks (although the cost of converging after a change to the graph is not considered).

The scheme requires no full view, but it is no longer guaranteed to be compact. Stretch remains bounded at no more than 3 for arbitrary network graphs, but the routing table size may not be sub-linear for general graphs.
For highly symmetric topologies (rings, grids, full meshes), the landmark set can expand to encompass the entire network, and the scheme reverts to shortest path routing.

In practice, routing table sizes are quite small for real world graphs, but I do not have enough data points to extrapolate how it scales.
I think the landmark set size should scale no faster than soft-O(n^(1/(1+lambda))) for scale-free graphs, but I have not determined how the cluster sizes scale.
The bound for landmark size comes from the degree distribution of links in a power-law graph, the number of landmarks that are selected from the node "weight" distribution, and the fact that each node's weight is bounded by its degree.

The differences from the normal TZ scheme are briefly discussed below, along with performance on a simulation of the AS-level internet topology.

### Scheme Description

#### Packet Forwarding

The packet forwarding procedure is slightly modified from the TZ scheme, to reduce stretch a little further.
A separate hashtable lookup is used to check if destinations are a peer ("neighbor" in the usual terminology from graph theory), in the local cluster, or a landmark.
Forwarding uses the following priorities:

  1. If the destination is a direct peer, then it is forwarded to that peer.
  2. Else if the destination is in the node's cluster, forward to the next hop.
  3. Else if the destination is in the node's set of known landmarks, forward to the next hop.
  4. Else if this node is the landmark for the destination, forward based on the port number in the packet's header.
  5. Else drop the packet (this should not happen, so the simulation just panics).

Checking peers is a deviation from the usual TZ forwarding procedure.
My rationale is that each node needs to store some state per peer anyway (for the distance-vector routing portion of the scheme).
This is just exploiting extra information that is present at each node, but not actually in the routing table, to slightly improve routing performance.
In particular, you presumably need to store at least a port number per peer, and a hashtable is a natural structure to use for those lookups anyway.

#### Landmarks

Landmark selection is significantly different, to remove the full view requirement.
An iterative scheme is used to calculate an approximation of each node's coreness.
Each node is assigned a weight, equal to degree minus coreness ("dmc" in the scheme's name).
A target weight W is selected, equal to the maximum weight for which at least W nodes have weight >= W.
The nodes with weight >= W are kept as landmarks.
This landmark selection process is done independently at each node, using only information in the local routing table.
Nodes propagate information about the selected landmarks to their peers, and the network converges on one set of landmarks to use.

#### Additional Details

It is possible to extend the scheme for name-independent routing, but this has not been implemented in the simulation.
It is assumed that the network would include some mechanism to allow header lookups based on flat IDs (e.g. kademlia dht, key = cryptographically generated address, value = header w/ timestamp and cryptographic signature).
It is also assumed that a node which is equidistant from multiple landmarks will keep a header for each landmark, and send all of them in dht lookup responses.
The sender may then select the header which uses the nearest landmark, which helps to further reduce the stretch of the routing scheme (as per the scheme from Strowes's thesis, this optimization is included in the simulation).
Senders should keep the header cached and check for updates periodically.
Use of a dht would generally introduce an additional logarithmic term to the memory requirement for each node.

For the pathfinding itself, the simulation uses a distance vector routing protocol with a soft-state approach.
Each node periodically sends a sequence-numbered message to its direct peers (neighbors).
If no message is received for a significant amount of time, then the node times out and the information is removed from the routing table.
This was the simplest thing I could think of to implement for the simulation; something better should be used for a real implementation.

### Performance

Several compact routing schemes have been [studied](http://arxiv.org/pdf/0708.2309) on the 9204-node "skitter" AS-level internet topology graph, available from [CAIDA](https://www.caida.org/), so I have decided to use that for comparison.
I'm not sure exactly which DIMES dataset was used in that study, so the scheme has not been tested on that topology.

When running the tzdmc scheme on the skitter network graph, the average routing table size for each node is 64, with a maximum of 98 for any node.
Although (multiplicative) stretch is bounded at 3.0, the maximum observed stretch is only 2.0, with an average stretch of 1.001 on this graph.
