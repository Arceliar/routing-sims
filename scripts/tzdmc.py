# Implementation of Thorup and Zwick's compact routing scheme.
# A weight is assigned to each node, equal to degree minus (approximate) coreness
# Landmarks are then selected by sorting nodes by (weight, nodeID) and applying cuts

# WARNING:
#   A broken or dishonest node could do the following pretty trivially:
#     Report incorrect distances, causing other people's routing to break
#     Lie about their weight, incorrectly adding them to the landmark set
#     Silently drop packets
#     Basically anything else that can go wrong in a distance-vector scheme
#   That being said, I think all attacks fall into one of the following 2 categories:
#     1: Something already present in distance-vector schemes
#     2: Wasting resources by adding a node to a table where it doesn't really belong
#       But in the worst case, 2 falls back to a traditional dv-scheme

# NOTE: (was formerly a FIXME)
#   Should probably always keep direct (non-landmark) peers in your own cluster
#   To make sure you never use a longer path to them
#   You need to store some per-peer state anyway, so this doesn't change scaling
#   (Just makes the per-peer space usage higher by a constant factor)
#   This is currently not done just to keep the clusters "clean" for printouts
#     (Including all peers would make some clusters huge, make debugging harder)
#   Actually... that's overly complicated
#   Don't keep them in cluster, but do check direct peers when forwarding packets
#     It's just one more hashtable lookup
#   That gets you the best of both worlds
#   Done, works great, keeping this comment here as a note

# TODO:
#   Simulate a DHT to lookup routing info based on just node IP
#   For now we just assume that we can somehow do a lookup to get a node's nearest location info
#   We also store a lot of stuff in PathInfo that is actually only needed in the DHT

# TODO?
#   Possibly use a (LIFO) backpressure queue to route around congestion

# TODO?
#   Keep periodic updates for direct peers
#   For everything else, explicitely advertise route changes?
#   Would cut down on spam from every announcing that they exist ever 30s or whatever
#   At a minimum, we can stop sending cluster info to landmarks...

# TODO?
#   Anything else we can do to improve on DV routing?
#   (Look at what Babel does, for example, to converge faster after movements.)

# FIXME:
#   Potential routing loop immediately after a topology change.
#   What if a firstHop from a landmark no longer knows a path?
#     They would forward it back to the landmark
#     Infinite loop
#   Similar problem someone later in a route no longer has a route to the dest
#   These stem from reverting from cluster routing to landmark routing
#   Easy solution:
#     Add a bool flag.
#     Set to true when no longer routing towards landmark
#     If no route to dest is known and flag is true, drop packet
#   Maybe there's a clever fix that doesn't require the added flag

###########
# Imports #
###########

import copy
import random
import math
import heapq

######################
# Global "constants" #
######################

INFINITY = 2**64-1
TIMEOUT  = 60
LINK_COST = 1   # Just as a reminder of when a per-link cost would come in

###########
# Classes #
###########

class Msg:
  def __init__(self, sender=None):
    if sender:
      self.info = sender.info
      self.landmarks = sender.landmarks
      self.cluster = sender.cluster
    else:
      self.info = None
      self.landmarks = dict()
      self.cluster = dict()

class PathInfo:
  def __init__(self, nodeID):
    self.nodeID    = nodeID   # Identifier, e.g. IPv6
    self.seq       = 0        # Sequence number (for distance vector routing), possibly network time?
    self.coreness  = 0        # Approximate
    self.weight    = 0        # Used in landmark selection
    self.distance  = 0        # Distance to node
    self.landmarks = dict()   # PathInfo of closest landmarks to this node (dist < closestDist+1), really just need nodeID and firstHop
    self.firstHop  = None     # If this paths is from a landmark, this is the first hop taken from the node on this path
    self.landDist  = INFINITY # Distance to landmark
    self.nextHop   = None     # NodeID of peer that is the next hop in the path
    self.time      = 0        # Time this path info was updated, for timeouts
    self.changed   = True     # If this info changed since our last send

class Node:
  def __init__(self, nodeID):
    self.info      = PathInfo(nodeID) # Self PathInfo
    self.landmarks = dict()           # PathInfo for landmarks used for routing, indexed by nodeID
    self.cluster   = dict()           # PathInfo for nodes within this one's cluster, indexed by nodeID
    self.peers     = dict()           # PathInfo for peers, indexed by nodeID
    self.links     = dict()           # Node objects for peers, indexed by nodeID, to pass messages
    self.messages  = []               # Messages to be handled

  def tick(self):
    # Periodic maintenance work
    # Update local info
    self.info.time += 1
    if not self.info.time % (TIMEOUT/4):
      self.info.seq += 1 # Update seq number periodically
      self.info.changed = True
    changed = False
    if self.updateWeight(): changed = True
    if self.updateLandmarks(): changed = True
    self.cleanCluster()
    # Create message
    msg = Msg()
    msg.landmarks = dict()
    msg.cluster = dict()
    for info in self.landmarks.values():
      if info.changed and not self.info.time - info.time > TIMEOUT: msg.landmarks[info.nodeID] = copy.copy(info)
    for info in self.cluster.values():
      if info.changed and not self.info.time - info.time > TIMEOUT: msg.cluster[info.nodeID] = copy.copy(info)
    msg.info = copy.copy(self.info)
    # Reset changed status for future messages
    self.info.changed = False
    for info in self.landmarks.values():
      info.changed = False
    for info in self.cluster.values():
      info.changed = False
    # Decide if we need to send to peers
    send = msg.info.changed or msg.landmarks or msg.cluster
    # Send to peers
    if send:
      for peer in self.links:
        new = cloneMsg(msg)
        if self.info.nodeID in new.landmarks:
          new.landmarks[self.info.nodeID].firstHop = peer
        if peer in new.landmarks:
          new.cluster = dict() # No need to tell landmarks about our cluster
        self.links[peer].messages.append(new)
    return changed # Tracks if landmark changed, so we know when the sim can stop

  def updateWeight(self):
    # Sets coreness = number for which this node has at least as many peers which do not have < same coreness
    peers = sorted(self.peers.values(), key=lambda x: (x.coreness, x.nodeID), reverse=True)
    newCore = len(peers)
    for index in xrange(len(peers)):
      if peers[index].coreness < index:
        newCore = index
        break
    changed = (self.info.coreness != newCore)
    self.info.coreness = newCore
    # Set weight based on coreness and degree
    newWeight = len(peers) - self.info.coreness
    changed = changed or (self.info.weight != newWeight)
    self.info.weight = newWeight
    return changed

  def updateLandmarks(self):
    # Update landmarks
    self.landmarks[self.info.nodeID] = self.info
    landmarks = sorted(self.landmarks.values(), key=lambda x: (x.weight, x.nodeID), reverse=True)
    reqWeight = 0
    for index in xrange(len(landmarks)):
      landmark = landmarks[index]
      if landmark.weight > index:
        reqWeight = landmark.weight
      elif landmark.weight < reqWeight: del self.landmarks[landmark.nodeID]
    landmarks = self.landmarks.values()
    bestDist = INFINITY
    for landmark in landmarks:
      if self.info.time - landmark.time > 2*TIMEOUT: del self.landmarks[landmark.nodeID]
      if self.info.time - landmark.time > TIMEOUT: continue
      if landmark.distance < bestDist: bestDist = landmark.distance
    newMarks = dict()
    for landmark in landmarks:
      if landmark.distance == bestDist: newMarks[landmark.nodeID] = landmark.firstHop
    changed = (set(self.info.landmarks.keys()) != set(newMarks.keys()))
    self.info.landmarks = newMarks
    self.info.landDist  = bestDist
    return changed

  def cleanCluster(self):
    for info in self.cluster.values():
      if self.info.time - info.time > 2*TIMEOUT: del self.cluster[info.nodeID]
      elif info.nodeID in self.landmarks: del self.cluster[info.nodeID]
      elif self.info.nodeID in self.landmarks: del self.cluster[info.nodeID]
      elif not [v for v in info.landmarks if v in self.landmarks]: del self.cluster[info.nodeID]
      elif not info.distance < info.landDist: del self.cluster[info.nodeID]

  def handleMessages(self):
    while self.messages:
      self.handleMessage(self.messages.pop())

  def handleMessage(self, msg):
    self.peers[msg.info.nodeID] = msg.info
    msg.cluster[msg.info.nodeID] = msg.info
    for info in msg.landmarks.values():
      info.distance += LINK_COST
      info.nextHop = msg.info.nodeID
      info.time = self.info.time
      if info.nodeID in self.landmarks:
        old = self.landmarks[info.nodeID]
        best = bestInfo(old, info)
      else: best = info
      if best == info: self.landmarks[best.nodeID] = best
    for info in msg.cluster.values():
      info.distance += LINK_COST
      info.nextHop = msg.info.nodeID
      info.time = self.info.time
      if info.nodeID in self.cluster:
        old = self.cluster[info.nodeID]
        best = bestInfo(old, info)
      else: best = info
      if best == info: self.cluster[best.nodeID] = best

  def closestLandmark(self, landmarks):
    closest = None
    for landmark in [v for v in landmarks if v in self.landmarks]:
      if not closest or self.landmarks[landmark].distance < closest.distance:
        closest = self.landmarks[landmark]
    if not closest: return None, None
    return closest.nodeID, landmarks[closest.nodeID]

class Packet:
  def __init__(self, source, dest):
    self.sourceID = source.info.nodeID
    self.destID = dest.info.nodeID
    self.destLandmark, self.firstHop = source.closestLandmark(dest.info.landmarks)
    self.carrier = source
    self.hops = [] # Should probably use ttl instead, but this makes debugging easier
  def next(self):
    # Return 0 if still in transit
    # Return 1 if routing finishes
    # Return 2 if no next hop is known (blackhole)
    # Return 3 if routing loop occurs (TTL exceeded)
    if self.carrier.info.nodeID == self.destID:
      # Success
      return 1
    # Get nextHop info
    info = None
    if self.destID in self.carrier.peers: info = self.carrier.peers[self.destID] # Direct peer
    elif self.destID in self.carrier.cluster: info = self.carrier.cluster[self.destID]
    elif self.destID in self.carrier.landmarks: info = self.carrier.landmarks[self.destID]
    elif self.destLandmark in self.carrier.landmarks: info = self.carrier.landmarks[self.destLandmark]
    atLandmark = False
    if self.destLandmark == self.carrier.info.nodeID: atLandmark = True
    # Decide if we can forward the packet
    if not (info and ((info.nextHop in self.carrier.links) or (atLandmark and self.firstHop in self.carrier.links))):
      print "Failed to route, no next hop: {} -> {} ({}) @ {}".format(self.sourceID, self.destID, self.destLandmark, self.carrier.info.nodeID)
      if info:
        infodump = "{} {} {} {} {} {}".format(info.nodeID, info.time, self.carrier.info.time, info.nextHop, info.distance, info.landDist)
        if info.nodeID in self.carrier.landmarks: print "Landmark", infodump
        elif info.nodeID in self.carrier.cluster: print "Cluster", infodump
        else: print "Have info but can't find it...", infodump
      if atLandmark: print "At landmark", self.firstHop, sorted(self.carrier.links.keys())
      return 2
    if self.carrier.info.nodeID in self.hops:
      print "Routing loop: {} -> {} ({}) @ {}".format(self.sourceID, self.destID, self.destLandmark, self.carrier.info.nodeID)
      print "First/next hop and peers", self.firstHop, info.nextHop, sorted(self.carrier.links.keys())
      print "Hops used", self.hops
      return 3
    # Forward the packet
    self.hops.append(self.carrier.info.nodeID)
    if atLandmark: nextHop = self.firstHop
    else: nextHop = info.nextHop
    if nextHop not in self.carrier.links:
      print "Error!", "{} -> {} ({}) @ {}".format(self.sourceID, self.destID, self.destLandmark, self.carrier.info.nodeID)
      print nextHop, sorted(self.carrier.links.keys())
    self.carrier = self.carrier.links[nextHop]
    return 0

####################
# Helper functions #
####################

def bestInfo(info1, info2):
  # Return whichever pathInfo is better, expects old first but may tolerate reverse order
  if info1.seq <= info2.seq:
    old = info1
    new = info2
  else:
    old = info2
    new = info1
  # Test
  if new.seq < old.seq: return old
  if old.seq < new.seq: return new
  if new.distance < old.distance: return new
  # End test
  if new.seq < old.seq: return old
  if new.seq == old.seq and new.distance < old.distance: return new
  if new.seq > old.seq:
    if old.seq+1 < new.seq: return new
    if new.distance <= old.distance: return new
  return old

def dijkstra(nodestore, startingNodeID):
  # Idea to use heapq and basic implementation taken from stackexchange post
  # http://codereview.stackexchange.com/questions/79025/dijkstras-algorithm-in-python
  results = dict()
  queue = [(0, startingNodeID)]
  while queue:
    dist, nodeID = heapq.heappop(queue)
    if nodeID not in results: # Unvisited, otherwise we skip it
      results[nodeID] = dist
      for peer in nodestore[nodeID].links:
        if peer not in results:
          # Peer is also unvisited, so add to queue
          heapq.heappush(queue, (dist+LINK_COST, peer))
  return results

def linkNodes(node1, node2):
  node1.peers[node2.info.nodeID] = node2.info
  node2.peers[node1.info.nodeID] = node1.info
  node1.links[node2.info.nodeID] = node2
  node2.links[node1.info.nodeID] = node1

def randomly(seq):
    shuffled = list(seq)
    random.shuffle(shuffled)
    return shuffled

def cloneMsg(msg):
  new = Msg()
  new.info = copy.copy(msg.info)
  for info in msg.landmarks.values():
    new.landmarks[info.nodeID] = copy.copy(info)
  for info in msg.cluster.values():
    new.cluster[info.nodeID] = copy.copy(info)
  return new

############################
# Store topology functions #
############################

def makeStoreSquareGrid(sideLength, randomize=True):
  store = dict()
  nodeIDs = list(range(sideLength*sideLength))
  if randomize: random.shuffle(nodeIDs)
  for nodeID in nodeIDs:
    store[nodeID] = Node(nodeID)
  for index in xrange(len(nodeIDs)):
    if (index % sideLength != 0): linkNodes(store[nodeIDs[index]], store[nodeIDs[index-1]])
    if (index >= sideLength): linkNodes(store[nodeIDs[index]], store[nodeIDs[index-sideLength]])
  print "Grid store created, size {}".format(len(store))
  return store

def makeStoreHubSpoke(nodes):
  store = dict()
  for nodeID in xrange(nodes):
    store[nodeID] = Node(nodeID)
    if nodeID != 0: linkNodes(store[0], store[nodeID])
  print "Hub and spoke network created, size {}".format(len(store))
  return store

def makeStoreFullMesh(nodes, spokes=0):
  store = dict()
  for nodeID in xrange(nodes):
    store[nodeID] = Node(nodeID)
    for peerID in store:
      if nodeID == peerID: continue
      linkNodes(store[nodeID], store[peerID])
  if spokes: # Pathelogical thing, add a spoke to each mesh node
    for nodeID in store.keys():
      for spokeID in xrange(spokes):
        newID = str(nodeID)+"-spoke-"+str(spokeID)
        store[newID] = Node(newID)
        linkNodes(store[nodeID], store[newID])
  print "Full mesh network created, size {}".format(len(store))
  return store

def makeStoreCaidaGraph(pathToGraph):
  #Existing network graphs, in caida.org's format (undirected ASx ASy pairs per line)
  with open(pathToGraph, "r") as f:
    inData = f.readlines()
  store = dict()
  for line in inData:
    nodes = map(int, line.rstrip('\n').split(' ')[0:2])
    if nodes[0] not in store: store[nodes[0]] = Node(nodes[0])
    if nodes[1] not in store: store[nodes[1]] = Node(nodes[1])
    linkNodes(store[nodes[0]], store[nodes[1]])
  print "CAIDA graph successfully imported, size {}".format(len(store))
  return store

def makeStoreHypeGraph(pathToGraph):
  # Expects the format found in http://www.fc00.org/static/graph.json
  import json
  with open(pathToGraph, "r") as f:
    inData = json.loads(f.read())
  store = dict()
  for edge in inData[u'edges']:
    sourceID = str(edge[u'sourceID'])
    destID = str(edge[u'targetID'])
    if sourceID not in store: store[sourceID] = Node(sourceID)
    if destID not in store: store[destID] = Node(destID)
    if destID not in store[sourceID].links: linkNodes(store[sourceID], store[destID])
  print "Hyperboria graph successfully imported, size {}".format(len(store))
  return store

##################
# Main execution #
##################

def main(log=False):
  # Make any randomness deterministic, to help with reproducibility when debugging
  random.seed(12345)
  # Create store
  print "Creating Store..."
  #store = makeStoreSquareGrid(32)
  #store = makeStoreHubSpoke(64)
  #store = makeStoreFullMesh(64)
  #store = makeStoreHypeGraph("graph.json") # See: http://www.fc00.org/static/graph.json
  store = makeStoreCaidaGraph("bgp_tables") # Internet AS graph, from bgp tables
  #store = makeStoreCaidaGraph("skitter") # Internet AS graph, from skitter
  print "Store Created"
  for node in store.values():
    node.info.time = random.randint(0, TIMEOUT) # Start w/ random node time
  step = 0
  lastChange = 0
  verbose = True # Print out per-node store sizes when done
  dump = False # Dump debugging store info when finished
  print "Beginning network simulation..."
  while step - lastChange < 2*TIMEOUT:
    step += 1
    for node in store.values():
      if node.tick(): lastChange = step
    for node in store.values():
      node.handleMessages()
    print "Step {}, last change {}".format(step, lastChange)
  print "Network should now have converged"
  # Find min/avg/max store sizes:
  minLandmarks = INFINITY
  minCluster = INFINITY
  avgLandmarks = 0
  avgCluster = 0
  maxLandmarks = 0
  maxCluster = 0
  for node in store.values():
    node.cleanCluster()
    for peer in node.peers:
      if peer in node.cluster:
        # This is a hack to ignore cluster nodes that are also peers
        # Purely to get a better idea of how space is being used
        #del node.cluster[peer]
        pass
    minLandmarks  = min(minLandmarks, len(node.landmarks))
    minCluster    = min(minCluster, len(node.cluster))
    avgLandmarks += len(node.landmarks)
    avgCluster   += len(node.cluster)
    maxLandmarks  = max(maxLandmarks, len(node.landmarks))
    maxCluster    = max(maxCluster, len(node.cluster))
  avgLandmarks /= float(max(1, len(store)))
  avgCluster /= float(max(1, len(store)))
  # Test paths
  ids = sorted(store.keys())
  maxHops = 0
  totalHops = 0
  totalExpected = 0
  avgStretch = 0
  maxStretch = 0
  totalChecked = 0
  nodeCount = 0
  diameter = 0
  if log: f = open("lengths.csv", "w")
  for sourceID in ids:
    nodeCount += 1
    print "Testing paths from node {}/{} ({})".format(nodeCount, len(store), sourceID)
    expected = dijkstra(store, sourceID)
    for destID in ids:
      if sourceID == destID: continue     # Don't test self-route
      if destID not in expected: continue # No route exists
      packet = Packet(store[sourceID], store[destID])
      hops = 0
      while True:
        status = packet.next()
        if status == 0:
          hops += 1
          continue
        elif status == 1:
          # handle success
          maxHops = max(maxHops, hops)
          totalHops += hops
          diameter = max(diameter, expected[destID])
          totalExpected += expected[destID]
          stretch = float(hops)/max(1, expected[destID])
          avgStretch += stretch
          maxStretch = max(maxStretch, stretch)
          totalChecked += 1
          if log:
            result = (sourceID, destID, expected[destID], hops)
            line = ",".join(map(str, result)) + "\n"
            f.write(line)
        else:
          print status
          dump = True
          assert False # DEBUG
        break
  if log: f.close()
  avgStretch /= max(1, totalChecked)
  if verbose: # Verbose nodesore output
    for node in sorted(store.values(), key=lambda x: (len(x.landmarks), len(x.cluster), x.info.nodeID)):
      print "Node {}, weight {}, cluster {}, peers {}".format(node.info.nodeID, node.info.weight, len(node.cluster), len(node.peers))
    for node in ids:
      if node in store[node].landmarks:
        print "Landmark {}, weight {}, peers {}".format(node, store[node].info.weight, len(store[node].peers))
  if dump: # Debugging nodestore output
    for node in store.values():
      print "Node", node.info.nodeID, node.info.landmark, "Peers", sorted(node.links.keys())
      print "Cluster", map(lambda x: (x.nodeID, x.distance), sorted(node.cluster.values(), key=lambda x: x.nodeID))
      print "Landmarks", map(lambda x: (x.nodeID, x.distance, x.firstHop), sorted(node.landmarks.values(), key=lambda x: x.nodeID))
      print # Blank line
  print "Max hops used / graph diameter: {} / {}".format(maxHops, diameter)
  print "Stretch (Avg / Max): {} / {}".format(avgStretch, maxStretch)
  print "Bandwidth usage: {}".format(float(totalHops)/max(1, totalExpected))
  print "Node min/avg/max store sizes: {}+{} / {}+{} / {}+{}".format(minLandmarks, minCluster, avgLandmarks, avgCluster, maxLandmarks, maxCluster)
  return store

main()

