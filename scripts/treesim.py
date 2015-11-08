# Tree routing scheme (named Yggdrasil, after the world tree from Norse mythology)
# Steps:
#   1: Pick any node, here I'm using highest nodeID
#   2: Build spanning tree, each node stores path back to root
#     Optionally with weights for each hop
#   3: Keep track of extra shortcuts where the tree stretch is too high
#   4: Use distance of shortcuts + remaining tree distance as a distance metric
#   5: Perform (modified) greedy lookup via this metric for each direction (A->B and B->A)
#   6: Source-route traffic using the shorter/better of those two paths

# Note:
#   Currently set to do worstCasePathLength lookups instead of the greedy ones
#   To test if/when max stretch bounds are violated
#   Switch back to greedy ones to see average stretch in practice (much better)

# TODO
#   We keep the smallest amount of info I could find that doesn't break stretch bound
#   There's an alternative subset of info to store, but it isn't perfect
#   Keeps much less info, but the stretch bound is violated for some paths
#   Caused by something like the following:
#     B->X->D is known by B, B does not care about X
#     B->C->D also exists, B does care about C
#     A->B->X->D is long enough that A doesn't care about it
#     A->B->C is short enough that A would care about it, if A heard
#   Attempted fix: have B switch to using B->C->D instead of B->X->D
#   Problem: This caused oscillation in some networks
#   It's rare for this to be a problem (most paths are fine)
#   If table size with the normal requirement becomes too high, switch to this
#     On a node-by-node basis
#     It will let you drop paths that are unlikely to cause stretch issues
#   In the long term, figure out the minimum set needed is, just use that
#     This is probably difficult
#     Problems come from interactions between nodes, not 1 node's view

# TODO:
#   Investigate using Peleg's distance labeling scheme for trees (also used in the bc scheme)
#   That would mean we could drop the path-vector aspects and use hybrid tree+dv
#   (Probably not compatible with using weighted graphs)
#   (Harder to secure, but less resource usage, so could be nice for sensor nets)

# TODO:
#   Make better use of drop?
#   In particular, we should be ignoring *all* recently dropped *paths* to the root
#     To minimize route flapping
#     Not really an issue in the sim, but needed for a real network

import random
import heapq

#############
# Constants #
#############

# Maximum allowed path length: y = m*x + b
# y = maximum path length allowed in the scheme
# x = shortest path length between two nodes in the network
# m = multiplicative stretch, MAX_MS
# b = additive stretch, MAX_AS
MAX_MS = 3
MAX_AS = 3

# Reminder of where link cost comes in
LINK_COST = 1

# Timeout before dropping something, in simulated seconds
TIMEOUT = 60

###########
# Classes #
###########

class PathInfo:
  def __init__(self, nodeID):
    self.nodeID = nodeID   # e.g. IP
    self.coords = []       # Position in tree
    self.tstamp = 0        # Timestamp from sender, to keep track of old vs new info
    # The above should be signed
    self.path   = [nodeID] # Path to node (in path-vector route)
    self.time   = 0        # Time info was updated, to keep track of e.g. timeouts
  def clone(self):
    # Return a deep-enough copy of the path
    clone = PathInfo(None)
    clone.nodeID = self.nodeID
    clone.coords = self.coords[:]
    clone.tstamp = self.tstamp
    clone.path = self.path[:]
    clone.time = self.time
    return clone
# End class PathInfo

class Node:
  def __init__(self, nodeID):
    self.info  = PathInfo(nodeID) # Self NodeInfo
    self.root  = None             # PathInfo to node at root of tree
    self.clus  = dict()           # PathInfo to nodes nearby relative to tree
    self.drop  = dict()           # PathInfo to nodes from clus that have timed out
    self.peers = dict()           # PathInfo to peers
    self.links = dict()           # Links to peers (to pass messages)
    self.msgs  = []               # Said messages

  def tick(self):
    # Do periodic maintenance stuff, including push updates
    self.info.time += 1
    if self.info.time > self.info.tstamp + TIMEOUT/4:
      # Update timestamp at least once every 1/4 timeout period
      # This should probably be randomized in a real implementation
      self.info.tstamp = self.info.time
    changed = False # Used to track when the network has converged
    #changed |= self.cleanRoot()
    if self.cleanRoot():
      print "DEBUG:", self.info.nodeID, "cleanRoot"
      changed = True
    #changed |= self.cleanCluster()
    if self.cleanCluster():
      print "DEBUG:", self.info.nodeID, "cleanCluster"
      changed = True
    self.cleanDropped()
    msg = self.createMessage()
    self.sendMessage(msg)
    return changed

  def cleanRoot(self):
    changed = False
    if self.root and self.info.time - self.root.time > TIMEOUT:
      self.drop[self.root.nodeID] = self.root
      self.root = None
      changed = True
    if not self.root or self.root.nodeID < self.info.nodeID:
      # No need to drop someone who'se worse than us
      self.info.coords = [self.info.nodeID]
      self.root = self.info.clone()
      changed = True
    elif self.root.nodeID == self.info.nodeID:
      self.root = self.info.clone()
    return changed

  def cleanCluster(self):
    changed = False
    nodes = self.clus.values()
    for node in nodes:
      if self.info.time - node.time > TIMEOUT:
        # Timed out
        self.drop[node.nodeID] = node
        del self.clus[node.nodeID]
        changed = True
      elif node.coords[0] != self.root.nodeID:
        # Wrong tree, something's unconverged, wait for that to get fixed first
        del self.clus[node.nodeID]
        changed = True
      elif not self.mustStorePath(node):
        # The tree route is good enough
        del self.clus[node.nodeID]
        changed = True
    return changed

  def cleanDropped(self):
    nodes = self.drop.values()
    for node in nodes:
      if self.info.time - node.time > 4*TIMEOUT:
        del self.drop[node.nodeID]
    return None

  def mustStorePath(self, info):
    if (info.coords[0] != self.info.coords[0]): return False
    assert(info.path[-1] == self.info.nodeID)
    tPath = treePath(self.info.coords, info.coords) # DEBUG
    assert tPath[-1] == self.info.nodeID # DEBUG
    assert tPath[0] == info.nodeID # DEBUG
    pathLength = len(info.path)-1 # The last hop is ourself, doesn't count
    treeLength = treeDist(self.info.coords, info.coords)
    maxAllowedHops = pathLength*MAX_MS + MAX_AS
    if not maxAllowedHops < treeLength: return False # Stretch is tolerable
    hasBetterPath = False
    for nodeID in self.clus: # Possibly sort keys first? (Is oscillation possible?)
      if nodeID == info.nodeID: continue
      node = self.clus[nodeID]
      nodePathLength = len(node.path)-1
      infoNodeSeparation = treeDist(node.coords, info.coords)
      infoDistViaNode = nodePathLength + infoNodeSeparation
      if infoDistViaNode < treeLength:
        # Node would be a better route to info than via us
        maxAllowedNodeHops = nodePathLength*MAX_MS + MAX_AS
        if not maxAllowedHops < maxAllowedNodeHops + infoNodeSeparation:
          # The worst-case path via node would have tolerable stretch
          #   (Can't use actual path, because our peers may think differently)
          #   (There are probably ways to throw out more info than this and still work)
          # If routing table size becomes too large, then instead check:
          #   if not maxAllowedHops < infoDistViaNode:
          # That's unsafe (it sometimes causes stretch to be violated)
          # But it tends to keep stretch under the bound for the vast majority of paths
          # So use that to prioritize which info to keep or drop
          hasBetterPath = True
    if not hasBetterPath: return True
    return False

  def createMessage(self):
    # Message is just a list
    # First element in the list is always the sender
    # Second element is the root
    # The rest are cluster nodes, in no particular order
    # We will .clone() everything during the send operation
    msg = []
    msg.append(self.info)
    msg.append(self.root)
    for node in self.clus.values():
      msg.append(node)
    return msg

  def sendMessage(self, msg):
    for link in self.links.values():
      newMsg = map(lambda info: info.clone(), msg)
      link.msgs.append(newMsg)
    return None

  def handleMessages(self):
    changed = False
    while self.msgs:
      changed |= self.handleMessage(self.msgs.pop())
    return changed

  def handleMessage(self, msg):
    changed = False
    for node in msg:
      if self.info.nodeID in node.path: continue # Loopy route
      node.path.append(self.info.nodeID)
      node.time = self.info.time
      assert node.path[-1] == self.info.nodeID
      if node.nodeID in self.drop:
        if node.tstamp <= self.drop[node.nodeID].tstamp:
          # This is old, skip it
          continue
        else:
          # It's new
          pass
      if not self.mustStorePath(node):
        # The tree route is good enough
        continue
      updateNode = False
      if not node.nodeID in self.clus:
        updateNode = True
        changed = True
      else:
        if node.path == self.clus[node.nodeID].path:
          self.clus[node.nodeID] = node
        if len(node.path) < len(self.clus[node.nodeID].path):
          self.clus[node.nodeID] = node
          changed = True
      if updateNode:
        self.clus[node.nodeID] = node
        if node.nodeID in self.drop: del self.drop[node.nodeID]
    sender = msg[0]
    assert sender.time == self.info.time
    self.peers[sender.nodeID] = sender
    root = msg[1]
    assert root.time == self.info.time or self.info.nodeID in root.path
    # Decide if we want to update the root
    updateRoot = False
    if root.nodeID in self.drop and self.drop[root.nodeID].tstamp >= root.tstamp: pass
    elif not self.root: updateRoot = True
    elif self.root.nodeID < root.nodeID: updateRoot = True
    elif self.root.nodeID != root.nodeID: pass
    elif self.root.tstamp > root.tstamp: pass
    elif len(root.path) < len(self.root.path): updateRoot = True
    # TODO case where current path missed at least 1 update
    # (Without it, we wait for the path to time out)
    elif self.root.path != root.path: pass
    elif self.root.tstamp < root.tstamp: updateRoot = True
    if updateRoot:
      if not self.root or self.root.path != root.path: changed = True
      self.root = root
      self.info.coords = self.root.path
    return changed

  def lookup(self, dest):
    best = self.info
    bestDist = treeDist(self.info.coords, dest.coords)
    # <= means we prefer to replace ourself w/ a peer, and a peer w/ a cluster node
    # That disfavors routing on the tree, which (hopefully) reduces congestion
    for node in self.peers.itervalues():
      dist = len(node.path)-1 + treeDist(node.coords, dest.coords)
      if dist <= bestDist:
        best = node
        bestDist = dist
    for node in self.clus.itervalues():
      dist = len(node.path)-1 + treeDist(node.coords, dest.coords)
      if dist <= bestDist:
        best = node
        bestDist = dist
    if best.nodeID != self.info.nodeID:
      #print "DEBUG BEST:", self.info.nodeID, best.path[::-1]
      next = best.path[-2]
    else:
      tPath = treePath(self.info.coords, dest.coords)
      #print "DEBUG TREE:", self.info.nodeID, tPath
      next = tPath[-2]
    assert next in self.peers
    return next

  def getWorstCasePathLength(self, dest):
    best = self.info
    bestDist = treeDist(self.info.coords, dest.coords)
    # <= means we prefer to replace ourself w/ a peer, and a peer w/ a cluster node
    # That disfavors routing on the tree, which (hopefully) reduces congestion
    for node in self.clus.itervalues():
      dist = len(node.path)-1 + treeDist(node.coords, dest.coords)
      if dist <= bestDist:
        best = node
        bestDist = dist
    return bestDist

# End class Node

####################
# Helper Functions #
####################

def treePath(source, dest):
  # Return path with source at head and dest at tail
  shortest = min(len(source), len(dest))
  lastMatch = 0
  for idx in xrange(shortest):
    if source[idx] == dest[idx]: lastMatch = idx
    else: break
  path = dest[-1:lastMatch:-1] + source[lastMatch:]
  assert path[0] == dest[-1]
  assert path[-1] == source[-1]
  return path

def treeDist(source, dest):
  shortest = min(len(source), len(dest))
  dist = len(source) + len(dest)
  for idx in xrange(shortest):
    if source[idx] == dest[idx]: dist -= 2
    else: break
  #assert dist == len(treePath(source, dest))-1 # DEBUG, very slow
  return dist

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
  node1.links[node2.info.nodeID] = node2
  node2.links[node1.info.nodeID] = node1

def randomly(seq):
    shuffled = list(seq)
    random.shuffle(shuffled)
    return shuffled

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

def makeStoreASRelGraph(pathToGraph):
  #Existing network graphs, in caida.org's asrel format (ASx|ASy|z per line, z denotes relationship type)
  with open(pathToGraph, "r") as f:
    inData = f.readlines()
  store = dict()
  for line in inData:
    if line.strip(" ")[0] == "#": continue # Skip comment lines
    nodes = map(int, line.rstrip('\n').split('|')[0:2])
    if nodes[0] not in store: store[nodes[0]] = Node(nodes[0])
    if nodes[1] not in store: store[nodes[1]] = Node(nodes[1])
    linkNodes(store[nodes[0]], store[nodes[1]])
  print "CAIDA AS-relation graph successfully imported, size {}".format(len(store))
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

##########################
# Network Test Functions #
##########################

def idleUntilConverged(store):
  nodeIDs = sorted(store.keys())
  timeOfLastChange = 0
  step = 0
  # Idle until the network has converged
  while step - timeOfLastChange < 4*TIMEOUT:
    step += 1
    print "Step: {}, last change: {}".format(step, timeOfLastChange)
    changed = False
    for nodeID in nodeIDs:
      # Update node status, send messages
      changed |= store[nodeID].tick()
    for nodeID in nodeIDs:
      # Process messages
      changed |= store[nodeID].handleMessages()
    if changed: timeOfLastChange = step
  return store
  
def getClusterSizes(store):
  avgClus = 0.0
  maxClus = 0.0
  for node in store.itervalues():
    clus = len(node.clus)
    avgClus += clus
    maxClus = max(maxClus, clus)
  avgClus /= len(store)
  print "Avg / Max cluster sizes: {} / {}".format(avgClus, maxClus)
  return avgClus, maxClus

def testPaths(store):
  nodeIDs = sorted(store.keys())
  maxStretch = 0.0
  avgStretch = 0.0
  checked = 0
  nodesChecked = 0
  for sourceID in nodeIDs:
    nodesChecked += 1
    clus = len(store[sourceID].clus)
    print "Testing paths from node {} / {} ({}) ({})".format(nodesChecked, len(nodeIDs), sourceID, clus)
    expected = dijkstra(store, sourceID)
    for destID in nodeIDs:
      if destID <= sourceID: continue # No point in testing path to self, or retesting an already tested path
      if destID not in expected: continue # The network is split, no path exists
      eHops = expected[destID]
      maxAllowedLength = eHops*MAX_MS + MAX_AS
      hops = 0
      for pair in (sourceID, destID), (destID, sourceID):
        sourceInfo = store[pair[0]].info
        destInfo = store[pair[1]].info
        loc = sourceInfo.nodeID
        path = [loc]
        while loc != destInfo.nodeID:
          next = store[loc].lookup(destInfo)
          if next in path:
            print "ROUTING LOOP!"
            print sourceInfo.nodeID, sourceInfo.coords, destInfo.nodeID, destInfo.coords, loc, next, path
            #return store # Debug it
            #assert False
            break
          loc = next
          path.append(loc)
        nHops = len(path)-1
        #if nHops > maxAllowedLength:
          # Disregard, new scheme requires handshake for stretch bound to work...why?
          #print "EXCEEDED MAX ALLOWED PATH LENGTH:", sourceInfo.nodeID, destInfo.nodeID, nHops, maxAllowedLength, path
          #return store # Debug it
          #assert False
        if not hops or nHops < hops: hops = nHops
      stretch = float(hops)/max(1, eHops)
      avgStretch += 2*stretch
      maxStretch = max(maxStretch, stretch)
      checked += 2
  avgStretch /= max(1, checked)
  print "Avg / Max source-routed stretch: {} / {}".format(avgStretch, maxStretch)
  return avgStretch, maxStretch

def testWorstCasePaths(store):
  # This version checks the lengths of the path according to each node's store
  # This is the thing that must satisfy our stretch bound, not the path actually taken
  # (Which will often be much shorter)
  nodeIDs = sorted(store.keys())
  maxStretch = 0.0
  avgStretch = 0.0
  checked = 0
  nodesChecked = 0
  for sourceID in nodeIDs:
    nodesChecked += 1
    clus = len(store[sourceID].clus)
    print "Testing paths from node {} / {} ({}) ({})".format(nodesChecked, len(nodeIDs), sourceID, clus)
    expected = dijkstra(store, sourceID)
    for destID in nodeIDs:
      if destID <= sourceID: continue # No point in testing path to self, or retesting an already tested path
      if destID not in expected: continue # The network is split, no path exists
      eHops = expected[destID]
      maxAllowedLength = eHops*MAX_MS + MAX_AS
      hops = 0
      for pair in (sourceID, destID), (destID, sourceID):
        sourceInfo = store[pair[0]].info
        destInfo = store[pair[1]].info
        nHops = store[sourceInfo.nodeID].getWorstCasePathLength(destInfo)
        if nHops > maxAllowedLength:
          # Disregard, new scheme requires handshake for stretch bound to work...why?
          print "EXCEEDED MAX ALLOWED PATH LENGTH:", sourceInfo.nodeID, destInfo.nodeID, nHops, maxAllowedLength
          #return store # Debug it
          assert False
        if not hops or nHops < hops: hops = nHops
      stretch = float(hops)/max(1, eHops)
      avgStretch += 2*stretch
      maxStretch = max(maxStretch, stretch)
      checked += 2
  avgStretch /= max(1, checked)
  print "Avg / Max source-routed stretch: {} / {}".format(avgStretch, maxStretch)
  return avgStretch, maxStretch

##################
# Main execution #
##################

def main(store):
  for node in store.values():
    node.time = random.randint(0, TIMEOUT)
  print "Begin testing network"
  idleUntilConverged(store)
  # First print some table size info, since testing paths takes forever
  avgClus, maxClus = getClusterSizes(store)
  # Now test the paths, takes forever, unless you use the worst-case paths only
  avgStretch, maxStretch = testWorstCasePaths(store) # TODO finish testing w/ this
  #avgStretch, maxStretch = testPaths(store)
  print "Finished testing network"
  print "Avg / Max source-routed stretch: {} / {}".format(avgStretch, maxStretch)
  print "Avg / Max cluster sizes: {} / {}".format(avgClus, maxClus)
  return avgStretch, maxStretch, avgClus, maxClus

def processASRelFiles():
  # Like main, but saves final results to disk
  import glob
  import os
  paths = sorted(glob.glob("asrel/datasets/*"))
  outDir = "output-treesim"
  if not os.path.exists(outDir): os.makedirs(outDir)
  assert os.path.exists(outDir)
  exists = sorted(glob.glob(outDir+"/*"))
  for path in paths:
    date = os.path.basename(path).split(".")[0]
    outpath = outDir+"/{}".format(date)
    if outpath in exists:
      print "Skipping {}, already processed".format(date)
      continue
    store = makeStoreASRelGraph(path)
    print "Beginning {}, size {}".format(date, len(store))
    # Main takes too long, just idle and get cluster sizes
    idleUntilConverged(store)
    avgClus, maxClus = getClusterSizes(store)
    avgStretch, maxStretch = 0, 0 # In case we skip testing paths
    avgStretch, maxStretch = testWorstCasePaths(store) # Fast-ish worst-case test
    #avgStretch, maxStretch = testPaths(store) # Comment out to skip, slow
    print "Finished testing network"
    print "Avg / Max source-routed stretch: {} / {}".format(avgStretch, maxStretch)
    print "Avg / Max cluster sizes: {} / {}".format(avgClus, maxClus)
    with open(outpath, "w") as f:
      results = "{}, {}, {}, {}, {}".format(len(store), avgClus, maxClus, avgStretch, maxStretch)
      f.write(results)
    print "Finished {} with {} nodes".format(date, len(store))
    break # Stop after 1, because they can take forever

if __name__ == "__main__":
  random.seed(12345) # DEBUG
  store = dict()
  #store = makeStoreSquareGrid(8)
  #store = makeStoreHypeGraph("graph.json") # See: http://www.fc00.org/static/graph.json
  #store = makeStoreCaidaGraph("skitter") # Internet AS graph, from skitter
  #store = makeStoreASRelGraph("asrel/datasets/19980101.as-rel.txt")
  for node in store.values():
    node.time = random.randint(0, TIMEOUT)
  if store: main(store)
  else: processASRelFiles()

