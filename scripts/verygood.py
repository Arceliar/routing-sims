#!/usr/bin/env python2.7

# This is meant as a hybrid between VRR and Ygg (basically have VRR setup use
# ygg for route finding).

# VRR + Ygg sounds like "very g", which is the start of "very good", so if this
# is the "good" version of the routing scheme then the name makes sense (compared
# to the older Chord+Ygg combo that we use today, which suffers from long lookup
# times and issues with mobility).

# This does not need to simulate the algorithm itself, it just needs to end up
# in the final state that the algorithm would be expected to reach. That means
# no request/response packets, tree announcements, etc., are requried, we can
# cheat by using the full view of the network to figure out what each node would
# have eventually decided to do.

###########
# Imports #
###########

import array
import gc
import heapq
import random

#############
# Constants #
#############

LINK_COST = 1

###########
# Classes #
###########

class vsetInfo():
  def __init__(self, sourceID, destID, prevID, nextID):
    self.sourceID = sourceID
    self.destID = destID
    self.prevID = prevID # Peer back to sourceID
    self.nextID = nextID # Peer forward to destID

class Node:
  def __init__(self, nodeID):
    self.NodeID = nodeID
    self.Parent = None
    self.Coords = None
    self.VSet = []  # VRR routing table
    # The rest is the low-level network links etc., not real code
    self.links = dict() # Map of nodeID onto neighboring nodes

  def treeLookup(self, dest): # dest is destination coords
    best = self.Coords
    bestDist = treeDist(best, dest)
    for peer in self.links:
      cs = self.links[peer].Coords
      if cs[0] != self.Coords[0]: continue
      dist = treeDist(cs, dest)
      if dist < bestDist:
        best = cs
        bestDist = dist
    return best[-1]

  def chordLookup(self, dest): # dest is destination nodeID
    best = self.NodeID
    bestNext = self.NodeID
    for info in self.VSet:
      if info.sourceID == dest or isChordOrdered(dest, info.sourceID, best):
        best = info.sourceID
        bestNext = info.prevID
    for info in self.VSet:
      break # Only check source direction for now
      if info.destID == dest or isChordOrdered(dest, info.destID, best):
        best = info.destID
        bestNext = info.nextID
    for peerID in self.links:
      # Peers are checked last, so the peerID == dest case can override longer paths...
      if peerID in [dest, best] or isChordOrdered(dest, peerID, best):
        best = peerID
        bestNext = peerID
    return bestNext
    
#####################
# Utility Functions #
#####################

def buildTree(nodestore, rootNodeID):
  nodeIDs = sorted(nodestore.keys())
  nNodes = len(nodeIDs)
  visited = set()
  queue = [(0, rootNodeID, None)]
  while queue:
    dist, nodeID, parent = heapq.heappop(queue)
    if nodeID in visited: continue
    visited.add(nodeID)
    node = nodestore[nodeID]
    node.Parent = parent
    for peer in node.links:
      if peer in visited: continue
      heapq.heappush(queue, (dist+LINK_COST, peer, nodeID))
  # Now build the coords per node
  for nodeID in nodestore:
    node = nodestore[nodeID]
    rcoords = []
    here = node
    while here:
      rcoords.append(here.NodeID)
      here = nodestore[here.Parent] if here.Parent != None else None
    node.Coords = rcoords
    node.Coords.reverse()
  return

def getIndexOfLCA(source, dest):
  # Return index of last common ancestor in source/dest coords
  # -1 if no common ancestor (e.g. different roots)
  lcaIdx = -1
  minLen = min(len(source), len(dest))
  for idx in xrange(minLen):
    if source[idx] == dest[idx]: lcaIdx = idx
    else: break
  return lcaIdx

def treeDist(source, dest):
  dist = len(source) + len(dest)
  lcaIdx = getIndexOfLCA(source, dest)
  dist -= 2*(lcaIdx+1)
  return dist

def isChordOrdered(first, second, third):
  if first < second < third: return True
  if second < third < first: return True
  if third < first < second: return True
  return False

def buildVSets(nodestore):
  nodeIDs = sorted(nodestore.keys())
  nNodes = len(nodeIDs)
  for sourceIdx in xrange(nNodes):
    sourceID = nodeIDs[sourceIdx]
    source = nodestore[sourceID]
    print "Builidng vset paths for node {} / {} ({})".format(sourceIdx+1, nNodes, sourceID)
    for offset in [1]: #[-2, -1, 1, 2]: # Only maintain one path for now, bare minimum for things to work
      destIdx = (sourceIdx + offset) % nNodes
      destID = nodeIDs[destIdx]
      dest = nodestore[destID]
      prev = sourceID
      here = nodestore[sourceID]
      while True:
        next = here.treeLookup(dest.Coords)
        info = vsetInfo(sourceID, destID, prev, next)
        here.VSet.append(info)
        prev = here.NodeID
        if next == here.NodeID: break
        here = nodestore[next]
  return

def dijkstrall(nodestore):
  # Idea to use heapq and basic implementation taken from stackexchange post
  # http://codereview.stackexchange.com/questions/79025/dijkstras-algorithm-in-python
  nodeIDs = sorted(nodestore.keys())
  nNodes = len(nodeIDs)
  idxs = dict()
  for nodeIdx in xrange(nNodes):
    nodeID = nodeIDs[nodeIdx]
    idxs[nodeID] = nodeIdx
  dists = array.array("H", [0]*nNodes*nNodes) # use GetCacheIndex(nNodes, start, end)
  for sourceIdx in xrange(nNodes):
    print "Finding shortest paths for node {} / {} ({})".format(sourceIdx+1, nNodes, nodeIDs[sourceIdx])
    queue = [(0, sourceIdx)]
    while queue:
      dist, nodeIdx = heapq.heappop(queue)
      distIdx = getCacheIndex(nNodes, sourceIdx, nodeIdx)
      if not dists[distIdx]: # Unvisited, otherwise we skip it
        dists[distIdx] = dist
        for peer in nodestore[nodeIDs[nodeIdx]].links:
          pIdx = idxs[peer]
          pdIdx = getCacheIndex(nNodes, sourceIdx, pIdx)
          if not dists[pdIdx]:
            # Peer is also unvisited, so add to queue
            heapq.heappush(queue, (dist+LINK_COST, pIdx))
  return dists

def getPathLengths(nodestore, name, cache):
  nodeIDs = sorted(nodestore.keys())
  nNodes = len(nodeIDs)
  dists = array.array("H", [0]*nNodes*nNodes) # use GetCacheIndex(nNodes, start, end)
  idxs = dict()
  for nodeIdx in xrange(nNodes):
    nodeID = nodeIDs[nodeIdx]
    idxs[nodeID] = nodeIdx
  for sourceIdx in xrange(nNodes):
    sourceID = nodeIDs[sourceIdx]
    source = nodestore[sourceID]
    print "Testing {} paths from node {} / {} ({})".format(name, sourceIdx+1, nNodes, sourceID)
    for destIdx in xrange(nNodes):
      destID = nodeIDs[destIdx]
      dest = nodestore[destID]
      hereIdx = sourceIdx
      dist = 0
      update = True
      while hereIdx != destIdx:
        nextIdx = cache[getCacheIndex(nNodes, hereIdx, destIdx)]
        if nextIdx == hereIdx:
          update = False # Dead end, possibly because the destination is outside our connected component
          break
        hereIdx = nextIdx
        dist += 1
      if update: dists[getCacheIndex(nNodes, sourceIdx, destIdx)] = dist
  return dists

def getCacheIndex(nodes, sourceIndex, destIndex):
  return sourceIndex*nodes + destIndex

def getTreeCache(store):
  nodeIDs = sorted(store.keys())
  nNodes = len(nodeIDs)
  nodeIdxs = dict()
  for nodeIdx in xrange(nNodes):
    nodeIdxs[nodeIDs[nodeIdx]] = nodeIdx
  cache = array.array("H", [0]*nNodes*nNodes)
  for sourceIdx in xrange(nNodes):
    sourceID = nodeIDs[sourceIdx]
    print "Building fast tree lookup table for node {} / {} ({})".format(sourceIdx+1, nNodes, sourceID)
    for destIdx in xrange(nNodes):
      destID = nodeIDs[destIdx]
      if sourceID == destID: nextHop = destID # lookup would fail
      else: nextHop = store[sourceID].treeLookup(store[destID].Coords)
      nextHopIdx = nodeIdxs[nextHop]
      cache[getCacheIndex(nNodes, sourceIdx, destIdx)] = nextHopIdx
  return cache

def getChordCache(store):
  nodeIDs = sorted(store.keys())
  nNodes = len(nodeIDs)
  nodeIdxs = dict()
  for nodeIdx in xrange(nNodes):
    nodeIdxs[nodeIDs[nodeIdx]] = nodeIdx
  cache = array.array("H", [0]*nNodes*nNodes)
  for sourceIdx in xrange(nNodes):
    sourceID = nodeIDs[sourceIdx]
    print "Building fast chord lookup table for node {} / {} ({})".format(sourceIdx+1, nNodes, sourceID)
    for destIdx in xrange(nNodes):
      destID = nodeIDs[destIdx]
      if sourceID == destID: nextHop = destID # lookup would fail
      else: nextHop = store[sourceID].chordLookup(destID)
      nextHopIdx = nodeIdxs[nextHop]
      cache[getCacheIndex(nNodes, sourceIdx, destIdx)] = nextHopIdx
  return cache

###################
# Debugging tools #
###################

def dumpDists(nodestore, typename, dists):
  nodeIDs = sorted(nodestore.keys())
  nNodes = len(nodeIDs)
  nodeIdxs = dict()
  for nodeIdx in xrange(nNodes):
    nodeIdxs[nodeIDs[nodeIdx]] = nodeIdx
  for sourceID in nodeIDs:
    for destID in nodeIDs:
      print "SourceID {}, DestID {}, {} distance {}".format(sourceID, destID, typename, dists[getCacheIndex(nNodes, nodeIdxs[sourceID], nodeIdxs[destID])])

def dumpVSEndpoints(nodestore):
  nodeIDs = sorted(nodestore.keys())
  for nodeID in nodeIDs:
    node = nodestore[nodeID]
    dests = set()
    for info in node.VSet:
      dests.add(info.sourceID)
      dests.add(info.destID)
    dests = sorted(dests)
    print "Node: {}, Endpoints: {}".format(nodeID, dests)

########################
# Nework Architectures #
########################

def linkNodes(node1, node2): # Helper used in the below
  node1.links[node2.NodeID] = node2
  node2.links[node1.NodeID] = node1

def makeStoreSquareGrid(sideLength, randomize=True):
  # Simple grid in a sideLength*sideLength square
  # Just used to validate that the code runs
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

def makeStoreGeneratedGraph(pathToGraph, root=None):
  with open(pathToGraph, "r") as f:
    inData = f.readlines()
  store = dict()
  for line in inData:
    if line.strip()[0] == "#": continue # Skip comment lines
    nodes = map(int, line.strip().split(' ')[0:2])
    node1 = nodes[0]
    node2 = nodes[1]
    if node1 == root: node1 += 1000000
    if node2 == root: node2 += 1000000
    if node1 not in store: store[node1] = Node(node1)
    if node2 not in store: store[node2] = Node(node2)
    linkNodes(store[node1], store[node2])
  print "Generated graph successfully imported, size {}".format(len(store))
  return store

def connectComponents(nodestore):
  nodeIDs = sorted(nodestore.keys())
  nNodes = len(nodeIDs)
  visited = set()
  roots = set()
  queue = []
  def reset():
    for nodeID in nodeIDs:
      if nodeID in visited: continue
      roots.add(nodeID)
      heapq.heappush(queue, (0, nodeID))
      return
  reset()
  while queue:
    dist, nodeID = heapq.heappop(queue)
    if nodeID in visited:
      if len(queue) == 0: reset()
      continue
    visited.add(nodeID)
    node = nodestore[nodeID]
    for peer in node.links:
      if peer in visited: continue
      heapq.heappush(queue, (dist+LINK_COST, peer))
    if len(queue) == 0: reset()
  # Now link the roots
  roots = sorted(roots)
  for idx1 in xrange(len(roots)):
    nodeID1 = roots[idx1]
    for nodeID2 in roots[idx1+1:]:
      linkNodes(nodestore[nodeID1], nodestore[nodeID2])
  return

#########
# Tests #
#########

def testPathStretch(nodestore, pathType, pathDists, dijkstraDists):
  nodeIDs = sorted(nodestore.keys())
  nNodes = len(nodeIDs)
  nodeIdxs = dict()
  for nodeIdx in xrange(nNodes):
    nodeIdxs[nodeIDs[nodeIdx]] = nodeIdx
  sumTree = 0.
  sumDij = 0.
  sumStretch = 0.
  maxStretch = 0.
  nPaths = 0.
  for sourceID in nodeIDs:
    for destID in nodeIDs:
      if sourceID == destID: continue # Skip paths to self
      sourceIdx = nodeIdxs[sourceID]
      destIdx = nodeIdxs[destID]
      dijDist = dijkstraDists[getCacheIndex(nNodes, sourceIdx, destIdx)]
      pathDist = pathDists[getCacheIndex(nNodes, sourceIdx, destIdx)]
      if dijDist == 0: continue # Not in same connected component
      assert pathDist > 0
      stretch = float(pathDist)/dijDist if dijDist>0 else 1
      sumTree += pathDist
      sumDij += dijDist
      sumStretch += stretch
      maxStretch = max(maxStretch, stretch)
      nPaths += 1
  print "{} avg stretch: {}, bandwidth: {}".format(pathType, sumStretch/nPaths, sumTree/sumDij)
  print "{} max stretch: {}".format(pathType, maxStretch)

def testResourceUsage(nodestore):
  nodeIDs = sorted(nodestore.keys())
  nNodes = len(nodestore)
  minPeers = nNodes**2
  sumPeers = 0.
  maxPeers = 0.
  minVSets = nNodes**2
  sumVSets = 0.
  maxVSets = 0.
  minRatio = nNodes**2
  sumRatio = 0.
  maxRatio = 0.
  for nodeID in sorted(nodestore.keys()):
    # peers
    peers = len(nodestore[nodeID].links)
    minPeers = min(minPeers, peers)
    sumPeers += float(peers)
    maxPeers = max(maxPeers, peers)
    # vsets
    vsets = len(nodestore[nodeID].VSet)
    minVSets = min(minVSets, vsets)
    sumVSets += float(vsets)
    maxVSets = max(maxVSets, vsets)
    # ratio
    ratio = float(vsets)/float(peers)
    minRatio = min(minRatio, ratio)
    sumRatio += ratio
    maxRatio = max(maxRatio, ratio)
  print "Peer min/avg/max: {} / {} / {}".format(minPeers, sumPeers/nNodes, maxPeers)
  print "VSet min/avg/max: {} / {} / {}".format(minVSets, sumVSets/nNodes, maxVSets)
  print "Ratio min/avg/max: {} / {} / {}".format(minRatio, sumRatio/nNodes, maxRatio)

#############
# Execution #
#############

def main():
  # Setup
  random.seed(12345)
  # Build the network
  #store = makeStoreSquareGrid(4)
  #store = makeStoreGeneratedGraph("fc00-2017-08-12.txt")
  store = makeStoreGeneratedGraph("skitter")
  # Make sure the whole network is in one connected component, this simplifies testing
  connectComponents(store)
  # Build the spanning tree for Ygg, use it to build VRR vset paths
  buildTree(store, max(store.keys()))
  buildVSets(store)
  # Get the path lengths with dijkstra's alg, Ygg, and VRR
  dij = dijkstrall(store)
  ygg = getPathLengths(store, "Ygg", getTreeCache(store))
  vrr = getPathLengths(store, "VRR", getChordCache(store))
  # Debugging output
  #dumpVSEndpoints(store)
  #dumpDists(store, "Dijkstra", dij)
  #dumpDists(store, "Ygg", ygg)
  #dumpDists(store, "VRR", vrr)
  # Test results
  testPathStretch(store, "Ygg", ygg, dij)
  testPathStretch(store, "VRR", vrr, dij)
  if True: # Source routing: minimum of vrr path, and then minimize that from A->B and B->A
    for idx in xrange(len(vrr)):
      vrr[idx] = min(vrr[idx], ygg[idx])
    testPathStretch(store, "Min(ygg,vrr)", vrr, dij)
    nNodes = len(store)
    for sourceIdx in xrange(nNodes):
      for destIdx in xrange(nNodes):
        vrr[getCacheIndex(nNodes, sourceIdx, destIdx)] = min(vrr[getCacheIndex(nNodes, sourceIdx, destIdx)], vrr[getCacheIndex(nNodes, destIdx, sourceIdx)])
    testPathStretch(store, "Min(s->d,d->s)", vrr, dij)
  testResourceUsage(store)

if __name__ == '__main__':
  main()

