# Tree routing scheme (named Yggdrasil, after the world tree from Norse mythology)
# Steps:
#   1: Pick any node, here I'm using highest nodeID
#   2: Build spanning tree, each node stores path back to root
#     Optionally with weights for each hop
#     Ties broken by preferring a parent with higher degree
#   3: Distance metric: self->peer + (via tree) peer->dest
#   4: Perform (modified) greedy lookup via this metric for each direction (A->B and B->A)
#   5: Source-route traffic using the better of those two paths

# Note: This makes to attempt to simulate a dynamic network
#   E.g. A node's peers cannot be disconnected

# TODO:
#   Make better use of drop?
#   In particular, we should be ignoring *all* recently dropped *paths* to the root
#     To minimize route flapping
#     Not really an issue in the sim, but probably needed for a real network

import ctypes
import glob
import heapq
import multiprocessing as MP
import os
import random

#############
# Constants #
#############

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
    self.degree = 0        # Number of peers the sender has, used to break ties
    # The above should be signed
    self.path   = [nodeID] # Path to node (in path-vector route)
    self.time   = 0        # Time info was updated, to keep track of e.g. timeouts
  def clone(self):
    # Return a deep-enough copy of the path
    clone = PathInfo(None)
    clone.nodeID = self.nodeID
    clone.coords = self.coords[:]
    clone.tstamp = self.tstamp
    clone.degree = self.degree
    clone.path = self.path[:]
    clone.time = self.time
    return clone
# End class PathInfo

class Node:
  def __init__(self, nodeID):
    self.info  = PathInfo(nodeID) # Self NodeInfo
    self.root  = None             # PathInfo to node at root of tree
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
      self.info.degree = len(self.peers)
    changed = False # Used to track when the network has converged
    changed |= self.cleanRoot()
    self.cleanDropped()
    # Should probably send messages infrequently if there's nothing new to report
    if self.info.tstamp == self.info.time:
      msg = self.createMessage()
      self.sendMessage(msg)
    return changed

  def cleanRoot(self):
    changed = False
    if self.root and self.info.time - self.root.time > TIMEOUT:
      print "DEBUG: clean root,", self.root.path
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

  def cleanDropped(self):
    nodes = self.drop.values()
    for node in nodes:
      if self.info.time - node.time > 4*TIMEOUT:
        del self.drop[node.nodeID]
    return None

  def createMessage(self):
    # Message is just a tuple
    # First element in the list is always the sender
    # Second element is the root
    # We will .clone() everything during the send operation
    msg = (self.info, self.root)
    return msg

  def sendMessage(self, msg):
    for link in self.links.values():
      newMsg = (msg[0].clone(), msg[1].clone())
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
      # Update the path and timestamp for the sender and root info
      node.path.append(self.info.nodeID)
      node.time = self.info.time
    # Update the sender's info in our list of peers
    sender = msg[0]
    self.peers[sender.nodeID] = sender
    # Decide if we want to update the root
    root = msg[1]
    updateRoot = False
    isSameParent = False
    isBetterParent = False
    if len(self.root.path) > 1 and len(root.path) > 1:
      parent = self.peers[self.root.path[-2]]
      if parent.nodeID == sender.nodeID: isSameParent = True
      if sender.degree > parent.degree:
        # This would also be where you check path uptime/reliability/whatever
        # All else being equal, we prefer parents with high degree
        # We are trusting peers to report degree correctly in this case
        # So expect some performance reduction if your peers aren't trustworthy
        # (Lies can increase average stretch by a few %)
        isBetterParent = True
      if sender.degree == parent.degree and len(root.path) < len(self.root.path):
        isBetterParent = True
    if self.info.nodeID in root.path[:-1]: pass # No loopy routes allowed
    elif root.nodeID in self.drop and self.drop[root.nodeID].tstamp >= root.tstamp: pass
    elif not self.root: updateRoot = True
    elif self.root.nodeID < root.nodeID: updateRoot = True
    elif self.root.nodeID != root.nodeID: pass
    elif self.root.tstamp > root.tstamp: pass
    elif len(root.path) < len(self.root.path): updateRoot = True
    elif isBetterParent and len(root.path) == len(self.root.path): updateRoot = True
    elif isSameParent and self.root.tstamp < root.tstamp: updateRoot = True
    if updateRoot:
      if not self.root or self.root.path != root.path: changed = True
      self.root = root
      self.info.coords = self.root.path
    return changed

  def lookup(self, dest):
    # Note: Can loop in an unconverged network
    # The person looking up the route is responsible for checking for loops
    best = None
    bestDist = 0
    for node in self.peers.itervalues():
      # dist = distance to node + dist (on tree) from node to dest
      dist = len(node.path)-1 + treeDist(node.coords, dest.coords)
      if not best or dist < bestDist:
        best = node
        bestDist = dist
    if best:
      next = best.path[-2]
      assert next in self.peers
      return next
    else:
      # We failed to look something up
      # TODO some way to signal this which doesn't crash
      assert False
# End class Node

####################
# Helper Functions #
####################

def getIndexOfLCA(source, dest):
  # Return index of last common ancestor in source/dest coords
  # -1 if no common ancestor (e.g. different roots)
  lcaIdx = -1
  minLen = min(len(source), len(dest))
  for idx in xrange(minLen):
    if source[idx] == dest[idx]: lcaIdx = idx
    else: break
  return lcaIdx

def treePath(source, dest):
  # Return path with source at head and dest at tail
  lastMatch = getIndexOfLCA(source, dest)
  path = dest[-1:lastMatch:-1] + source[lastMatch:]
  assert path[0] == dest[-1]
  assert path[-1] == source[-1]
  return path

def treeDist(source, dest):
  dist = len(source) + len(dest)
  lcaIdx = getIndexOfLCA(source, dest)
  dist -= 2*(lcaIdx+1)
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

############################
# Store topology functions #
############################

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

def makeStoreASRelGraphMaxDeg(pathToGraph, degIdx=0):
  with open(pathToGraph, "r") as f:
    inData = f.readlines()
  store = dict()
  nodeDeg = dict()
  for line in inData:
    if line.strip(" ")[0] == "#": continue # Skip comment lines
    nodes = map(int, line.rstrip('\n').split('|')[0:2])
    if nodes[0] not in nodeDeg: nodeDeg[nodes[0]] = 0
    if nodes[1] not in nodeDeg: nodeDeg[nodes[1]] = 0
    nodeDeg[nodes[0]] += 1
    nodeDeg[nodes[1]] += 1
  sortedNodes = sorted(nodeDeg.keys(), \
                       key=lambda x: (nodeDeg[x], x), \
                       reverse=True)
  maxDegNodeID = sortedNodes[degIdx]
  return makeStoreASRelGraphFixedRoot(pathToGraph, maxDegNodeID)

def makeStoreASRelGraphFixedRoot(pathToGraph, rootNodeID):
  with open(pathToGraph, "r") as f:
    inData = f.readlines()
  store = dict()
  for line in inData:
    if line.strip(" ")[0] == "#": continue # Skip comment lines
    nodes = map(int, line.rstrip('\n').split('|')[0:2])
    if nodes[0] == rootNodeID: nodes[0] += 1000000000
    if nodes[1] == rootNodeID: nodes[1] += 1000000000
    if nodes[0] not in store: store[nodes[0]] = Node(nodes[0])
    if nodes[1] not in store: store[nodes[1]] = Node(nodes[1])
    linkNodes(store[nodes[0]], store[nodes[1]])
  print "CAIDA AS-relation graph successfully imported, size {}".format(len(store))
  return store

def makeStoreDimesEdges(pathToGraph, rootNodeID=None):
  # Read from a DIMES csv-formatted graph
  index = 0
  store = dict()
  size = os.path.getsize(pathToGraph)
  with open(pathToGraph, "r") as f:
    index = 0
    for edge in f:
      if not index % 1000:
        pos = f.tell()
        pct = 100.0*pos/size
        print "Processing edge {}, {:.2f}%".format(index, pct)
      index += 1
      dat = edge.rstrip('\n').split(',')
      node1 = "N" + str(dat[0])
      node2 = "N" + str(dat[1])
      if '?' in node1 or '?' in node2: continue
      if node1 == rootNodeID: node1 = "R" + str(dat[0])
      if node2 == rootNodeID: node2 = "R" + str(dat[1])
      if node1 not in store: store[node1] = Node(node1)
      if node2 not in store: store[node2] = Node(node2)
      if node1 != node2: linkNodes(store[node1], store[node2])
  print "DIMES graph successfully imported, size {}".format(len(store))
  return store

############################################
# Functions used as parts of network tests #
############################################

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
  
def getCacheIndex(nodes, sourceIndex, destIndex):
  return sourceIndex*nodes + destIndex

def compactPathCacheWorker(store, cache, processNum, processTotal):
  nodeIDs = sorted(store.keys())
  nNodes = len(nodeIDs)
  nodeIdxs = dict()
  for nodeIdx in xrange(nNodes):
    nodeIdxs[nodeIDs[nodeIdx]] = nodeIdx
  for sourceIdx in xrange(nNodes):
    if sourceIdx % processTotal != processNum: continue # Not our work to do
    sourceID = nodeIDs[sourceIdx]
    print "Building fast lookup table for node {} / {} ({})".format(sourceIdx+1, nNodes, sourceID)
    for destIdx in xrange(nNodes):
      destID = nodeIDs[destIdx]
      if sourceID == destID: nextHop = destID # lookup would fail
      else: nextHop = store[sourceID].lookup(store[destID].info)
      nextHopIdx = nodeIdxs[nextHop]
      cache[getCacheIndex(nNodes, sourceIdx, destIdx)] = nextHopIdx
  return

def getCompactPathCache(store, processTotal=1):
  nodeIDs = sorted(store.keys())
  nNodes = len(nodeIDs)
  nodeIdxs = dict()
  for nodeIdx in xrange(nNodes):
    nodeIdxs[nodeIDs[nodeIdx]] = nodeIdx
  cache = MP.RawArray(ctypes.c_ushort, nNodes*nNodes)
  children = []
  for processNum in xrange(processTotal):
    print "Creating worker {} / {}".format(processNum+1, processTotal)
    q = MP.Queue()
    p = MP.Process(target=compactPathCacheWorker, args=(store, cache, processNum, processTotal))
    children.append(p)
  for child in children: child.start() # Launch child processes
  for child in children: child.join()  # Wait for children to finish
  return cache

def pathTestWorker(store, cache, output, processNum, processTotal):
  nodeIDs = sorted(store.keys())
  nNodes = len(nodeIDs)
  idxs = dict()
  for nodeIdx in xrange(nNodes):
    nodeID = nodeIDs[nodeIdx]
    idxs[nodeID] = nodeIdx
  results = dict()
  for sourceIdx in xrange(nNodes):
    if sourceIdx % processTotal != processNum: continue # Not our work to do
    sourceID = nodeIDs[sourceIdx]
    print "Testing paths from node {} / {} ({})".format(sourceIdx+1, len(nodeIDs), sourceID)
    expected = dijkstra(store, sourceID)
    for destIdx in xrange(sourceIdx+1, nNodes):
      destID = nodeIDs[destIdx]
      if destID <= sourceID: continue # Skip anything we've already checked
      if destID not in expected: continue # The network is split, no path exists
      eHops = expected[destID]
      hops = 0
      for pair in (sourceIdx, destIdx), (destIdx, sourceIdx):
        sourceInfo = store[nodeIDs[pair[0]]].info
        destInfo = store[nodeIDs[pair[1]]].info
        locIdx = idxs[sourceInfo.nodeID] # Get the index of our current position
        nHops = 0
        dIdx = idxs[destInfo.nodeID] # Get the index of the destination
        while locIdx != dIdx:
          locIdx = cache[getCacheIndex(nNodes, locIdx, dIdx)]
          nHops += 1
        if not hops or nHops < hops: hops = nHops
      if eHops not in results: results[eHops] = dict()
      if hops not in results[eHops]: results[eHops][hops] = 0
      results[eHops][hops] += 2 # Once for source->dest, once for dest->source
  output.put(results)

def testPaths(store, processTotal=1):
  #cache = getCompactPathCache(store, processTotal)
  # Temporarily running with just 1 worker
  # This part is relatively quick, and 1 worker gives the CPU a chance to cool a bit
  # (Most relevant when running many tests over many networks in series)
  cache = getCompactPathCache(store)
  children = []
  pathMatrices = []
  for processNum in xrange(processTotal):
    print "Creating worker {} / {}".format(processNum+1, processTotal)
    q = MP.Queue()
    p = MP.Process(target=pathTestWorker, args=(store, cache, q, processNum, processTotal))
    children.append(p)
    pathMatrices.append(q)
  for child in children: child.start()
  for child in children: child.join()
  print "Workers finished."
  pathMatrix = dict()
  for q in pathMatrices:
    pm = q.get()
    for eHops in pm:
      if eHops not in pathMatrix: pathMatrix[eHops] = dict()
      for nHops in pm[eHops]:
        if nHops not in pathMatrix[eHops]: pathMatrix[eHops][nHops] = 0
        pathMatrix[eHops][nHops] += pm[eHops][nHops]
  return pathMatrix

def getAvgStretch(pathMatrix):
  avgStretch = 0.
  checked = 0.
  for eHops in sorted(pathMatrix.keys()):
    for nHops in sorted(pathMatrix[eHops].keys()):
      count = pathMatrix[eHops][nHops]
      stretch = float(nHops)/float(max(1, eHops))
      avgStretch += stretch*count
      checked += count
  avgStretch /= max(1, checked)
  return avgStretch

def getMaxStretch(pathMatrix):
  maxStretch = 0.
  for eHops in sorted(pathMatrix.keys()):
    for nHops in sorted(pathMatrix[eHops].keys()):
      stretch = float(nHops)/float(max(1, eHops))
      maxStretch = max(maxStretch, stretch)
  return maxStretch

def getResults(pathMatrix):
  results = []
  for eHops in sorted(pathMatrix.keys()):
    for nHops in sorted(pathMatrix[eHops].keys()):
      count = pathMatrix[eHops][nHops]
      results.append("{} {} {}".format(eHops, nHops, count))
  return '\n'.join(results)

####################################
# Functions to run different tests #
####################################

def runTest(store, processTotal=1):
  # Runs the usual set of tests on the store
  # Does not save results, so only meant for quick tests
  # To e.g. check the code works, maybe warm up the pypy jit
  for node in store.values():
    node.info.time = random.randint(0, TIMEOUT)
    node.info.tstamp = TIMEOUT
  print "Begin testing network"
  idleUntilConverged(store)
  pathMatrix = testPaths(store, processTotal)
  avgStretch = getAvgStretch(pathMatrix)
  maxStretch = getMaxStretch(pathMatrix)
  print "Finished testing network"
  print "Avg / Max stretch: {} / {}".format(avgStretch, maxStretch)
  return # End of function

def rootNodeASTest(path, processTotal=1):
  # Checks performance for every possible choice of root node
  # Saves output for each root node to a separate file on disk
  # path = input path to some caida.org formatted AS-relationship graph
  # processTotal = number of processes to run in parallel
  outDir = "output-treesim-AS"
  if not os.path.exists(outDir): os.makedirs(outDir)
  assert os.path.exists(outDir)
  exists = sorted(glob.glob(outDir+"/*"))
  store = makeStoreASRelGraph(path)
  nodes = sorted(store.keys())
  for nodeIdx in xrange(len(nodes)):
    rootNodeID = nodes[nodeIdx]
    outpath = outDir+"/{}".format(rootNodeID)
    if outpath in exists:
      print "Skipping {}, already processed".format(rootNodeID)
      continue
    store = makeStoreASRelGraphFixedRoot(path, rootNodeID)
    for node in store.values():
      node.info.time = random.randint(0, TIMEOUT)
      node.info.tstamp = TIMEOUT
    print "Beginning {}, size {}".format(nodeIdx, len(store))
    idleUntilConverged(store)
    pathMatrix = testPaths(store, processTotal)
    avgStretch = getAvgStretch(pathMatrix)
    maxStretch = getMaxStretch(pathMatrix)
    results = getResults(pathMatrix)
    with open(outpath, "w") as f:
      f.write(results)
    print "Finished test for root AS {} ({} / {})".format(rootNodeID, nodeIdx+1, len(store))
    print "Avg / Max stretch: {} / {}".format(avgStretch, maxStretch)
    #break # Stop after 1, because they can take forever
  return # End of function

def timelineASTest(processTotal=1):
  # Meant to study the performance of the network as a function of network size
  # Loops over a set of AS-relationship graphs
  # Runs a test on each graph, selecting highest-degree node as the root
  # Saves results for each graph to a separate file on disk
  outDir = "output-treesim-timeline-AS"
  if not os.path.exists(outDir): os.makedirs(outDir)
  assert os.path.exists(outDir)
  paths = sorted(glob.glob("asrel/datasets/*"))
  exists = sorted(glob.glob(outDir+"/*"))
  for path in paths:
    date = os.path.basename(path).split(".")[0]
    outpath = outDir+"/{}".format(date)
    if outpath in exists:
      print "Skipping {}, already processed".format(date)
      continue
    store = makeStoreASRelGraphMaxDeg(path)
    for node in store.values():
      node.info.time = random.randint(0, TIMEOUT)
      node.info.tstamp = TIMEOUT
    print "Beginning {}, size {}".format(date, len(store))
    idleUntilConverged(store)
    pathMatrix = testPaths(store, processTotal)
    avgStretch = getAvgStretch(pathMatrix)
    maxStretch = getMaxStretch(pathMatrix)
    results = getResults(pathMatrix)
    with open(outpath, "w") as f:
      f.write(results)
    print "Finished {} with {} nodes".format(date, len(store))
    print "Avg / Max stretch: {} / {}".format(avgStretch, maxStretch)
    #break # Stop after 1, because they can take forever
  return # End of function

##################
# Main Execution #
##################

if __name__ == "__main__":
  cpus = MP.cpu_count()
  if True: # Run a quick test
    random.seed(12345) # DEBUG
    store = makeStoreSquareGrid(4)
    runTest(store, 1) # Quick test, 1 worker process
  # Do some real work
  #runTest(makeStoreDimesEdges("../../datasets/DIMES/ASEdges1_2007.csv"), cpus)
  #rootNodeASTest("asrel/datasets/19980101.as-rel.txt", cpus)
  #timelineASTest(cpus)
  rootNodeASTest("hype-rel.links", cpus)

