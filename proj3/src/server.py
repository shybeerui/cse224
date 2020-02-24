from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from hashlib import sha256
import argparse
import threading
import time
import random
import xmlrpc.client

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

# A simple ping, returns true
def ping():
    """A simple ping method"""
    print("Ping()")
    return True

# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    print("GetBlock(" + h + ")")

    blockData = bytes(4)
    return blockData

# Puts a block
def putblock(b):
    """Puts a block"""
    print("PutBlock()")

    return True

# Given a list of hashes, return the subset that are on this server
def hasblocks(hashlist):
    """Determines which blocks are on this server"""
    print("HasBlocks()")

    return hashlist

def isMajorUncrashed():
    uncrashNumber = 0
    for hostport in serverlist:
        client  = xmlrpc.client.ServerProxy('http://' + hostport)
        if client.surfstore.isCrashed() == False:
            uncrashNumber += 1
    if uncrashNumber + 1 > maxnum /2:
        return True
    else:
        return False

# Retrieves the server's FileInfoMap
def getfileinfomap():

    """Gets the fileinfo map"""
    if not isLeader():
        #raise error
        return False
    else:
        while not isMajorUncrashed():
        #if majority not crashed:
            continue
        print("GetFileInfoMap()")
        return fileinfomap

# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    if isleader == True:
        while not isMajorUncrashed():
            #raise Exception('blocked')
            #if majority not crashed:
            continue

        print("UpdateFile("+filename+")")

        if filename in fileinfomap:
            if fileinfomap[filename][0] == version - 1:
                fileinfomap[filename] = [version, hashlist]
            else:
                return False
        else:
            fileinfomap[filename] = [version, hashlist]
        global logindex
        logindex += 1
        for hostport in serverlist:
            client  = xmlrpc.client.ServerProxy('http://' + hostport)
            client.surfstore.appendEntries(servernum, term_local, logindex, fileinfomap)
        return True
    else:
        print ('not a leader')
        raise Exception('not a leader.')
        return False

# PROJECT 3 APIs below

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    global isleader
    print("IsLeader()")
    if isleader == True:
        return True
    else:
        return False

# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    global is_crashed
    print("Crash()")
    is_crashed = True
    global isleader
    isleader = False
    return True

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    print("Restore()")
    global is_crashed
    is_crashed = False
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    #print("IsCrashed()")
    global is_crashed
    return is_crashed

# Requests vote from this server to become the leader
def requestVote(serverid, term, leaderindex):
    """Requests vote to be the leader"""
    global term_local
    global timeout_ele
    global has_voted
    global begin_ele
    global servernum
    global isleader
    global logindex
    if is_crashed == True:
        return False
        #and flag_reelection == False
    if (term > term_local or (term == term_local and leaderindex > logindex)) and isleader == False:
        print(servernum, "vote for", serverid)
        has_voted.add(term)
        term_local = term
        begin_ele = time.time()
        return True
    else:
        return False
    # else:
    #     if term == term_local and flag_reelection == False and term not in has_voted:
    #         print(servernum, "vote for", serverid)
    #         has_voted.add(term)
    #         isleader = False
    #         begin_ele = time.time()
    #         return True
    #     else:
    #         return False

# Updates fileinfomap
def appendEntries(serverid, term, leaderindex = None, leaderfileinfomap=None):
    """Updates fileinfomap to match that of the leader"""
    global begin_ele
    global term_local
    global isleader
    global is_crashed
    global flag_reelection
    isleader = False

    if is_crashed == True:
        print('crashed')
        return False
    print("receiving heartbeat")
    term_local = term
    flag_reelection = False
    begin_ele = time.time()
    if leaderfileinfomap != None:
        global fileinfomap
        fileinfomap = leaderfileinfomap #update to leaderinformap
        global logindex
        logindex = leaderindex
    return True

def tester_getversion(filename):
    return fileinfomap[filename][0]

# Reads the config file and return host, port and store list of other servers
def readconfig(config, servernum):
    """Reads cofig file"""
    global serverlist
    fd = open(config, 'r')
    l = fd.readline()

    maxnum = int(l.strip().split(' ')[1])
    print(maxnum, servernum)

    if servernum >= maxnum or servernum < 0:
        raise Exception('Server number out of range.')

    d = fd.read()
    d = d.splitlines()

    for i in range(len(d)):
        hostport = d[i].strip().split(' ')[1]
        if i == servernum:
            host = hostport.split(':')[0]
            port = int(hostport.split(':')[1])

        else:
            serverlist.append(hostport)


    return maxnum, host, port

def ele_time():
    global begin_ele
    global election_timeout
    global isleader
    global is_crashed
    while(1):
        print("enter ele_time")
        print(election_timeout / 1000)
        begin_ele = time.time()
        while (is_crashed == True or isleader == True or (time.time() - begin_ele) < (election_timeout / 1000)):
            if isleader == True:
                begin_ele = time.time()
        print("ele_timeout!")
        election_timeout = random.randint(600, 800)
        begin_ele = time.time()
        #reelection()
        t = threading.Thread(target=reelection)
        t.start()

def appe(hostport, term, logindex, fileinfomap):
    try:
        client = xmlrpc.client.ServerProxy('http://' + hostport)
        client.surfstore.appendEntries(servernum, term, logindex, fileinfomap)
    except (ConnectionRefusedError, OSError):
        print("Server: ")

def hb_time():
    global begin_hb
    global hb_timeout
    global isleader
    global serverlist
    global term_local
    global is_crashed
    begin_hb = time.time()
    while(1):
        while(is_crashed == True or isleader == False or time.time() - begin_hb < hb_timeout / 1000):
            if isleader == False:
                begin_hb = time.time()
            if is_crashed == True:
                isleader = False
        print("sending heartbeat")
        threads = []
        for hostport in serverlist:
            t = threading.Thread(target=appe, args=(hostport, term_local,logindex, fileinfomap,))
            threads.append(t)
        for i in range(maxnum - 1):
            threads[i].start()
        begin_hb = time.time()



'''
def total_time():
    global begin_total
    global total_timeout
    global timeout_total
    global timeout_ele
    while(1):
        while(time.time() - begin_total < total_timeout):
            if timeout_ele == False:
                begin_total = time.time()
        timeout_total = True
        print("total_timeout!")
        begin_total = time.time()
'''
def reqvote(hostport,votelist):
    global term_local
    #global votes
    global servernum
    
    try:
        global logindex
        client  = xmlrpc.client.ServerProxy('http://' + hostport)
        if(client.surfstore.requestVote(servernum, term_local, logindex)):
            print("get one more vote")
            votelist.append(1)
    except (ConnectionRefusedError, OSError):
        print("Server: ")

# def vote_to_leader():
#     global votes
#     global servernum
#     global leader_id
#     global maxnum
#     global term_local
#     while votes <= maxnum / 2:
#         continue
#     global isleader
#     isleader = True
#     threads = []
#     for hostport in serverlist:
#         t = threading.Thread(target=appe, args=(hostport, term_local,))
#         threads.append(t)
#     for i in range(maxnum - 1):
#         threads[i].start()
#     leader_id = servernum
#     print(servernum, " is leader now")
#     global flag_reelection
#     flag_reelection = False
#     votes = 0

def reelection():
    global begin_ele
    global term_local
    global maxnum
    global timeout_ele
    global has_voted
    global isleader
    global timeout_ele
    
    # while timeout_ele == False:
    #     continue
    timeout_ele = False
    
    term_local += 1
    global flag_reelection
    global is_crashed
    #global votes
    votelist = [1]
    flag_reelection = True
    votes = 1
    has_voted.add(term_local)
    threads = []
    try:
        for hostport in serverlist:
            t = threading.Thread(target=reqvote, args=(hostport,votelist,))
            threads.append(t)
        for i in range(maxnum - 1):
            threads[i].start()
        for i in range(maxnum - 1):
            threads[i].join()
        #begin_ele = time.time()
        #while(time.time() - begin_ele < 500 / 1000):
        votes = sum(votelist)
        #print("votes received: ", votes)
        if flag_reelection == False:
            return
        if votes > maxnum / 2 and flag_reelection == True and is_crashed == False:
            isleader = True
            threads = []
            for hostport in serverlist:
                t = threading.Thread(target=appe, args=(hostport, term_local, logindex, fileinfomap,))
                threads.append(t)
            for i in range(maxnum - 1):
                threads[i].start()
            #leader_id = servernum
            print(servernum, " is leader now")
        flag_reelection = False
            
        return
        # t = threading.Thread(target=vote_to_leader)
        # t.start()
    except (ConnectionRefusedError, OSError):
        print("Server: ")


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="SurfStore server")
        parser.add_argument('config', help='path to config file')
        parser.add_argument('servernum', type=int, help='server number')

        args = parser.parse_args()

        config = args.config
        servernum = args.servernum

        # server list has list of other servers
        serverlist = []

        # maxnum is maximum number of servers
        maxnum, host, port = readconfig(config, servernum)

        hashmap = dict();

        fileinfomap = dict()

        term_local = 0
        votes = 0
        leader_id = -1
        flag_reelection = False
        is_crashed = False
        timeout_ele = False
        timeout_total = False
        isleader = False
        has_voted = set()
        logindex = 0

        print("Attempting to start XML-RPC Server...")
        print(host, port)
        server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping,"surfstore.ping")
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(updatefile,"surfstore.updatefile")
        # Project 3 APIs
        server.register_function(isLeader,"surfstore.isLeader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.isCrashed")
        server.register_function(requestVote,"surfstore.requestVote")
        server.register_function(appendEntries,"surfstore.appendEntries")
        server.register_function(tester_getversion,"surfstore.tester_getversion")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")

        election_timeout = random.randint(300, 500)
        #election_timeout = 600 + servernum * 200 / (maxnum - 1)
        total_timeout = 5
        hb_timeout = 50
        begin_ele = time.time()
        begin_total = time.time()
        begin_hb = time.time()

        print("election_timeout: ", election_timeout)

        t = threading.Thread(target=ele_time)
        t.start()
        t = threading.Thread(target=hb_time)
        t.start()

        #t.cancel()


        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))

