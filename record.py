from collections import deque
from enum import Enum

class LockType(Enum):
    READ = 1
    WRITE = 1

class Lock:
    def __init__(self, transactionId, lockType):
        self.transactionId = transactionId
        self.lockType = lockType

class RecordVersion:
    def __init__(self, data, transactionId, commitTime = None):
        self.data = data
        self.transactionId = transactionId
        self.commitTime = commitTime

class Record:
    def __init__(self, initialValue, replicated= False):
        self.versions = deque([])
        self.recovered = True
        self.locks = deque([])
        self.replicated = replicated
        self.versions.appendleft(RecordVersion(initialValue, "initialValue", 0))

    def insertNewVersion(self, data, transactionId, commitTime = None):
        self.versions.appendleft(RecordVersion(data, transactionId, commitTime))

    def addLockRequest(self, transactionId, lockType):
        if lockType == LockType.READ:
            for lock in self.locks:
                if lock.transactionId == transactionId:
                    return
        else:
            for lock in self.locks:
                if lock.transactionId == transactionId and lock.lockType == LockType.WRITE:
                    return
        self.locks.append( Lock(transactionId, lockType) )

    def isLockAquired(self, transactionId, lockType):
        if lockType == LockType.READ:
            for lock in self.locks:
                if lock.transactionId == transactionId:
                    return True
                if lock.transactionId != transactionId and lock.lockType == LockType.WRITE:
                    return False
        else:
            for lock in self.locks:
                if lock.transactionId == transactionId and lock.lockType == LockType.WRITE:
                    return True
                if lock.transactionId != transactionId:
                    return False
        
        return False
    
    def removeUncommittedVersionForTrans(self, transactionId):
        newVersions = deque([])
        for version in self.versions:
            if version.commitTime == None and version.transactionId == transactionId:
                continue
            else:
                newVersions.append(version)

        self.versions = newVersions

    def removeAllUncommitedVersions(self):
        newVersions = deque([])
        for version in self.versions:
            if version.commitTime == None:
                continue
            else:
                newVersions.append(version)
        self.versions = newVersions

    def fail(self):
        self.removeAllUncommitedVersions()
        if self.replicated:
            self.recovered = False
        self.locks = deque([])
        
    
    def getLatestData(self):
        # TODO: probably just return the latest data. because if there is a write from a trans 
        # and the same trans reads it, then we should return the writes value.
        # for i in range( len(self.versions) ):
        #     if self.versions[i].commitTime != None:
        #         return self.versions[i].data
        
        # return None
        return self.versions[0].data

    def getLatestCommittedData(self):
        for version in self.versions:
            if version.commitTime != None:
                return version.data

    def removeLocksForTrans(self, transactionId):
        newLocks = deque([])
        for lock in self.locks:
            if lock.transactionId == transactionId:
                continue
            else:
                newLocks.append(lock)
        self.locks = newLocks

    def commitTransaction(self, transactionId, commitTime):
        for version in self.versions:
            if version.commitTime == None and version.transactionId == transactionId:
                version.commitTime = commitTime
        self.recovered = True

    def getDataAtSpecificTime(self, requestedTime):
        for i in range( len(self.versions) ):
            if self.versions[i].commitTime != None and self.versions[i].commitTime <= requestedTime:
                return self.versions[i].data
        
        return None

    def getBlockingRelations(self):
        blockingRelations = set()
        for current in range(len(self.locks)):
            for previous in range(current):
                if self.blocking(self.locks[previous],self.locks[current]):
                    blockingRelations.add((self.locks[current].transactionId, self.locks[previous].transactionId))

        return blockingRelations

    def blocking(self, previous, current):
        if previous.lockType == LockType.READ and current.lockType == LockType.READ:
            return False
        if previous.transactionId == current.transactionId:
            return False
        
        return True
        
        
