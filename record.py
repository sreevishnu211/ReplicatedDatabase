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
    def __init__(self, initialValue):
        self.versions = deque([])
        self.recovered = True
        self.locks = deque([])
        self.versions.appendleft(RecordVersion(initialValue, "initialValue", 0))

    def insertNewVersion(self, data, transactionId, commitTime = None):
        self.versions.appendleft(RecordVersion(data, transactionId, commitTime))
        self.recovered = True

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
    
    def removeUncommitedVersions(self, transactionId):
        newVersions = deque([])
        for i in range( len(self.versions) ):
            if self.versions[i].commitTime == None and self.versions[i].transactionId == transactionId:
                continue
            else:
                newVersions.append(self.versions[i])

        self.versions = newVersions
    
    def getLatestData(self):
        # TODO: probably just return the latest data. because if there is a write from a trans 
        # and the same trans reads it, then we should return the writes value.
        # for i in range( len(self.versions) ):
        #     if self.versions[i].commitTime != None:
        #         return self.versions[i].data
        
        # return None
        return self.versions[0].data

    def getDataAtSpecificTime(self, requestedTime):
        for i in range( len(self.versions) ):
            if self.versions[i].commitTime != None and self.versions[i].commitTime <= requestedTime:
                return self.versions[i].data
        
        return None