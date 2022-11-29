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
    def __init__(self):
        self.versions = deque([])
        self.recovered = True
        self.locks = deque([])

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
    
    def removeUncommitedVersions(self, transactionId):
        newVersions = deque([])
        for i in range( len(self.versions) ):
            if self.versions[i].commitTime == None and self.versions[i].transactionId == transactionId:
                continue
            else:
                newVersions.append(self.versions[i])

        self.versions = newVersions
    
    def getLatestData(self):
        for i in range( len(self.versions) ):
            if self.versions[i].commitTime != None:
                return self.versions[i].data
        
        return None

    def getDataAtSpecificTime(self, requestedTime):
        for i in range( len(self.versions) ):
            if self.versions[i].commitTime != None and self.versions[i].commitTime <= requestedTime:
                return self.versions[i].data
        
        return None