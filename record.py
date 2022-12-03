from collections import deque
from enum import Enum

class LockType(Enum):
    READ = 1
    WRITE = 2

class Lock:
    """
    This class represents either a read/write lock request for a particular transaction.
    """

    def __init__(self, transactionId, lockType):
        self.transactionId = transactionId
        self.lockType = lockType
    
    def __str__(self):
        return "{}.W".format(self.transactionId) if self.lockType == LockType.WRITE else "{}.R".format(self.transactionId)

class RecordVersion:
    """
    Each record will have versions.
    So that we can facilitate Multiversion Reads.
    And also to store temporary data as a new version which hasnt commited yet.
    Each object of this calss represents a single version of the record.
    """

    def __init__(self, data, transactionId, commitTime = None):
        """
        data is the actual data pertaining to this version of the record.
        commitTime specifies the time if the reocord is committed, 
        else its none and signifies that its not committed yet.
        """
        self.data = data
        self.transactionId = transactionId
        self.commitTime = commitTime

class Record:
    """
    Each object of this class is one of the 20 records.
    It facilitates versioning, locking, failure, and recovery.
    """

    def __init__(self, initialValue, replicated= False):
        """
        initialValue - starting value of this record, which is usually 10*recordNumber.
        replicated - is true for even numbered records.
        versions - is a deque of RecordVersion objects, and ones closer to zero index are the 
        most recently added versions.
        locks - is a deque of Lock objects, and the ones closer to zero index are the ones closer 
        to getting the lock.
        """
        self.versions = deque([])
        self.recovered = True
        self.locks = deque([])
        self.replicated = replicated
        self.versions.appendleft(RecordVersion(initialValue, "initialValue", 0))

    def insertNewVersion(self, data, transactionId, commitTime = None):
        """
        Appends a new version to the zero index of self.versions.
        """
        self.versions.appendleft(RecordVersion(data, transactionId, commitTime))

    def addLockRequest(self, transactionId, lockType):
        """
        Adds a read lock request to the queue if there already isnt a read/write lock
        request present for the same transaction.
        Adds a write lock request to the queue if there already isnt a write lock request
        present for the same transaction.
        """
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
        """
        For a read lock it checks to see if the lock request is at index 0
        or if the read lock can be shared among transactions.
        For a write lock it checks to see if the write lock is at index 0
        or if any other read/write lock is blocking this transactions 
        write lock.
        """
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
        """
        self.versions stores uncommitted versions, this method removes
        uncommitted versions for a particular transaction id.
        """
        newVersions = deque([])
        for version in self.versions:
            if version.commitTime == None and version.transactionId == transactionId:
                continue
            else:
                newVersions.append(version)

        self.versions = newVersions

    def removeAllUncommitedVersions(self):
        """
        self.versions stores uncommitted versions, this method removes
        all uncommitted data.
        """
        newVersions = deque([])
        for version in self.versions:
            if version.commitTime == None:
                continue
            else:
                newVersions.append(version)
        self.versions = newVersions

    def fail(self):
        """
        Process that happens when a datamanager fails.
        remove all uncommitted versions.
        set recovered flags appropriately.
        clear locks.
        """
        self.removeAllUncommitedVersions()
        if self.replicated:
            self.recovered = False
        self.locks = deque([])
        
    
    def getLatestData(self):
        """
        this method returns uncommitted data if it is present.
        """
        return self.versions[0].data

    def getLatestCommittedData(self):
        """
        this method returns the latest committed data on self.versions.
        """
        for version in self.versions:
            if version.commitTime != None:
                return version.data

    def removeLocksForTrans(self, transactionId):
        """
        remove all the locks for a trans.
        gets called when transaction is ending.
        """
        newLocks = deque([])
        for lock in self.locks:
            if lock.transactionId == transactionId:
                continue
            else:
                newLocks.append(lock)
        self.locks = newLocks

    def commitTransaction(self, transactionId, commitTime):
        """
        commits all records for a transaction by setting commitTime.
        """
        for version in self.versions:
            if version.commitTime == None and version.transactionId == transactionId:
                version.commitTime = commitTime
                self.recovered = True

    def getBlockingRelations(self):
        """
        If the locks has values [T1.R, T2.R, T3.W]
        Then T1 will block T3 and also T2 will block T3.
        """
        blockingRelations = set()
        for current in range(len(self.locks)):
            for previous in range(current):
                # print("{} - {} -> {}".format(self.locks[previous], self.locks[current],self.blocking(self.locks[previous],self.locks[current]) ))
                if self.blocking(self.locks[previous],self.locks[current]):
                    blockingRelations.add((self.locks[current].transactionId, self.locks[previous].transactionId))
        return blockingRelations

    def blocking(self, previous, current):
        """
        This determines if the previous transaction blocks the current transaction
        that we are trying to test.
        Only in cases where both the transactions are read locks or if the lock
        requests are from the same transaction there wont be blocking.
        In every other case there will be a block.
        """
        if previous.lockType == LockType.READ and current.lockType == LockType.READ:
            return False
        if previous.transactionId == current.transactionId:
            return False
        
        return True
        
        
