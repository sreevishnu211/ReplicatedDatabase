from record import *
from enum import Enum
from collections import OrderedDict

class DataManagerStatus(Enum):
    LIVE = 1
    FAILED = 2
    RECOVERING = 3

class DataManager:
    def __init__(self, dataManagerId, numOfRecords):
        self.dataManagerId = dataManagerId
        self.status = DataManagerStatus.LIVE
        self.failedTimes = []
        self.records = OrderedDict()
        for i in range(1, numOfRecords + 1):
            if i % 2 == 0:
                self.records[i] = Record(initialValue=i*10, replicated=True)
            elif self.dataManagerId == 1 + ( i % 10 ):
                self.records[i] = Record(initialValue=i*10, replicated=False)

    
    def isReadOKForRWTrans(self, record):
        if self.status == DataManagerStatus.FAILED:
            return False

        if record in self.records and self.records[record].recovered:
            return True
        else:
            return False

    def isWriteOKForRWTrans(self, record):
        if self.status == DataManagerStatus.FAILED:
            return False
        
        if record in self.records:
            return True
        else:
            return False

    def requestReadLock(self, transactionId, record):
        if record in self.records:
            self.records[record].addLockRequest(transactionId, LockType.READ)

    def requestWriteLock(self, transactionId, record):
        if record in self.records:
            self.records[record].addLockRequest(transactionId, LockType.WRITE)

    def isReadLockAquired(self, transactionId, record):
        if record in self.records:
            return self.records[record].isLockAquired(transactionId, LockType.READ)
        else:
            return False

    def isWriteLockAquired(self, transactionId, record):
        if record in self.records:
            return self.records[record].isLockAquired(transactionId, LockType.WRITE)
        else:
            return False
    
    def readRecord(self, record):
        if record in self.records:
            return self.records[record].getLatestData()
        else:
            return None
    
    def writeRecord(self, record, value, transactionId, commitTime=None ):
        if record in self.records:
            self.records[record].insertNewVersion(value, transactionId, commitTime)

    def fail(self, failureTime):
        if self.status == DataManagerStatus.FAILED:
            raise Exception("Site {} is already failed".format(self.dataManagerId))
        
        self.status = DataManagerStatus.FAILED
        self.failedTimes.append(failureTime)
        for record in self.records:
            record.fail()


    def recover(self):
        if self.status == DataManagerStatus.FAILED:
            self.status = DataManagerStatus.RECOVERING
        else:
            raise Exception("Site {} is already live or recovering.".format(self.dataManagerId))

    def dump(self):
        result = []
        for recordId, record in self.records.items():
            result.append("x"+ str(recordId) + ":" + str(record.getLatestCommittedData()))
        print("Site " + str(self.dataManagerId) + ": " + " ".join(result))
        
        