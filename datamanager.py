from record import *
from enum import Enum
from collections import OrderedDict

class DataManagerStatus(Enum):
    LIVE = 1
    FAILED = 2

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

    
    def isReadOKForRWTrans(self, record, transactionId):
        if self.status == DataManagerStatus.FAILED:
            return False

        if record in self.records and ( self.records[record].recovered or self.records[record].versions[0].transactionId == transactionId ):
            return True
        else:
            return False

    def readRecordForROTrans(self, record, transStartTime):
        if self.status == DataManagerStatus.FAILED or record not in self.records:
            return [False, None]
        
        versionToRead = None
        for version in self.records[record].versions:
            if version.commitTime != None and version.commitTime <= transStartTime:
                versionToRead = version
                break
        if versionToRead == None:
            versionToRead = self.records[record].versions[-1]
        
        if self.records[record].replicated:
            for failTimes in self.failedTimes:
                if versionToRead.commitTime < failTimes < transStartTime:
                    return [False, None]
        
        return [True,versionToRead.data]
        



    def isWriteOKForRWTrans(self, record):
        if self.status == DataManagerStatus.FAILED:
            return False
        
        if record in self.records:
            return True
        else:
            return False

    def requestReadLock(self, transactionId, record):
        if self.status == DataManagerStatus.FAILED:
            return

        if record in self.records:
            self.records[record].addLockRequest(transactionId, LockType.READ)

    def requestWriteLock(self, transactionId, record):
        if self.status == DataManagerStatus.FAILED:
            return

        if record in self.records:
            self.records[record].addLockRequest(transactionId, LockType.WRITE)

    def isReadLockAquired(self, transactionId, record):
        if self.status == DataManagerStatus.FAILED:
            return False

        if record in self.records:
            return self.records[record].isLockAquired(transactionId, LockType.READ)
        else:
            return False

    def isWriteLockAquired(self, transactionId, record):
        if self.status == DataManagerStatus.FAILED:
            return False

        if record in self.records:
            return self.records[record].isLockAquired(transactionId, LockType.WRITE)
        else:
            return False
    
    def readRecord(self, record):
        if self.status == DataManagerStatus.FAILED:
            return None

        if record in self.records:
            return self.records[record].getLatestData()
        else:
            return None
    
    def writeRecord(self, record, value, transactionId, commitTime=None ):
        if self.status == DataManagerStatus.FAILED:
            return

        if record in self.records:
            self.records[record].insertNewVersion(value, transactionId, commitTime)

    def fail(self, failureTime):
        if self.status == DataManagerStatus.FAILED:
            raise Exception("Site {} is already failed".format(self.dataManagerId))
            exit()
        
        self.status = DataManagerStatus.FAILED
        self.failedTimes.append(failureTime)
        for record in self.records.values():
            record.fail()


    def recover(self):
        if self.status == DataManagerStatus.FAILED:
            self.status = DataManagerStatus.LIVE
        else:
            raise Exception("Site {} is already live.".format(self.dataManagerId))

    def dump(self):
        result = []
        for recordId, record in self.records.items():
            result.append("x"+ str(recordId) + ":" + str(record.getLatestCommittedData()))
        print("Site " + str(self.dataManagerId) + ": " + " ".join(result))

    def removeUncommittedDataForTrans(self, transactionId):
        for record in self.records.values():
            record.removeUncommittedVersionForTrans(transactionId)
    
    def removeLocksForTrans(self, transactionId):
        for record in self.records.values():
            record.removeLocksForTrans(transactionId)

    def commitTransaction(self, transactionId, commitTime):
        # TODO: Make sure all operations are happening only when dm is alive and not failed.
        if self.status == DataManagerStatus.FAILED:
            return

        for record in self.records.values():
            record.commitTransaction(transactionId, commitTime)

    def getBlockingRelations(self):
        if self.status == DataManagerStatus.FAILED:
            return set()

        blockingRelations = set()
        for record in self.records.values():
            blockingRelations.update(record.getBlockingRelations())
        return blockingRelations
        
        