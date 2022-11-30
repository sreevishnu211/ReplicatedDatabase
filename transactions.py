from enum import Enum
from operations import *
from datamanager import *

class TransactionStatus(Enum):
    ALIVE = 1
    COMPLETED = 2
    ABORTED = 3

class TransactionBaseClass:
    def __init__(self, transactionId, startTime, dataManagers):
        self.transactionId = transactionId
        self.startTime = startTime
        self.operations = []
        self.dataManagers = dataManagers
        self.status = TransactionStatus.ALIVE
        self.dataManagersTouched = set()

    def processOperation(self, operation):
        raise Exception("TransactionBaseClass.processOperation not implemented.")
    
    def finishTransaction(self):
        raise Exception("TransactionBaseClass.finishTransaction not implemented.")

    def abortTransaction(self):
        raise Exception("TransactionBaseClass.abortTransaction not implemented.")


class ReadOnlyTransaction(TransactionBaseClass):
    def __init__(self, transactionId, startTime, dataManagers):
        super().__init__(transactionId, startTime, dataManagers)
        print("Read Only Transaction {} begins.".format(self.transactionId))

    def processOperation(self, operation):
        pass

    def finishTransaction(self):
        pass

    def abortTransaction(self):
        pass


class ReadWriteTransaction(TransactionBaseClass):
    def __init__(self, transactionId, startTime, dataManagers):
        super().__init__(transactionId, startTime, dataManagers)
        print("Read Write Transaction {} begins.".format(self.transactionId))

    def readOperation(self, operation):
        if operation.status == OperationStatus.COMPLETED:
            return

        for dm in self.dataManagers.values():
            if dm.isReadOKForRWTrans(operation.record):
                dm.requestReadLock(self.transactionId, operation.record)
                if dm.isReadLockAquired(self.transactionId, operation.record):
                    data = dm.readRecord(operation.record)
                    print("{} read from {} and got {}".format(self.transactionId, operation.record, data))
                    self.dataManagersTouched.add(dm.dataManagerId)
                    operation.status = OperationStatus.COMPLETED
                    return



    def writeOperation(self, operation):
        if operation.status == OperationStatus.COMPLETED:
            return
        
        writeLockStatus = []
        for dm in self.dataManagers.values():
            if dm.isWriteOKForRWTrans(operation.record):
                dm.requestWriteLock(self.transactionId, operation.record)
                writeLockStatus.append( dm.isWriteLockAquired(self.transactionId, operation.record) )

        wroteRecordTo = []
        if len(writeLockStatus) > 0 and all(writeLockStatus):
            for dm in self.dataManagers.values():
                if dm.isWriteOKForRWTrans(operation.record):
                    dm.writeRecord(operation.record, operation.value, self.transactionId, None)
                    wroteRecordTo.append(dm.dataManagerId)

        if len(wroteRecordTo) > 0:
            print("{} wrote the value {} to record {} in {}".format(self.transactionId, operation.value, operation.record, wroteRecordTo))
            for dmId in wroteRecordTo:
                self.dataManagersTouched.add(dmId)
            operation.status = OperationStatus.COMPLETED


    def processOperation(self, operation):
        self.operations.append(operation)
        if isinstance(operation, ReadOp):
            self.readOperation(operation)
        elif isinstance(operation, WriteOp):
            self.writeOperation(operation)


    def finishTransaction(self):
        pass

    def abortTransaction(self):
        pass

    def refreshOperations(self):
        pass