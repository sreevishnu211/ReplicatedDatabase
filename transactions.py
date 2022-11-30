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

        if not operation.lockRequested:
            for dm in self.dataManagers.values():
                if dm.isReadOKForRWTrans(operation.record):
                    dm.requestReadLock(self.transactionId, operation.record)
                    operation.lockRequested = True
                    
        
        if operation.lockRequested:
            for dm in self.dataManagers.values():
                if dm.isReadOKForRWTrans(operation.record) and dm.isReadLockAquired(self.transactionId, operation.record):
                    data = dm.readRecord(operation.record)
                    print("{} read from {} and got {}".format(self.transactionId, operation.record, data))
                    self.dataManagersTouched.add(dm.dataManagerId)
                    operation.status = OperationStatus.COMPLETED
                    return



    def writeOperation(self, operation):
        pass

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