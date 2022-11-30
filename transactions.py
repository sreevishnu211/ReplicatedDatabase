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

        if operation.lockRequestedSite == None:
            for key, dm in self.dataManagers:
                if dm.isReadOKForRWTrans(operation.record):
                    dm.requestReadLock(self.transactionId, operation.record)
                    operation.lockRequestedSite = dm.dataManagerId
                    break
        
        if operation.lockRequestedSite != None:
            if self.dataManagers[operation.lockRequestedSite].isReadOKForRWTrans(operation.record):
                if self.dataManagers[operation.lockRequestedSite].isReadLockAquired(self.transactionId, operation.record):
                    data = self.dataManagers[operation.lockRequestedSite].readRecord(operation.record)
                    operation.status = OperationStatus.COMPLETED
                    print("{} read from {} and got {}".format(self.transactionId, operation.record, data))
                    return
            else:
                # TODO: You hit this step, when the site from which you initially req lock from failed.
                # So this transaction should now be in aborted state.
                # the most I can do here is if i still have to complete this transaction, i would have to 
                # get a lock from a new place, by setting operation.lockRequestedSite = None and 
                # recursively calling this function.
                pass



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