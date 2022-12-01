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
        self.isDeadlocked = False

    def processOperation(self, operation):
        raise Exception("TransactionBaseClass.processOperation not implemented.")
    
    def endOperation(self):
        raise Exception("TransactionBaseClass.finishTransaction not implemented.")

    def abortDeadlockedTransaction(self):
        raise Exception("TransactionBaseClass.abortTransaction not implemented.")


class ReadOnlyTransaction(TransactionBaseClass):
    def __init__(self, transactionId, startTime, dataManagers):
        super().__init__(transactionId, startTime, dataManagers)
        print("Read Only Transaction {} begins.".format(self.transactionId))

    def readOperation(self, operation):
        if operation.status == OperationStatus.COMPLETED:
            return
        
        for dm in self.dataManagers.values():
            resultAndData = dm.readRecordForROTrans(operation.record, self.startTime)
            if resultAndData and resultAndData[0]:
                print("{} read from {} and got {}".format(self.transactionId, operation.record, resultAndData[1]))
                operation.status = OperationStatus.COMPLETED
                return

    def processOperation(self, operation):
        self.operations.append(operation)
        if isinstance(operation, ReadOp):
            self.readOperation(operation)
        elif isinstance(operation, EndOp):
            self.endOperation(operation)
        elif isinstance(operation, WriteOp):
            print("Error: Received a write operation - {} on a ReadOnly Transaction {}".format(operation, self.transactionId))
            exit()

    def endOperation(self, operation):
        if operation.status == OperationStatus.COMPLETED:
            return

        if len(self.operations) > 0 and not isinstance( self.operations[-1], EndOp):
            print("Transaction {} has received an operation {} after the end operation".format(self.transactionId, self.operations[-1]))
            exit()

        allOperationStatus = [ self.operations[i].status == OperationStatus.COMPLETED for i in range(len(self.operations) - 1) ]

        if all(allOperationStatus): # TODO: Decide if you want to throw an error or wait for operations to complete
            print("Transaction {} was committed.".format(self.transactionId))
            operation.status = OperationStatus.COMPLETED
            self.status = TransactionStatus.COMPLETED


class ReadWriteTransaction(TransactionBaseClass):
    def __init__(self, transactionId, startTime, dataManagers):
        super().__init__(transactionId, startTime, dataManagers)
        print("Read Write Transaction {} begins.".format(self.transactionId))

    def readOperation(self, operation):
        if operation.status == OperationStatus.COMPLETED:
            return

        if self.status == TransactionStatus.ABORTED and self.isDeadlocked:
            # TODO: Revisit this.
            print("Transaction - {} has already been aborted due to a deadlock, so operation {} wont be executed.".format(self.transactionId, operation))
            operation.status = OperationStatus.COMPLETED
            return

        for dm in self.dataManagers.values():
            if dm.isReadOKForRWTrans(operation.record, self.transactionId):
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

        if self.status == TransactionStatus.ABORTED and self.isDeadlocked:
            # TODO: Revisit this.
            print("Transaction - {} has already been aborted due to a deadlock, so operation {} wont be executed.".format(self.transactionId, operation))
            operation.status = OperationStatus.COMPLETED
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
        elif isinstance(operation, EndOp):
            self.endOperation(operation)


    def endOperation(self, operation):
        if operation.status == OperationStatus.COMPLETED:
            return

        if self.status == TransactionStatus.ABORTED and self.isDeadlocked:
            print("Transaction {} was aborted due to a deadlock in the past.".format(self.transactionId))
            operation.status = OperationStatus.COMPLETED
            self.status = TransactionStatus.COMPLETED
            return

        if len(self.operations) > 0 and not isinstance( self.operations[-1], EndOp):
            print("Transaction {} has received an operation {} after the end operation".format(self.transactionId, self.operations[-1]))
            exit()

        allOperationStatus = [ self.operations[i].status == OperationStatus.COMPLETED for i in range(len(self.operations) - 1) ]
        
        if all(allOperationStatus): # TODO: Decide if you want to throw an error or wait for operations to complete
            if self.status == TransactionStatus.ABORTED:
                for dataManager in self.dataManagers.values():
                    dataManager.removeUncommittedDataForTrans(self.transactionId)
                    dataManager.removeLocksForTrans(self.transactionId)
                print("Transaction {} was aborted due to a site failure.".format(self.transactionId))
            else:
                for dataManager in self.dataManagers.values():
                    dataManager.commitTransaction(self.transactionId, operation.commitTime)
                    dataManager.removeLocksForTrans(self.transactionId)
                print("Transaction {} was committed.".format(self.transactionId))
            
            self.dataManagersTouched = set()
            operation.status = OperationStatus.COMPLETED
            self.status = TransactionStatus.COMPLETED
        
            

    def abortDeadlockedTransaction(self):
        self.isDeadlocked = True
        self.status = TransactionStatus.ABORTED
        self.dataManagersTouched = set()

        for operation in self.operations:
            operation.status = OperationStatus.COMPLETED

        for dataManager in self.dataManagers.values():
            dataManager.removeUncommittedDataForTrans(self.transactionId)
            dataManager.removeLocksForTrans(self.transactionId)
        print("Transaction {} was aborted due to a deadlock".format(self.transactionId))