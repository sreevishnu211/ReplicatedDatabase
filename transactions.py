from enum import Enum
from operations import *
from datamanager import *

class TransactionStatus(Enum):
    ALIVE = 1
    COMPLETED = 2
    ABORTED = 3

class TransactionBaseClass:
    """
    Base class to represent both readonly and readwrite transactions.
    """
    def __init__(self, transactionId, startTime, dataManagers):
        """
        operations are a list of all the operations this transaction has received.
        dataManagers is a reference to all the dataManagers.
        dataManagersTouched are all the data managers that have been accessed for a 
        read/write by this transaction.
        """
        self.transactionId = transactionId
        self.startTime = startTime
        self.operations = []
        self.dataManagers = dataManagers
        self.status = TransactionStatus.ALIVE
        self.dataManagersTouched = set()
        self.isDeadlocked = False

    def processOperation(self, operation):
        raise Exception("TransactionBaseClass.processOperation not implemented.")


class ReadOnlyTransaction(TransactionBaseClass):
    """
    class to implement Read Only Transactions.
    """
    def __init__(self, transactionId, startTime, dataManagers):
        super().__init__(transactionId, startTime, dataManagers)
        print("Read Only Transaction {} begins.".format(self.transactionId))

    def readOperation(self, operation):
        """
        Read operation of a read only transaction.
        """
        if operation.status == OperationStatus.COMPLETED:
            return
        
        for dm in self.dataManagers.values():
            resultAndData = dm.readRecordForROTrans(operation.record, self.startTime)
            if resultAndData and resultAndData[0]:
                print("{} reads x{}.{} => {}".format(self.transactionId, operation.record, dm.dataManagerId, resultAndData[1]))
                operation.status = OperationStatus.COMPLETED
                return
        
        if operation.firstAttempt:
            print("{} will wait.".format(operation))
            operation.firstAttempt = False

    def processOperation(self, operation):
        """
        Processes all the operation given for a read only transaction.
        """
        if isinstance(operation, ReadOp):
            self.readOperation(operation)
        elif isinstance(operation, EndOp):
            self.endOperation(operation)
        elif isinstance(operation, WriteOp):
            print("InputError: Received a write operation - {} on a ReadOnly Transaction {}".format(operation, self.transactionId))
            exit()

    def endOperation(self, operation):
        """
        End operation of a read only transaction.
        """
        if operation.status == OperationStatus.COMPLETED:
            return

        if len(self.operations) > 0 and not isinstance( self.operations[-1], EndOp):
            print("InputError: {} has received an operation {} after the end operation".format(self.transactionId, self.operations[-1]))
            exit()

        allOperationStatus = [ self.operations[i].status == OperationStatus.COMPLETED for i in range(len(self.operations) - 1) ]

        if all(allOperationStatus): # TODO: Decide if you want to throw an error or wait for operations to complete
            print("{} commits.".format(self.transactionId))
            operation.status = OperationStatus.COMPLETED
            self.status = TransactionStatus.COMPLETED
        else:
            # or you can even do this stuff right after doing a read for a replicated data and if it fails with all
            # dms live then it will never pass. so abort then and there.
            # noOfDMsLive = 0
            # for dm in self.dataManagers.values():
            #     if dm.status == DataManagerStatus.LIVE:
            #         noOfDMsLive += 1
            # if noOfDMsLive == 10:
            #     print("{} aborts because there are no eligible sites to read".format(self.transactionId))
            #     for operation in self.operations:
            #         operation.status = OperationStatus.COMPLETED
            #     operation.status = OperationStatus.COMPLETED
            #     self.status = TransactionStatus.COMPLETED
            # else:
            #     print("InputError: received an {} when there are still operations pending in {}".format(operation, self.transactionId))
            #     exit()
            print("InputError: received an {} when there are still operations pending in {}".format(operation, self.transactionId))
            exit()


class ReadWriteTransaction(TransactionBaseClass):
    """
    class to implement a Read Write Transaction.
    """

    def __init__(self, transactionId, startTime, dataManagers):
        super().__init__(transactionId, startTime, dataManagers)
        print("Read Write Transaction {} begins.".format(self.transactionId))

    def readOperation(self, operation):
        """
        Read operation of a Read Write transaction.
        """
        if operation.status == OperationStatus.COMPLETED:
            return

        if self.status == TransactionStatus.ABORTED and self.isDeadlocked:
            print("{} has already been aborted due to a deadlock, so operation {} wont be executed.".format(self.transactionId, operation))
            operation.status = OperationStatus.COMPLETED
            return

        for dm in self.dataManagers.values():
            if dm.isReadOKForRWTrans(operation.record, self.transactionId):
                dm.requestReadLock(self.transactionId, operation.record)
                if dm.isReadLockAquired(self.transactionId, operation.record):
                    data = dm.readRecord(operation.record)
                    print("{} reads x{}.{} => {}".format(self.transactionId, operation.record, dm.dataManagerId, data))
                    self.dataManagersTouched.add(dm.dataManagerId)
                    operation.status = OperationStatus.COMPLETED
                    return

        if operation.firstAttempt:
            print("{} will wait.".format(operation))
            operation.firstAttempt = False



    def writeOperation(self, operation):
        """
        Write operation of a Read Write transaction
        """
        if operation.status == OperationStatus.COMPLETED:
            return

        if self.status == TransactionStatus.ABORTED and self.isDeadlocked:
            print("{} has already been aborted due to a deadlock, so operation {} wont be executed.".format(self.transactionId, operation))
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
            print("{} wrote {} to x{} in sites-{}".format(self.transactionId, operation.value, operation.record, wroteRecordTo))
            for dmId in wroteRecordTo:
                self.dataManagersTouched.add(dmId)
            operation.status = OperationStatus.COMPLETED
            return
        
        if operation.firstAttempt:
            print("{} will wait.".format(operation))
            operation.firstAttempt = False


    def processOperation(self, operation):
        """
        Processes all the operations pertaining to a Read Write transaction.
        """
        if isinstance(operation, ReadOp):
            self.readOperation(operation)
        elif isinstance(operation, WriteOp):
            self.writeOperation(operation)
        elif isinstance(operation, EndOp):
            self.endOperation(operation)


    def endOperation(self, operation):
        """
        End operation of a read write transaction.
        """
        if operation.status == OperationStatus.COMPLETED:
            return

        if self.status == TransactionStatus.ABORTED and self.isDeadlocked:
            print("{} was aborted due to a deadlock in the past.".format(self.transactionId))
            self.dataManagersTouched = set()
            operation.status = OperationStatus.COMPLETED
            self.status = TransactionStatus.COMPLETED
            return

        if len(self.operations) > 0 and not isinstance( self.operations[-1], EndOp):
            print("InputError: {} has received an operation {} after the end operation".format(self.transactionId, self.operations[-1]))
            exit()

        allOperationStatus = [ self.operations[i].status == OperationStatus.COMPLETED for i in range(len(self.operations) - 1) ]
        
        if all(allOperationStatus): # TODO: Decide if you want to throw an error or wait for operations to complete
            if self.status == TransactionStatus.ABORTED:
                for dataManager in self.dataManagers.values():
                    dataManager.removeUncommittedDataForTrans(self.transactionId)
                    dataManager.removeLocksForTrans(self.transactionId)
                print("{} aborts due to a site failure.".format(self.transactionId))
            else:
                for dataManager in self.dataManagers.values():
                    dataManager.commitTransaction(self.transactionId, operation.commitTime)
                    dataManager.removeLocksForTrans(self.transactionId)
                print("{} commits.".format(self.transactionId))
            
            self.dataManagersTouched = set()
            operation.status = OperationStatus.COMPLETED
            self.status = TransactionStatus.COMPLETED
        else:
            print("InputError: received an {} when there are still operations pending in {}".format(operation, self.transactionId))
            exit()
        
            

    def abortDeadlockedTransaction(self):
        """
        process to abort this transaction if it gets deadlocked.
        """
        self.isDeadlocked = True
        self.status = TransactionStatus.ABORTED
        self.dataManagersTouched = set()

        for operation in self.operations:
            operation.status = OperationStatus.COMPLETED

        for dataManager in self.dataManagers.values():
            dataManager.removeUncommittedDataForTrans(self.transactionId)
            dataManager.removeLocksForTrans(self.transactionId)
        print("{} was aborted due to a deadlock".format(self.transactionId))