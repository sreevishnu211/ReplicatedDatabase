from enum import Enum

class TransactionStatus(Enum):
    ALIVE = 1
    COMPLETED = 2
    ABORTED = 3

class TransactionBaseClass:
    def __init__(self, transactionId, startTime):
        self.transactionId = transactionId
        self.startTime = startTime
        self.operations = []
        self.status = TransactionStatus.ALIVE

    def processOperation(self, operation):
        raise Exception("TransactionBaseClass.processOperation not implemented.")
    
    def finishTransaction(self):
        raise Exception("TransactionBaseClass.finishTransaction not implemented.")

    def abortTransaction(self):
        raise Exception("TransactionBaseClass.abortTransaction not implemented.")


class ReadOnlyTransaction(TransactionBaseClass):
    def __init__(self, transactionId, startTime):
        super().__init__(transactionId, startTime)
        print("Read Only Transaction {} begins.".format(self.transactionId))

    def processOperation(self, operation):
        pass

    def finishTransaction(self):
        pass

    def abortTransaction(self):
        pass


class ReadWriteTransaction(TransactionBaseClass):
    def __init__(self, transactionId, startTime):
        super().__init__(transactionId, startTime)
        print("Read Write Transaction {} begins.".format(self.transactionId))

    def processOperation(self, operation):
        pass

    def finishTransaction(self):
        pass

    def abortTransaction(self):
        pass

    def refreshOperations(self):
        pass