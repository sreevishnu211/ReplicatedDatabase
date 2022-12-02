from enum import Enum

class OperationStatus(Enum):
    IN_PROGRESS = 1
    COMPLETED = 2

class BeginOp:
    def __init__(self, transactionId):
        self.transactionId = transactionId

    def __str__(self):
        return "begin({})".format(str(self.transactionId))

class BeginROOp:
    def __init__(self, transactionId):
        self.transactionId = transactionId
    
    def __str__(self):
        return "beginRO({})".format(str(self.transactionId))

class ReadOp:
    def __init__(self, transactionId, record):
        self.transactionId = transactionId
        self.record = int(record[1:])
        self.status = OperationStatus.IN_PROGRESS

    def __str__(self):
        return "R({},{})".format(str(self.transactionId), "x" + str(self.record))

class WriteOp:
    def __init__(self, transactionId, record, value):
        self.transactionId = transactionId
        self.record = int(record[1:])
        self.value = value
        self.status = OperationStatus.IN_PROGRESS

    def __str__(self):
        return "W({},{},{})".format(str(self.transactionId), "x" + str(self.record), str(self.value))

class DumpOp:
    def __init__(self):
        pass

    def __str__(self):
        return "dump()"

class EndOp:
    def __init__(self, transactionId):
        self.transactionId = transactionId
        self.commitTime = None
        self.status = OperationStatus.IN_PROGRESS
    
    def __str__(self):
        return "end({})".format(str(self.transactionId))

class FailOp:
    def __init__(self, site):
        self.site = int(site)

    def __str__(self):
        return "fail({})".format(str(self.site))
        
class RecoverOp:
    def __init__(self, site):
        self.site = int(site)

    def __str__(self):
        return "recover({})".format(str(self.site))