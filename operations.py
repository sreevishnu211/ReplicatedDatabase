from enum import Enum

class OperationStatus(Enum):
    IN_PROGRESS = 1
    COMPLETED = 2

class BeginOp:
    def __init__(self, transactionId):
        self.transactionId = transactionId

class BeginROOp:
    def __init__(self, transactionId):
        self.transactionId = transactionId

class ReadOp:
    def __init__(self, transactionId, variable):
        self.transactionId = transactionId
        self.variable = int(variable[1:])
        self.status = OperationStatus.IN_PROGRESS

class WriteOp:
    def __init__(self, transactionId, variable, value):
        self.transactionId = transactionId
        self.variable = int(variable[1:])
        self.value = value
        self.status = OperationStatus.IN_PROGRESS

class DumpOp:
    def __init__(self):
        pass

class EndOp:
    def __init__(self, transactionId):
        self.transactionId = transactionId

class FailOp:
    def __init__(self, site):
        self.site = int(site)
        
class RecoverOp:
    def __init__(self, site):
        self.site = int(site)