from record import Record
from enum import Enum

class DataManagerStatus(Enum):
    LIVE = 1
    FAILED = 2
    RECOVERING = 3

class DataManager:
    def __init__(self, dataManagerId, numOfRecords):
        self.dataManagerId = dataManagerId
        self.status = DataManagerStatus.LIVE
        self.lastFailedTime = 0
        self.records = {}
        for i in range(1, numOfRecords + 1):
            if i % 2 == 0:
                self.records[i] = Record()
            elif self.dataManagerId == 1 + ( i % 10 ):
                self.records[i] = Record()
        
        