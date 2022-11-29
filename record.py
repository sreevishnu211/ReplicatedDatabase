from collections import deque

class RecordVersion:
    def __init__(self, data, transactionId, commitTime = None):
        self.data = data
        self.transactionId = transactionId
        self.commitTime = commitTime

class Record:
    def __init__(self):
        self.versions = deque([])

    def insertNewVersion(self, data, transactionId, commitTime = None):
        self.versions.appendleft(RecordVersion(data, transactionId, commitTime))
    
    def remUncomVersForTrans(self, transactionId):
        newVersions = deque([])
        for i in range( len(self.versions) ):
            if self.versions[i].commitTime == None and self.versions[i].transactionId == transactionId:
                continue
            else:
                newVersions.append(self.versions[i])

        self.versions = newVersions
    
    def getLatestData(self):
        for i in range( len(self.versions) ):
            if self.versions[i].commitTime != None:
                return self.versions[i].data
        
        return None

    def getDataAtSpecificTime(self, requestedTime):
        for i in range( len(self.versions) ):
            if self.versions[i].commitTime != None and self.versions[i].commitTime <= requestedTime:
                return self.versions[i].data
        
        return None