from collections import OrderedDict
from datamanager import DataManager
from operations import *
import re
import sys
from transactions import *

class TransactionManager:
    def __init__(self, numOfSites, numOfRecords, fileName):
        self.numOfSites = numOfSites
        self.numOfRecords = numOfRecords
        
        if fileName:
            self.inputFile = open(fileName)
        else:
            self.inputFile = sys.stdin

        self.time = 0
        self.allTransactions = OrderedDict()
        self.dataManagers = OrderedDict()
        for i in range(1, self.numOfSites + 1):
            self.dataManagers[i] = DataManager(i, self.numOfRecords)

        
    def parseInput(self, line):
        line = line.strip()

        commentSplit = line.split("//")
        if commentSplit[0].strip():
            line = commentSplit[0].strip()
        else:
            return None
        
        
        def parseArgs(inp):
            inp = inp.strip("() ").split(",")
            temp = []
            for elem in inp:
                if elem.strip() and elem.strip().isalnum():
                    temp.append(elem.strip())
            return temp
        

        try:
            if not re.match("^(begin|beginRO|R|W|dump|end|fail|recover)[(]([A-Z]|[a-z]|[0-9]|,| )*[)]$", line):
                raise Exception()    
            
            if re.match("^begin[(]([A-Z]|[a-z]|[0-9]|,| )*[)]$", line):
                args = parseArgs(line[5:])
                if len(args) == 1:
                    return BeginOp(args[0])
                else:
                    raise Exception()
            elif re.match("^beginRO[(]([A-Z]|[a-z]|[0-9]|,| )*[)]$", line):
                args = parseArgs(line[7:])
                if len(args) == 1:
                    return BeginROOp(args[0])
                else:
                    raise Exception()
            elif re.match("^R[(]([A-Z]|[a-z]|[0-9]|,| )*[)]$", line):
                args = parseArgs(line[1:])
                if len(args) == 2 and re.match("^x([1-9]|1[0-9]|20)$", args[1]):
                    return ReadOp(args[0], args[1])
                else:
                    raise Exception()
            elif re.match("^W[(]([A-Z]|[a-z]|[0-9]|,| )*[)]$", line):
                args = parseArgs(line[1:])
                if len(args) == 3 and re.match("^x([1-9]|1[0-9]|20)$", args[1]):
                    return WriteOp(args[0], args[1], args[2])
                else:
                    raise Exception()
            elif re.match("^dump[(]([A-Z]|[a-z]|[0-9]|,| )*[)]$", line):
                args = parseArgs(line[4:])
                if len(args) == 0:
                    return DumpOp()
                else:
                    raise Exception()
            elif re.match("^end[(]([A-Z]|[a-z]|[0-9]|,| )*[)]$", line):
                args = parseArgs(line[3:])
                if len(args) == 1:
                    return EndOp(args[0])
                else:
                    raise Exception()
            elif re.match("^fail[(]([A-Z]|[a-z]|[0-9]|,| )*[)]$", line):
                args = parseArgs(line[4:])
                if len(args) == 1 and re.match("^([1-9]|10)$", args[0]):
                    return FailOp(args[0])
                else:
                    raise Exception()
            elif re.match("^recover[(]([A-Z]|[a-z]|[0-9]|,| )*[)]$", line):
                args = parseArgs(line[7:])
                if len(args) == 1 and re.match("^([1-9]|10)$", args[0]):
                    return RecoverOp(args[0])
                else:
                    raise Exception()
            else:
                raise Exception()
            
            
        except:
            print("The given input - {} doesnt match the input requirements.".format(line))
            exit()

    def dump(self):
        pass

    def checkAndDealWithDeadlock(self):
        pass

    def refreshTransactions(self):
        pass


    def run(self):
        for line in self.inputFile:
            if line.strip().lower() == "quit":
                quit()
            
            operation = self.parseInput(line.strip())
            if not operation:
                continue

            self.time += 1
            print("********** Time={} **********".format(self.time))

            if isinstance(operation, BeginOp):
                if operation.transactionId in self.allTransactions:
                    print("Error in input line - {}".format(line))
                    print("Transaction name - {} already exists".format(operation.transactionId))
                    exit()
                else:
                    self.allTransactions[operation.transactionId] = ReadWriteTransaction(operation.transactionId, self.time, self.dataManagers)
            elif isinstance(operation, BeginROOp):
                if operation.transactionId in self.allTransactions:
                    print("Error in input line - {}".format(line))
                    print("Transaction name - {} already exists".format(operation.transactionId))
                    exit()
                else:
                    self.allTransactions[operation.transactionId] = ReadOnlyTransaction(operation.transactionId, self.time, self.dataManagers)
            elif isinstance(operation, ReadOp) or isinstance(operation, WriteOp):
                if operation.transactionId not in self.allTransactions or \
                self.allTransactions[operation.transactionId].status == TransactionStatus.COMPLETED:
                    print("Error in input line - {}".format(line))
                    print("Transaction - {} hasnt been begun or is unknown or is ended".format(operation.transactionId))
                    exit()
                elif self.allTransactions[operation.transactionId].status == TransactionStatus.ABORTED:
                    print("Transaction - {} has already been aborted, so this operation will not execute".format(operation.transactionId))
                else:
                    self.allTransactions[operation.transactionId].processOperation(operation)
            elif isinstance(operation, DumpOp):
                self.dump()
            elif isinstance(operation, EndOp):
                if operation.transactionId not in self.allTransactions or \
                self.allTransactions[operation.transactionId].status == TransactionStatus.COMPLETED:
                    print("Error in input line - {}".format(line))
                    print("Transaction - {} hasnt been begun or is unknown or is ended".format(operation.transactionId))
                    exit()
                elif self.allTransactions[operation.transactionId].status == TransactionStatus.ABORTED:
                    # TODO: Revisit this
                    self.allTransactions[operation.transactionId].status = TransactionStatus.COMPLETED
                else:
                    self.allTransactions[operation.transactionId].finishTransaction()
                    self.refreshTransactions()
            elif isinstance(operation, FailOp):
                if operation.site not in self.dataManagers:
                    print("Error in input line - {}".format(line))
                    print("The given site - {} is not in the range 1-20".format(operation.site))
                    exit()
                else:
                    self.dataManagers[operation.site].fail()
                    # TODO: Abort transactions by using the locks table from dm.
            elif isinstance(operation, RecoverOp):
                if operation.site not in self.dataManagers:
                    print("Error in input line - {}".format(line))
                    print("The given site - {} is not in the range 1-20".format(operation.site))
                    exit()
                else:
                    # TODO: Probably might be a good idea to refresh transactions here also.
                    # what if a trans is waiting on a non replicated items read and the dm for it just came up.
                    self.dataManagers[operation.site].recover()

            self.checkAndDealWithDeadlock()