from collections import OrderedDict, defaultdict
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
        self.operations = []
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

    def fail(self, dataManagerId):
        print("Site-{} fails".format(dataManagerId))
        self.dataManagers[dataManagerId].fail(self.time)
        for transaction in self.allTransactions.values():
            if transaction.status == TransactionStatus.ALIVE and \
                isinstance(transaction, ReadWriteTransaction) and \
                dataManagerId in transaction.dataManagersTouched:
                transaction.status = TransactionStatus.ABORTED
                # print("Transaction {} will abort because it had touched site {}".format(transaction.transactionId, dataManagerId))


    def recover(self, dataManagerId):
        print("Site-{} recovers".format(dataManagerId))
        self.dataManagers[dataManagerId].recover()


    def dump(self):
        for dataManager in self.dataManagers.values():
            dataManager.dump()

    def cycleDetected(self, node, visited, root, graph):
        visited.add(node)
        for neighbour in graph[node]:
            if neighbour == root:
                return True
            if neighbour not in visited:
                if self.cycleDetected(neighbour, visited, root, graph):
                    return True
        return False

    def checkAndDealWithDeadlock(self):
        blockingRelations = set()
        for dataManager in self.dataManagers.values():
            blockingRelations.update(dataManager.getBlockingRelations())
        graph = defaultdict(set)
        for node, neighbour in blockingRelations:
            graph[node].add(neighbour)
        youngestTransTS = float("-inf")
        youngestTrans = None
        for trans in list(graph.keys()):
            if self.cycleDetected(trans, set(), trans, graph) and self.allTransactions[trans].startTime > youngestTransTS:
                youngestTransTS = self.allTransactions[trans].startTime
                youngestTrans = self.allTransactions[trans]
        
        if youngestTrans:
            print("Deadlock Detected")
            youngestTrans.abortDeadlockedTransaction()
            return True
        return False



    def refreshOperations(self):
        # TODO: Check very carefully if the order in which we refresh transactions makes any diff.
        # because allTransactions just stores the transactions in the order in which the trans came.
        # but operations themselves can happen in any way.
        
        for operation in self.operations:
            if isinstance(operation, (ReadOp, WriteOp, EndOp)) and operation.status == OperationStatus.IN_PROGRESS:
                self.allTransactions[operation.transactionId].processOperation(operation)


    def run(self):
        for line in self.inputFile:
            if line.strip().lower() == "quit":
                quit()
            
            operation = self.parseInput(line.strip())
            if not operation:
                continue

            self.time += 1
            print("********** Time={} **********".format(self.time))
            print(operation)
            if self.checkAndDealWithDeadlock():
                self.refreshOperations()

            self.operations.append(operation)

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
            elif isinstance(operation, ReadOp) or isinstance(operation, WriteOp) or isinstance(operation, EndOp):
                if operation.transactionId not in self.allTransactions or \
                self.allTransactions[operation.transactionId].status == TransactionStatus.COMPLETED:
                    print(self.allTransactions[operation.transactionId].status)
                    print("Error in input line - {}".format(line))
                    print("Transaction - {} hasnt been begun or is unknown or is ended".format(operation.transactionId))
                    exit()
                else:
                    if isinstance(operation, EndOp):
                        operation.commitTime = self.time
                    self.allTransactions[operation.transactionId].processOperation(operation)
            elif isinstance(operation, DumpOp):
                self.dump()
            elif isinstance(operation, FailOp):
                if operation.site not in self.dataManagers:
                    print("Error in input line - {}".format(line))
                    print("The given site - {} is not in the range 1-20".format(operation.site))
                    exit()
                else:
                    self.fail(operation.site)
            elif isinstance(operation, RecoverOp):
                if operation.site not in self.dataManagers:
                    print("Error in input line - {}".format(line))
                    print("The given site - {} is not in the range 1-20".format(operation.site))
                    exit()
                else:
                    # TODO: Probably might be a good idea to refresh transactions here also.
                    # what if a trans is waiting on a non replicated items read and the dm for it just came up.
                    self.recover(operation.site)
            
            self.refreshOperations()

            # One huge bug is:
            # I say a record is recovered when a trans writes to it. But it should recover when it commits to it.
            # But if i recover it at commit time then another read operation happening in the same trans wont be 
            # able to read it, because the record would not have recovered.
            # But if i leave it at write time then if the transaction aborts then i will leave the record as recovered even when its not.
            # Make the change in isReadOKForRWTrans