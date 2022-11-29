from datamanager import DataManager
from collections import OrderedDict
import re
import sys

class TransactionManager:
    def __init__(self, numOfSites, numOfRecords, fileName):
        self.numOfSites = numOfSites
        self.numOfRecords = numOfRecords
        
        if fileName:
            self.inputFile = open(fileName)
        else:
            self.inputFile = sys.stdin

        self.activeTransactions = OrderedDict()
        self.dataManagers = []
        for i in range(1, self.numOfSites + 1):
            self.dataManagers.append(DataManager(i, self.numOfRecords))

        
    def parseInput(self, line):
        line = line.strip()

        commentSplit = line.split("//")
        if commentSplit[0].strip():
            line = commentSplit[0].strip()
        else:
            return []
        
        
        def parseArgs(inp):
            inp = inp.strip("() ").split(",")
            temp = []
            for elem in inp:
                if elem.strip():
                    temp.append(elem.strip())
            return temp
        

        try:
            if not re.match("^(begin|beginRO|R|W|dump|end|fail|recover)[(]([A-Z]|[a-z]|[0-9]|,)*[)]$", line):
                raise Exception()    
            
            if re.match("^begin[(]([A-Z]|[a-z]|[0-9]|,)*[)]$", line):
                args = parseArgs(line[5:])
                if len(args) == 1:
                    return ["begin"] + args
                else:
                    raise Exception()
            elif re.match("^beginRO[(]([A-Z]|[a-z]|[0-9]|,)*[)]$", line):
                args = parseArgs(line[7:])
                if len(args) == 1:
                    return ["beginRO"] + args
                else:
                    raise Exception()
            elif re.match("^R[(]([A-Z]|[a-z]|[0-9]|,)*[)]$", line):
                args = parseArgs(line[1:])
                if len(args) == 2:
                    return ["R"] + args
                else:
                    raise Exception()
            elif re.match("^W[(]([A-Z]|[a-z]|[0-9]|,)*[)]$", line):
                args = parseArgs(line[1:])
                if len(args) == 3:
                    return ["W"] + args
                else:
                    raise Exception()
            elif re.match("^dump[(]([A-Z]|[a-z]|[0-9]|,)*[)]$", line):
                args = parseArgs(line[4:])
                if len(args) == 0:
                    return ["dump"]
                else:
                    raise Exception()
            elif re.match("^end[(]([A-Z]|[a-z]|[0-9]|,)*[)]$", line):
                args = parseArgs(line[3:])
                if len(args) == 1:
                    return ["end"] + args
                else:
                    raise Exception()
            elif re.match("^fail[(]([A-Z]|[a-z]|[0-9]|,)*[)]$", line):
                args = parseArgs(line[4:])
                if len(args) == 1:
                    return ["fail"] + args
                else:
                    raise Exception()
            elif re.match("^recover[(]([A-Z]|[a-z]|[0-9]|,)*[)]$", line):
                args = parseArgs(line[7:])
                if len(args) == 1:
                    return ["recover"] + args
                else:
                    raise Exception()
            else:
                raise Exception()
            
            
        except:
            print("The given input - {} doesnt match the input requirements.".format(line))
            exit()


    def run(self):
        for line in self.inputFile:
            if line.strip() == "quit":
                quit()
            
            parsedLine = self.parseInput(line.strip())
            if parsedLine:
                print(parsedLine)