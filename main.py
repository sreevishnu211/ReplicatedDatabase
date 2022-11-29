import argparse
from transactionManager import TransactionManager

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RepCRec - A Replicated & Concurrent Database.")
    parser.add_argument("inputFileName", nargs="?", default=None, help="A text file or leave empty to read from stdin.")
    arguments = parser.parse_args()
    
    transManager = TransactionManager(10, 20, arguments.inputFileName)
    transManager.run()