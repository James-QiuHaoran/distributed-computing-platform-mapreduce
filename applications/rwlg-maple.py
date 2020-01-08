#!/usr/bin/env python
import sys
import re

def main():
    for line in sys.argv[1].splitlines():
        print line.split(",")[1]+","+line.split(",")[0]
    
if __name__ == "__main__":
    main()
