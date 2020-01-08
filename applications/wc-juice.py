#!/usr/bin/env python
import sys
import re

def main():
    key = sys.argv[1]
    total = 0
    filepath = sys.argv[2]
    with open(filepath) as fp:
       line = fp.readline()
       while line:
           total = total + int(line.strip())
           line = fp.readline()
    print key+","+str(total)

if __name__ == "__main__":
    main()
