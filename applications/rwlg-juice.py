#!/usr/bin/env python
import sys
import re

def main():
    key = sys.argv[1]
    filepath = sys.argv[2]
    value = ""
    with open(filepath) as fp:
       line = fp.readline()
       while line:
           value = value + "," + line
           line = fp.readline()
    print key+","+value[1:]

if __name__ == "__main__":
    main()
