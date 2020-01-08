#!/usr/bin/env python
import sys
import re

def main():
    for word in sys.argv[1].split():
        print word+",1"
    
if __name__ == "__main__":
    main()
