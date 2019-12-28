#!/usr/bin/env python3

#
# Given two files of old bdb output and new bdb output,
# print the directories where the size has grown.
#

import sys

def into_dict(file_name):
    return { ' '.join(s[:-1]):float(s[-1])
             for s in 
             [ s.split() for s in open(file_name).readlines() ] }

if len(sys.argv) != 3:
    sys.stderr.write("expected two file arguments: old new\n")
    sys.exit(1)

older, newer = [ into_dict(file_name) for file_name in sys.argv[-2:]]

for (dir,size) in newer.items():
    growth = size - older.get(dir,0)
    if growth > 0:
        print('{} {:.2}'.format(dir, growth))





