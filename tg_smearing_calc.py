#### !/usr/bin/env python

#### import numpy as np
### This code calculates the smearing timescale of coherently de-dispersed or a PA data.

import math
import os, os.path, sys, glob, time, shutil, datetime

def tdelay_ideal(dm,fl,fh):
    delay_ms = 4.15e6*dm*((1/(fl**2))-(1/(fh**2)))
    return delay_ms

def tdelay_PA(dm,df,fc):
    delay_ms = 8.30e6*(df)*dm/(fc)**3
    return delay_ms

for j in range(1, len(sys.argv)):
    if(sys.argv[j] == "-fl"):
        fl = float(sys.argv[j+1])
    elif(sys.argv[j] == "-fh"):
        fh = float(sys.argv[j+1])
    elif(sys.argv[j] == "-fc"):
        fc = float(sys.argv[j+1])
    elif(sys.argv[j] == "-dm"):
        dm = float(sys.argv[j+1])
    elif(sys.argv[j] == "-df"):
        df = float(sys.argv[j+1])
    elif(sys.argv[j] =="-p"):
        p_s = float(sys.argv[j+1])
    elif(sys.argv[j] =="-h" or "--help"):
        print "This code calculates the smearing timescale of coherently de-dispersed or a PA data."
        print "Usage: python -fl [frequency of lower channel(MHz)] -fh [frequency of upper channel(MHz)] -dm DM(pc cm-3) -df [channel bandwidth(MHz)] -fc [central frequency(MHz)] -p [rough period in seconds(default: 10ms)]"

print("starting to calc`")
tdelay_ideal = tdelay_ideal(dm,fl,fh)
tdelay_PA = tdelay_PA(dm,df,fc)
print "Smearling time accross the low and high frequency channel is: %f ms" %tdelay_ideal
print "Smearing time accross a single channel is: %f ms" %tdelay_PA

p_ms = p_s / 1000
if(p_ms > tdelay_PA):
    print "Acceptable, Smearing time is less than spin period"
else:
    print "Not Acceptable, smearing time is more than spin period, the candidate is definitely noise"


