#!/usr/bin/env python
#################### ALESSANDRO RIDOLFI ########################

import sys, os, os.path, copy, time
import multiprocessing, subprocess
import numpy as np
import scipy, scipy.optimize

import sigpyproc.Readers
########################################################################################################



string_version = "0.1 (25Apr2019)"
time_factor = 1
freq_factor = 1
outfilename = ""
if  (len(sys.argv) == 1 or ("-h" in sys.argv) or ("-help" in sys.argv) or ("--help" in sys.argv)):
	print "Usage: %s -fil <obs.fil> -time_factor N -freq_factor N -o <oufilename>" % (os.path.basename(sys.argv[0]))
	print 
        exit()
elif (("-version" in sys.argv) or ("--version" in sys.argv)):
        print "Version: %s" % (string_version)
        exit()
else:
	for j in range( 1, len(sys.argv)):
		if  (sys.argv[j] == "-fil"):
			file_name = sys.argv[j+1]
                elif  (sys.argv[j] == "-time_factor"):
                        time_factor = int(sys.argv[j+1])
                elif  (sys.argv[j] == "-freq_factor"):
                        freq_factor = int(sys.argv[j+1])
                elif  (sys.argv[j] == "-o"):
                        outfilename = sys.argv[j+1]

print
print "#"*62
print "#" + " "*23 + "%s" % ("sigpy_decimate") + " "*23 + "#"
print "#" + " "*23 + "%s" % (string_version) + " "*22 + "#"
print "#"*62


if outfilename=="":
        outfilename = os.path.splitext(file_name)[0] + "_t%d_f%d.fil" % (time_factor, freq_factor)


filfile = sigpyproc.Readers.FilReader(file_name)
filfile.downsample(tfactor=time_factor, ffactor=freq_factor, filename=outfilename) 
