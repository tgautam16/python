#!/usr/bin/env python

import numpy as np
import matplotlib.pyplot as plt
#import astropy.units as u
#from astropy.time import Time
#from pulsar.predictor import Polyco
import argparse
import sys
print("LIBRARIES IMPORTED")

#data_dir = '/beegfsBN/u/tasha/alex_pipeline/uGmrt2fil/filterbanks/NGC6440/'
#fil_name = '01_NGC6440_cdp_02sep2018_bs5_80us.fil'
#fn = data_dir+fil_name
#file = open(fn,"rb")

for j in range(1, len(sys.argv)):
    if(sys.argv[j] == "-hdrsize"):
        hdrsize = np.int(sys.argv[j+1])
    elif(sys.argv[j] == "-nchan"):
        nchan = np.int(sys.argv[j+1])
    elif(sys.argv[j] == "-datasize"):
        datasize = np.int(sys.argv[j+1])
    elif(sys.argv[j] == "-fil"):
        fn = sys.argv[j+1]
    elif(sys.argv[j] == "-h"):
        print("python x -hdrsize -nchan -datasize -fil")
        exit()
with open(fn) as f:
        f.seek(0,2)
        totalbyte = f.tell()
        databyte = totalbyte - hdrsize
        f.seek(hdrsize,0)
        print("datasize: ",datasize)
        #block_dat = np.fromfile(f,dtype="uint8",count=databyte)
        block_dat = np.fromfile(f,dtype="uint8",count=datasize)
        n_timesamples = datasize/nchan
        #n_timesamples = 91529344
        print("number of timesamples: ", n_timesamples)
        block_dat = block_dat.reshape(n_timesamples, nchan)
	#block_dat_2 = block_dat.reshape()
        #print block_dat
        #print((block_dat[0,:]))
        #print((block_dat[9999,:]))
        #print(len(block_dat[0,:]))
        #print(len(block_dat[9999,:]))
        #plt.ion()
        #plt.clf()
        plt.figure(figsize=(12,12))
        plt.figure()
        plt.plot(block_dat.sum(axis=0))
        plt.show()
#nsamples = np.int(fn.tell() / (bytes_per_sample * nchan))
#skipping header
#file.seek(396,0)

print("Totalbytes in file: ", totalbyte)
#hdrbyte = file.tell()
#print("Total header bytes in file: ", hdrbyte)
#file.read()            
#file.seek(0,2)
#totalbyte = file.tell()
#databyte = totalbyte - hdrbyte
#print("Total data bytes without header in file: ", databyte)

#file.seek(396,0)
#nchan = 512
#n_timesamples = databyte/nchan
#print("number of timesamples: ", n_timesamples)
#block_dat = f.reshape(n_timesamples, nchan)

