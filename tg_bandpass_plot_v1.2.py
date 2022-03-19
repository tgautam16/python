#!/usr/bin/env python2.7

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
        hdrsize = int(sys.argv[j+1])
    elif(sys.argv[j] == "-nchan"):
        nchan = int(sys.argv[j+1])
    elif(sys.argv[j] == "-datasize"):
        datasize = int(sys.argv[j+1])
    elif(sys.argv[j] == "-fil"):
	fn = sys.argv[j+1]
    elif(sys.argv[j] == "-block_size"):
        data_bytes_block = float(sys.argv[j+1])
    elif(sys.argv[j] == "-tsample"):
        t_sample = float(sys.argv[j+1])
    elif(sys.argv[j] == "-format"):
        file_format = str(sys.argv[j+1])
    elif(sys.argv[j] == "-h"):
        print("python read_data.py -hdrsize headersize(if filterbank) -nchan numofchannels -datasize datasize(if filterbank) -block_size block_size -fil filename -format gmrt/filterbank")
        exit()
with open(fn) as f:
        f.seek(0,2)
        totalbyte = f.tell()
        print("totalbyte",totalbyte)
        if(file_format == "filterbank"):
            databyte = totalbyte - hdrsize
            f.seek(hdrsize,0)
            print("data_bytes_block: ",data_bytes_block)
            time_bytes_block = int(data_bytes_block/nchan)
            block_dat = np.fromfile(f,dtype="uint8",count=time_bytes_block*nchan)
        elif(file_format == "gmrt"):
            f.seek(0,0)
            databyte = totalbyte
            time_bytes_block = int(data_bytes_block/nchan)
            block_dat = np.fromfile(f,dtype="uint16",count=time_bytes_block*nchan)
        block_dat = block_dat.reshape(time_bytes_block, nchan)
        plt.figure()
        plt.xlabel('Frequency (number of channels)')
        plt.ylabel('Power')
        plt.title("Bandpass with %d blocks"%data_bytes_block)
        plt.plot(block_dat.sum(axis=0))
        plt.show()

print("Totalbytes in file: ", totalbyte)
