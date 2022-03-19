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
        hdrsize = int(sys.argv[j+1])
    elif(sys.argv[j] == "-nchan"):
        nchan = int(sys.argv[j+1])
    elif(sys.argv[j] == "-datasize"):
        datasize = int(sys.argv[j+1])
    elif(sys.argv[j] == "-fil"):
	fn = sys.argv[j+1]
    elif(sys.argv[j] == "-N_blocks"):
        data_bytes_block = float(sys.argv[j+1])
    elif(sys.argv[j] == "-tsample"):
        t_sample = float(sys.argv[j+1])
    elif(sys.argv[j] == "-h"):
        print("python read_data.py -hdrsize headersize -nchan numofchannels -datasize datasize -tsample sampling time -N_blocks block_size -fil filterbankfilename")
        exit()
with open(fn) as f:
        f.seek(0,2)
        totalbyte = f.tell()
        print(totalbyte)
        databyte = totalbyte - hdrsize
        f.seek(hdrsize,0)
        #data_bytes_block = int(nchan*t_sec/(t_sample))
        print("data_bytes_block: ",data_bytes_block)
        time_bytes_block = int(data_bytes_block/nchan)
        #t_sec_new = data_bytes_block*t_sample/nchan
        
        block_dat = np.fromfile(f,dtype="uint8",count=time_bytes_block*nchan)
        #block_dat = np.fromfile(f,dtype="uint8",count=datasize)
        #n_timesamples = int(data_bytes_block/nchan)
        #n_timesamples = 91529344
        #print("number of timesamples: ", n_timesamples)
        block_dat = block_dat.reshape(time_bytes_block, nchan)
	#block_dat_2 = block_dat.reshape()
        #print block_dat
        #print((block_dat[0,:]))
        #print((block_dat[9999,:]))
        #print(len(block_dat[0,:]))
        #print(len(block_dat[9999,:]))
        #plt.ion()
        #plt.clf()
        plt.figure()
        plt.xlabel('Frequency (number of channels)')
        plt.ylabel('Power')
        plt.title("Bandpass with %d blocks"%data_bytes_block)
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

