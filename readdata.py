#!/usr/bin/env python

import numpy as np
import matplotlib.pyplot as plt
#import astropy.units as u
#from astropy.time import Time
#from pulsar.predictor import Polyco
import argparse

print("LIBRARIES IMPORTED")

data_dir = '/u/tasha/FRB_dongzi/data/' #'/hercules/results/tilemath/INTERESTING/J0053+6730WS/'
fil_name = '0158+65_bm3_pa_550_200_32_24mar2020.raw.1.tBin3_fBin16_subIA.fil'#'J0053+6730WS_00_8bit.fil'
fn = data_dir+fil_name
#file = open(fn,"rb")
with open(fn) as f:
	f.seek(0,2)
	totalbyte = f.tell()
	databyte = totalbyte - 396
	f.seek(396,0)
	#block_dat = np.fromfile(f,dtype="uint8",count=databyte)
	block_dat = np.fromfile(f,dtype="uint8",count=65918033*512)
	nchan = 512
	#n_timesamples = databyte/nchan
	n_timesamples = 65918033
	print("number of timesamples: ", n_timesamples)
	block_dat = block_dat.reshape(n_timesamples, nchan)
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
#file.close()
