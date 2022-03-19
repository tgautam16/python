#!/usr/bin/env python
import os
import numpy as np
import matplotlib.pyplot as plt
import astropy.units as u
from astropy.time import Time
from time import strftime
#from pulsar.predictor import Polyco
import argparse
"""
Example call:  
run fold_1744.py -fn /beegfsBN/u/tasha/GMRT_GC/TER5A_project/01_Terzan5_cdp_02aug2018.gmrt_dat -t0 2018-08-02T17:02:21.573191900 --polyco polyco_new.dat --dm 242.15 --P0 0.01156314838986 --fbin 16 -nc 1024 -ng 128
"""
#Things to do : create loops for creating various chunks and their respective header files


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='segment GMRT data and create respective headers')
    parser.add_argument("-fn", "--filename")
    parser.add_argument("-t0","--tstart", nargs='?')
    parser.add_argument("-nchunks","--nchunk", default=4, type=int)
    parser.add_argument("-nc", "--nchan", nargs='?', default=2048, type=int)
   # parser.add_argument("-ng", "--ngate", nargs='?', default=16, type=int)
   # parser.add_argument("--fbin", nargs='?', default=16, type=int)
   # parser.add_argument("--f0", nargs='?', default=550, type=float)
   # parser.add_argument("--bw", nargs='?', default=200, type=float)
    parser.add_argument("-o", "--chunkfile")
    a = parser.parse_args()
    
    """ Load the file as a memmap, in correct shape (time, frequency)"""
    
    ### Take the input file as an input
    fn = a.filename
    suffix = '.gmrt_dat'
    suffix_hdr = '.gmrt_hdr'
    fchunk = a.chunkfile
    nchunk = a.nchunk
   # polyco = a.polyco
    nchan = a.nchan
   # ngate = a.ngate
   # f0 = a.f0
   # bw = a.bw
   # chanbin = a.fbin
    #T_temp = a.tstart
    print(a.tstart)
    T0 = Time(a.tstart, precision=9)
    
    # These values can be different in each observation, check the header
    # (or make code smarter, to read these values out)
    bytes_per_sample = 2
    ### tsamp should really be an input - perhaps .conf
    tsamp = 20.48*u.microsecond
    print(fn+suffix) 
    # Open file, record total number of samples in file
    # Each 'sample' contains an intensity value in every channel
    fh = open(fn+suffix, "r")
   
    # .seek() takes us to the end of file
    fh.seek(0,2)
    # .tell() gives the number of bytes(total) till the current position
    nsamples = np.int(fh.tell() / (bytes_per_sample * nchan))
    #Total time of observation(file)
    Tfile = nsamples*tsamp
    print("Total time in the file is {0}".format(Tfile.to(u.s)))
    
    # Return again to start of file
    # Load full data as memmap, access small sections as needed
    # Each value contains the total number of counts in the correlator, hover around ~2000
    # dtype = data type used to interpret the file contents
    # r= open file for reading only
    # offset = 0(i.e. 0 shift starting from start of file (always start from start in memmap))
    # shape of array created should be a multiple of dtype
    buff = np.memmap(fn+suffix, dtype=np.uint16, mode='r', offset=0, shape=(nsamples, nchan)) 
    
    
    """ Set up folding pars - single pulses
    T0:                  the starting point of the file, as astropy time (get from header)
    nbin, ngate:         number of time, phase bins respectively
    chanbin:             binning factor for frequencies
    P0:                  total amount of time you want to fold, as an astropy unit
    Tstart:              how far into the file you want to start, as an astropy unit
    size:                the number of samples you add iteratively in each step
    
    """
    
    #size = 400000
    #step = size-100000
	
	# integer value of nchan(actual)/chanbin(reduced chans)
    #foldchan = nchan // chanbin #factor by which channels are reduced
   
    P0 = Tfile
    T_chunk = (P0/nchunk)
    print('number of chunks produced and Time in first chunk will be: %s, %s ' % (nchunk, format(T_chunk.to(u.s))))
    for j in range(1, nchunk):
        T_temp = a.tstart
	Tstart = T0+((j-1)*T_chunk)
        #print('structure %s' %( Tstart.strptime
        print('Tstart of chunk number %s is %s ' % (j, Tstart))
        nstep = int(T_chunk/(tsamp).decompose())
        print('number of samples in the chunk is %s' %nstep)
        Tend = T0+((j)*T_chunk)
        #Take care that it should only go till the end of file.
        fhdr = open(fn+suffix_hdr, "r")        
        fk = open(str(j)+"_"+fchunk+suffix,"wb")
        outhdr = open(str(j)+"_"+fchunk+suffix_hdr,"w")
        print('T_end of chunk number %s is %s ' % (j, Tend))
        lines = fhdr.readlines()
        utc = 'UTC'
        mjd = 'MJD'
        for i in lines:
            if utc in i:
                print i
                #DD,MM,YY = [int(x) for x in Tstart.split(":")]
                Tstart_utc = str(Tstart.utc) 
                tstartsplit = Tstart_utc.split('2018')[-1].strip().split('T')
                print tstartsplit[1]
                #utcsplit = i.split('UTC')[-1].strip().split(':')
                outhdr.write("UTC             : "+tstartsplit[1]+"\n")
                #print utcsplit
            elif mjd in i:
                print i
                Tstart_mjd = "%.11f" % (Tstart.mjd)
                print Tstart_mjd
	        #mjdsplit = i.split('MJD')[-1].strip().split(':')
                outhdr.write("MJD             : "+Tstart_mjd+"\n")
                #print mjdsplit
            else:
                outhdr.write(i)
        outhdr.close()
	fhdr.close()
        for i in range(nstep):
            power = np.copy(buff[i])
            fk.write(power)
        #print('shape of power is : %s' % (power.shape()))        
        fk.close()
        del(power)
        
    #print('Total time in file is: %s and start time is %s : '%s (P0,Tstart))
    #T_chunk = P0 / nchunk
    #print('T_chunk is %s' % (T_chunk))
    #This relation is for incoherent dm(not including the intrachannel smearing)
    
    # compute starting time, number of steps from given pars
    # Floor to avoid rolling over
    # istart = int( (Tstart / tsamp).decompose() )
    #nstep = int( np.floor( (T_chunk / (step*tsamp)).decompose() ) )
    #nstep = int(T_chunk/(tsamp).decompose())
    #print('number of samples is %s' % (nstep))
    #first = True
    #power = np.copy(buff[i])
    # Loop to read, fold chunks of data (start with 1)
    #for i in range(nstep):
    #    print('step %s of %s' % (i, nstep))
    #    print('Reading...')
        # Number of samples to seek into the file
        #nseek = step*i #istart+step*i
        
    #    t0 = T0 + (i*tsamp)
     #   print('Time = {0}'.format(t0.isot))
        #phase_pol = psr_polyco.phasepol(t0)
    
        # Read in data, with extra buffer for dispersion wrapping
    #    power = np.copy(buff[i])
       
        ## Remove incoherent dedispersion, chop off edge
       # print("Dedispersing...")
       # for nc in np.arange(nchan):
        #    pshift = int( (t_delay[nc]/tsamp).decompose().value )
         #   power[:,nc] = np.roll(power[:,nc], pshift, axis=0)
        #power = power[:step]
        
        # Determining phases from times, polyco
       # tsr = t0 + tsamp * np.arange(power.shape[0])
       # phase = phase_pol(tsr.mjd)
        #taking the phase back to within 0 to 1
       # phase -= np.floor(phase[0])
        # ncycle encodes which rotation you are in
       # ncycle = int(np.floor(phase[-1])) + 1
        

	# making the power array in 3-D with first a
        # Bin in frequency
        #power = power.reshape(power.shape[0], power.shape[-1]//chanbin, chanbin).mean(-1)
        
       # profile = np.zeros((ncycle, ngate, foldchan), dtype=np.float32)
        #iphase = np.remainder(phase*ngate, ngate*ncycle).astype(np.int)
    
       # for j in range(foldchan):
        #    profile[..., j] = np.bincount(iphase, power[..., j], minlength=ngate*ncycle).reshape(ncycle, ngate);
        #icount = np.bincount(iphase, minlength=ngate*ncycle).reshape(ncycle, ngate)
    
       # if first:
       #     profiles = profile; icounts = icount
        #    first = not first;
       # else:
        #    if icounts[-1,-1] > 0 and icount[0,0] > 0:
         #       profiles = np.append(profiles, profile, axis=0)
          #      icounts = np.append(icounts, icount, axis=0)
           # else:
            #    profiles[-1] += profile[0]; icounts[-1] += icount[0]
             #   profiles = np.append(profiles, profile[1:], axis=0)
              #  icounts = np.append(icounts, icount[1:], axis=0)
                
    #icounts[icounts==0] = 1
    #profnorm = profiles / icounts[...,np.newaxis]
    #print('shape of power is : %s' %s (power.shape())    
   # fh_chunk.write(power)
   # fh_chunk.close()  
   # np.save('Fold_{0}_{1}sec_nc{2}_ng{3}.npy'.format(T0, Tfile.to(u.s).value, foldchan, ngate), profnorm)
