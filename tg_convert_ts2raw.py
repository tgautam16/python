#!/usr/bin/env python

#This script will take any ts file name as per the alex pipeline and fold that candidate for raw data (i.e. with frequency information)

#IMPORTANT : ONLY TO FOLD FULL FILE
#prepfold -ncpus 3 -npart 128 -n 128 -noxwin -accelcand 1 -accelfile /beegfsBN/u/tasha/NGC6440_meerkat/1123.5/decimated_rfifind/03_DEDISPERSION/1123_tdec4/full/ck00/1123_tdec4_full_ck00_DM227.67_ACCEL_20_JERK_60.cand -dm 227.67 -mask /beegfsBN/u/tasha/NGC6440_meerkat/1123.5/decimated_rfifind/01_RFIFIND/1123_tdec4_rfifind.mask -o raw_fold_1123_tdec4_full_ck00_DM227.67_z20_w60    /beegfsBN/u/tasha/NGC6440_meerkat/1123.5/decimated_rfifind/1123_tdec4.fil


#ts_fold_1123_tdec4_full_ck00_DM229.98_z20_w60_JERK_Cand_26

import numpy as np
import matplotlib.pyplot as plt
import argparse
import os,sys, subprocess

for j in range(1, len(sys.argv)):
	if(sys.argv[j] == '-num'):
		num = int(sys.argv[j+1])
	elif(sys.argv[j] == '-obs'):
		obsfile = sys.argv[j+1]
	elif(sys.argv[j] == "-list"):
		list_file = sys.argv[j+1]
        elif(sys.argv[j] == '-h'):
                print("python ts2raw_jerk.py -num number_of_cand_in_ts_pdf_file -list file_with_bestprof_list -obs /dir/datafile.fil")
		exit()
#input
#ts_basename = 'ts_fold_1123_tdec4_full_ck00_DM229.98_z20_w60_JERK_Cand_26'
#input
#obsfile = '/beegfsBN/u/tasha/NGC6440_meerkat/1123.5/decimated_rfifind/1123_tdec4.fil'
print(num)
with open (list_file,'r') as tsfile:
    for line_no, line in enumerate(tsfile, start=1):
        #print(line_no)
        if(line_no == num):
            print("inside")
            ts_file=line # The content of the line is in variable 'line'
tsfile.close()

#ts_location,ts_basename = os.path.split(ts_file)
#print("ts_location", ts_location)
#print("ts_basename", ts_basename)
ts_basename = ts_file.split('.pfd.bestprof')[0]

obs = obsfile.split('/')[-1].split('.fil')[0]
print(obs)
val = ts_basename.split(obs+'_')
print(val)
seg = val[1].split('_')[0]
ck = val[1].split('_')[1]
infile = ts_basename.split("ts_fold_")[-1]
accel1 = infile.split("_z")
z = accel1[-1].split("_")[0]
w=accel1[-1].split("_")[1].split("w")[-1]
print("z,w is: ", z,w)
if w != '0':
	print("inside not zero w loop")
	accelcand = accel1[0]+'_ACCEL_'+z+'_JERK_'+w+'.cand'
else: 
	accelcand = accel1[0]+'_ACCEL_'+z+'.cand'

candnum = ts_basename.split("_")[-1]
dm = accel1[0].split("DM")[-1]


if 'JERK' in infile:
	ofile = infile.split("_JERK")[0]
elif 'ACCEL' in infile:
	ofile = infile.split("_ACCEL")[0]

mask = obs+'_rfifind.mask'
print("obs,seg,ck,z,w,dm,accelcand,candnum")

print(obs)
print(seg)
print(ck)
print(z)
print(w)
print(dm)
print(accelcand)
print(candnum)
print(obsfile)
print(os.getcwd())
if seg != 'full':
	print "SEGMENT IS NOT FULL"
	hdr_cmd = "header %s" %obsfile
	#os.popen(hdr_cmd).read()
	for x in os.popen(hdr_cmd).readlines():
		if 'Number of samples' in x:
			numsamples = np.int(''.join(c for c in x if c in '0123456789'))
			print(numsamples)
		if 'Sample time (us)' in x:
                        tsample = np.float(''.join(c for c in x if c in '0123456789.'))		
			print(tsample)
	obslen=numsamples*tsample*1e-6
	seg_m = np.float(seg.split("m")[0])
	frac = (seg_m*60)/obslen
	ck_len = np.int(ck.split("ck")[1])
	print("seg_m", seg_m)
	print("frac is", frac)
	print("ck_len ", ck_len)
	frac_end = (ck_len+1)*frac	
	frac_start = frac_end - frac
	print("frac_end",frac_end)
	print("frac_start",frac_start)

ACCELFILE = os.getcwd()+'/03_DEDISPERSION/'+obs+'/'+seg+'/'+ck+'/'+accelcand
MASK = os.getcwd()+'/01_RFIFIND/'+mask
ACCELFILE=ACCELFILE.replace('/hercules/u/tasha/','/home1/')
MASK=MASK.replace('/hercules/u/tasha/','/home1/')
OUTPUTFILE=os.getcwd()+'/ts2raw/'
OUTPUTFILE=OUTPUTFILE.replace('/hercules/u/tasha/','/home1/')
obsfile=obsfile.replace('/u/tasha/','/home1/')
print("accelfile", ACCELFILE)
print("MASK", MASK)
try:
	os.mkdir('ts2raw')
except OSError as error:	
	print("file exists")



if seg != 'full':
        print "SEGMENT IS NOT FULL"
        hdr_cmd = "header %s" %obsfile
        #os.popen(hdr_cmd).read()
        for x in os.popen(hdr_cmd).readlines():
                if 'Number of samples' in x:
                        numsamples = np.int(''.join(c for c in x if c in '0123456789'))
                        print(numsamples)
                if 'Sample time (us)' in x:
                        tsample = np.float(''.join(c for c in x if c in '0123456789.'))
                        print(tsample)
        obslen=numsamples*tsample*1e-6
        seg_m = np.float(seg.split("m")[0])
        frac = (seg_m*60)/obslen
        ck_len = np.int(ck.split("ck")[1])
        print("seg_m", seg_m)
        print("frac is", frac)
        print("ck_len ", ck_len)
        frac_end = (ck_len+1)*frac
        frac_start = frac_end - frac
        print("frac_end",frac_end)
        print("frac_start",frac_start)


	print('sing_presto prepfold -ncpus 5 -npart 128 -n 128 -noxwin -start %.2f -end %.2f -accelcand %s -accelfile %s -dm %s -mask %s -o %s/raw_fold_%s %s' % (frac_start, frac_end, candnum, ACCELFILE, dm, MASK, OUTPUTFILE, ofile, obsfile))




	cmd = "singularity exec -H $HOME:/home1 -B /u/tasha/:/home/psr/work /u/tasha/docker_sing_imgs/prestonew5-2019-09-30-5cd93e04bd30.simg prepfold -ncpus 3 -npart 128 -noxwin -start %.2f -end %.2f -accelcand %s -accelfile %s -dm %s -mask %s -o %s/raw_fold_%s %s" % (frac_start, frac_end, candnum, ACCELFILE, dm, MASK, OUTPUTFILE, ofile, obsfile)

	os.system(cmd)

else:
	print('sing_presto prepfold -ncpus 5 -npart 128 -n 128 -noxwin -accelcand %s -accelfile %s -dm %s -mask %s -o %s/raw_fold_%s %s' % (candnum, ACCELFILE, dm, MASK, OUTPUTFILE, ofile, obsfile))




        cmd = "singularity exec -H $HOME:/home1 -B /u/tasha/:/home/psr/work /u/tasha/docker_sing_imgs/prestonew5-2019-09-30-5cd93e04bd30.simg prepfold -ncpus 3 -npart 128 -noxwin -accelcand %s -accelfile %s -dm %s -mask %s -o %s/raw_fold_%s %s" % (candnum, ACCELFILE, dm, MASK, OUTPUTFILE, ofile, obsfile)

        os.system(cmd)

#cmd = "prepfold -ncpus 3 -npart 128 -n 128 -noxwin -nsub 128 -accelcand %s -accelfile 03_DEDISPERSION/%s/%s/%s/%s -dm %s -mask 01_RFIFIND/%s -o ts2raw/raw_fold_%s %s" % (candnum, obs, seg, ck, accelcand, dm, mask, ofile, obsfile)

#executable = cmd.split()[0]
#list_for_Popen = cmd.split()
#proc = subprocess.Popen(list_for_Popen)
#proc.communicate()
