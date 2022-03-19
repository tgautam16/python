#This will fold 2,3,0.5 harmonic of a particular candidate, taking input as datafile(with directory)


import numpy as np
import matplotlib.pyplot as plt
import argparse
import os,sys, subprocess
p=[]
for j in range(1, len(sys.argv)):
        if(sys.argv[j] == '-np'):
		np = int(sys.argv[j+1])
		print("number of periods known", np)
	if(sys.argv[j] == '-ps'):			
                for i in range(0,np):
			p.append(float(sys.argv[j+i+1]))
	if(sys.argv[j] == '-candp'):
		candp = float(sys.argv[j+1])
        elif(sys.argv[j] == '-h'):
                print("python x -np number of periods -ps p1 p2 .. -candp period of candidate ")
                exit()
print(p)
for i in range(0,len(p)):
	print(p[i])
	mult1 = candp/p[i]
	mult2 = p[i]/candp
	print("Multiples include", mult1, mult2)
	#print("mult2", mult2)

#num=100
#input
#ts_basename = 'ts_fold_1123_tdec4_full_ck00_DM229.98_z20_ACCEL_Cand_26'
#input
#obsfile = '/beegfsBN/u/tasha/NGC6440_meerkat/1123.5/decimated_rfifind/1123_td

