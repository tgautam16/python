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
# FOR TERZAN 5 PULSARS
#python check_harms.py -np 38 -ps 11.56 8.43 4.71 2.19 5.54 21.67 4.92 9.57 80.33 2.96 2.24 3.56 8.66 1.67 1.72 2.81 5.02 6.11 7.08 3.28 2.07 4.20 2.99 2.04 2.46 5.78 5.11 5.08 1.39 3.65 3.30 4.44 4.96 21.22 2.95 1.89 5.95 2.93 -candp
