#!/usr/bin/python

import os,sys,math
import argparse
import numpy as np
from scipy.constants import pi,c

f_calc=650
#from math import pi
for i in range(1,len(sys.argv)):
    if(sys.argv[i]=='-alpha'):
        alpha = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-f1'):
        f1 = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-f2'):
        f2 = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-f_calc'):
        f_calc = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-S_f1'):
        S_f1 = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-S_f2'):
        S_f2 = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-h'):
        print("USAGE: python(3.6) flux_calc.py -alpha(-1.4) -f1 (MHz) -S_f1 (flux at f1) -f2 (MHz) -f_calc (MHz, freq to calculate)")
        exit()



def S_4rm_alpha(S_f1,f1,alpha,f_calc=650):
    S=S_f1*(f_calc/f1)**(alpha)
    return S

def alpha_4rm_freq(S_f1,S_f2,f1,f2):
    alpha=np.log(S_f1/S_f2)/np.log(f1/f2)
    return alpha



S=S_4rm_alpha(S_f1,f1,alpha,f_calc)
print("Flux calculated from spectral index is: ")
print(S)
#alpha = alpha_4rm_freq(S_f1, S_f2,f1,f2)
#print("Spectral index calculated:")
#print(alpha)

