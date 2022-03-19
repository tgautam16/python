#!/usr/bin/python

import os,sys,math
import argparse
import numpy as np
from scipy.constants import pi,c

#from math import pi
a=3
b=4

for i in range(1,len(sys.argv)):
    if(sys.argv[i]=='-pb'):
        pb_days = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-Mtot'):
        Mtot = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-e'):
        e = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-h'):
        print("USAGE: python(3.6) omdot.py -pb(days) -Mtot(solar mass) -e eccentricity ")
        exit()

pb_s=pb_days*(24*60*60)

T0=4.925490947e-6

omdot_radpersec=3*T0**(2./3)*(pb_s/(2*pi))**(-5./3)*(1/(1-e**2))*Mtot**(2./3)

omdot_degpersec = omdot_radpersec*360/(2*pi) 

omdot_degperyear=omdot_degpersec*(365*24*60*60)

print("omdot_degperyear")
print(omdot_degperyear)
