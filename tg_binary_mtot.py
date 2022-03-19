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
    elif(sys.argv[i]=='-omdot'):
        omdot_degperyear = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-e'):
        e = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-h'):
        print("USAGE: python(3.6) Mtot.py -pb(days) -omdot(deg per year) -e eccentricity ")
        exit()

pb_s=pb_days*(24*60*60)

T0=4.925490947e-6

omdot_radperyear = omdot_degperyear*2*pi/360

omdot_radpersec=omdot_radperyear/(365.2422*24*60*60)

Mtot= (omdot_radpersec*(1-(e*e))/((pb_s/(2*pi))**(-5./3)*(3*T0**(2./3))))**(3./2)

print(Mtot)
