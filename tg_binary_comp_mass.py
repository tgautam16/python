#!/usr/bin/python

import os,sys,math
import argparse
import numpy as np
from scipy.constants import pi,c

#from math import pi
inc = 90
for i in range(1,len(sys.argv)):
    if(sys.argv[i]=='-pb'):
        pb_days = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-x'):
        x = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-f'):
        f = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-i'):
        inc = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-Mtot'):
        Mtot = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-h'):
        print("USAGE: python(3.6) Comp_mass_4mMt.py -pb(days) -x (lt-s) -i inclination(optional) -Mtot Total mass (optional)")
        exit()

pb_s=pb_days*(24*60*60)

T0=4.925490947e-6


def comp_mass_4rm_Mtot(pb_s, Mtot, x, i=90):
    mc_min = (4*pi**2*x**3*Mtot**2/(T0*pb_s**2)*np.sin(np.deg2rad(i))**3)**(1./3)
    return mc_min

mc_min=comp_mass_4rm_Mtot(pb_s=pb_s,Mtot=Mtot,x=x,i=inc)
print("Minimum companion mass is: ")
print(mc_min)
