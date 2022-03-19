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
    elif(sys.argv[i]=='-pb_er'):
        pb_days_er = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-omdot_er'):
        omdot_degperyear_er = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-e_er'):
        e_er = np.float128(sys.argv[i+1])
    elif(sys.argv[i]=='-h'):
        print("USAGE: python(3.6) Mt_er.py -pb(days) -pb_er -omdot(deg per year) -omdot_er -e eccentricity -e_er")
        exit()

pb_s=pb_days*(24*60*60)

pb_s_er = pb_days_er*(24*60*60)
T0=4.925490947e-6

omdot_radperyear = omdot_degperyear*2*pi/360

omdot_radperyear_er = omdot_degperyear_er*2*pi/360

omdot_radpersec=omdot_radperyear/(365*24*60*60)

omdot_radpersec_er = omdot_radperyear_er/(365*24*60*60)


Mtot= (omdot_radpersec*(1-(e*e))*(pb_s/(2*pi))**(5./3)/(3*T0**(2./3)))**(3./2)

k = (2*pi)**(-5./2)/(3*T0)

dMtbydwdot = 3./2*omdot_radpersec**(0.5)*pb_s**(5./2)*(1-e**2)**(3./2)*k

dMtbydpb = (5./2)*pb_s**(3./2)*omdot_radpersec**(3./2)*(1-e**2)**(3./2)*k

dMtbyde = (3./2)*(1-e**2)**(0.5)*(-2*e)*pb_s**(5./2)*omdot_radpersec**(3./2)*k

Mt_er = np.sqrt((dMtbydwdot*omdot_radpersec_er)**2 + (dMtbydpb*pb_s_er)**2 + (dMtbyde*e_er)**2)


print(Mt_er)
