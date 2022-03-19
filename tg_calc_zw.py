#This script will take values of z and w or a and adot and calculates a and adot or z,w respectively

import numpy as np
import matplotlib.pyplot as plt
import argparse
import os,sys, subprocess

c = 299792458
h = 1
P = 1e-3
w=0
z=0
a=0
adot=0
for j in range(1, len(sys.argv)):
        if(sys.argv[j] == '-harm'):
            h = float(sys.argv[j+1])
        elif(sys.argv[j] == '-P'):
            P = float(sys.argv[j+1])
        elif(sys.argv[j] == '-T'):
            T = float(sys.argv[j+1])
        elif(sys.argv[j] == '-z'):
            z = float(sys.argv[j+1])
            a = z*c*P/(h*T*T)
        elif(sys.argv[j] == '-w'):
            w = float(sys.argv[j+1])
            adot = w*c*P/(h*T*T*T)
        elif(sys.argv[j] == '-a'):
            a = float(sys.argv[j+1])
            z = a*h*T*T/(c*P)
        elif(sys.argv[j] == '-adot'):
            adot = float(sys.argv[j+1])
            w = adot*h*T*T*T/(P*c)
        elif(sys.argv[j] == '-h'):
            print("python zw-calc -T Tobs -P spin period -h harmonic number -a -adot")


print("z : ", z)
print("a :", a)
print("w : ", w)
print("adot : ", adot)









