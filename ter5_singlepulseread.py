#!/usr/bin/env python

import numpy as np
import matplotlib.pyplot as plt
import astropy.units as u
from astropy.time import Time
from pulsar.predictor import Polyco
import argparse
print("HELLO")
profnorm_data = 'Fold_2018-08-02T17:02:21.573191900_6660.05143552sec_nc64_ng128.npy'
profnorm_dir = '/u/tasha/Ter5A/'
fn = profnorm_dir+profnorm_data
from mpl_toolkits import mplot3d

#x=np.arange(0,5,0.1)
#y=np.sin(x)
#plt.plot(x,y)
#plt.show()


#buff=np.memmap(fn,dtype=np.uint16, mode ='r', offset =0, shape=(64,128))
#d=buff
#plt.imshow(d,aspect='auto')
#plt.colorbar()
#profnorm =np.load('/u/tasha/Ter5A/Fold_2018-08-02T17:02:21.573191900_6660.05143552sec_nc64_ng128.npy')
#profnorm = np.memmap(fn,dtype=np.uint16, mode='r', offset=0, shape=())

profnorm_map = np.memmap(fn, dtype=np.uint16, mode='r', offset=0, shape=(575406, 128, 64))
n12 = profnorm_map.mean(axis=0)
n02 = profnorm_map.mean(axis=1)
n01 = profnorm_map.mean(axis=2)
n2= profnorm_map - np.mean(profnorm_map,axis=1, keepdims=True)

#print("The shape of profnorm is:",profnorm_map.shape)
#print("The shape of n12 is:",n12.shape)
#print("The shape of n02 is:",n02.shape)
#print("The shape of n01 is:",n01.shape)
#print("The shape of n2=data-mean is :",n2.shape)

s=np.std(n12.mean(1))
m=np.mean(n12.mean(1))


plt.figure(figsize=(12,12))
plt.imshow(n2.mean(0),aspect='auto')
plt.xlabel('phase')
plt.ylabel('freq')
plt.figure()
plt.plot(n2.mean(0).mean(-1))


#plt.imshow(n12.T,aspect='auto',cmap='jet', vmin=m-3*s, vmax=m+5*s)
#plt.colorbar()
#plt.show()

#plt.imshow(n12,aspect='auto')
plt.show()
#ax = plt.axes(projection='3d')
#ax.plot3D(profnorm_map(axis=0),profnorm_map(axis=1),profnorm_map(axis=2),'gray')
