#!/usr/bin/env python2
import sys, os, os.path, glob, subprocess, shlex, shutil, copy
import random, time, datetime, gc, imp
import numpy as np

import sqlite3
import psrfits
import filterbank

import rfifind    #From PRESTO
import sifting    #From PRESTO
import psr_utils  #From PRESTO
import prestofft  #From pypulsar
import datfile as datfile_pypulsar

import warnings
import multiprocessing
from multiprocessing.pool import ThreadPool
from multiprocessing import Pool
from functools import partial
from multiprocessing import Semaphore
from multiprocessing import Process
warnings.simplefilter('ignore', UserWarning)

import jerk_v1_3 as jerk

# input parameters: infile, work_dir, log_abspath, numharm=8, zmax=0, wmax=0, other_flags="", dict_env = {}, flag_verbose=0, flag_LOG_append=1

if (len(sys.argv) == 1):
	print "Usage: %s -config <configfile.cfg> -obs <observation_name> -infile <.dat>"  % (os.path.basename(sys.argv[0]))
	print
	exit()
else:
	for j in range( 1, len(sys.argv)):
		if (sys.argv[j] == "-config"):
			config_filename = sys.argv[j+1]
		elif (sys.argv[j] == "-v"):
			flag_verbose = 1
		elif (sys.argv[j] == "-obs"):
			obsname = sys.argv[j+1]
		elif (sys.argv[j] == "-infile"):
			infile = sys.argv[j+1:]




ncores_accelsearch = 48
sema = Semaphore(ncores_accelsearch)
all_processes = []


for ifile in infile:
 


	datdir=os.path.dirname(ifile)
	dat_basename=os.path.basename(ifile)
	ifile=ifile.strip()
	list_files_to_search=[]
	pwd = os.getcwd()
	config                  = jerk.SurveyConfiguration(config_filename, obsname)
	dict_flag_steps = {'flag_step_dedisperse': config.flag_step_dedisperse , 'flag_step_realfft': config.flag_step_realfft, 'flag_step_accelsearch': config.flag_step_accelsearch}
	obsfile = os.path.basename(obsname)
	LOG_basename = '03_prepsubband_and_search_FFT_'+obsfile
	LOG_dir = os.path.join(config.root_workdir, "LOG")
	work_dir = datdir
	obs=datdir.split('/')[-3]
	seg=datdir.split('/')[-2]
	ck=datdir.split('/')[-1]

	dir_birdies = os.path.join(config.root_workdir, "02_BIRDIES")
	zapfile = dir_birdies+'/'+obs+'_DM00.00.zaplist'
	flag_use_cuda = config.flag_use_cuda
	list_cuda_ids =   config.list_cuda_ids
	numharm=   config.accelsearch_gpu_numharm
	list_zmax =   config.accelsearch_gpu_list_zmax
	list_wmax=   config.accelsearch_gpu_list_wmax
	period_to_search_min_s = config.period_to_search_min
	period_to_search_max_s= config.period_to_search_max 
	other_flags_accelsearch = config.accelsearch_flags  
	flag_remove_fftfile= config.flag_remove_fftfiles
	presto_env_zmax_0 = config.presto_env
	presto_env_zmax_any= config.presto_gpu_env
	flag_verbose=   int(1)
	flag_LOG_append=  int(1)



#	periodicity_search_FFT_partial=partial(jerk.periodicity_search_FFT, work_dir, LOG_dir, LOG_basename, zapfile, flag_use_cuda, list_cuda_ids, numharm, list_zmax, list_wmax, period_to_search_min_s, period_to_search_max_s, other_flags_accelsearch, flag_remove_fftfile, presto_env_zmax_0, presto_env_zmax_any,flag_verbose, flag_LOG_append, dict_flag_steps)

#	periodicity_search_FFT_partial(infile[i])

        sema.acquire()
        p = Process(target=jerk.periodicity_search_FFT, args=(sema, work_dir, LOG_dir, LOG_basename, zapfile, flag_use_cuda, list_cuda_ids, numharm, list_zmax, list_wmax, period_to_search_min_s, period_to_search_max_s, other_flags_accelsearch, flag_remove_fftfile, presto_env_zmax_0, presto_env_zmax_any,flag_verbose, flag_LOG_append, dict_flag_steps, ifile))
        all_processes.append(p)
        p.start()

	print "periodicity_search_FFT function ran for file: ", ifile


for p in all_processes:
	p.join()


#periodicity_search_FFT(work_dir, LOG_basename, zapfile, flag_use_cuda=0, list_cuda_ids=[0], numharm=8, list_zmax=[20], list_wmax=[50], period_to_search_min_s=0.001, period_to_search_max_s=20.0, other_flags_accelsearch="", flag_remove_fftfile=0, presto_env_zmax_0=os.environ['PRESTO'], presto_env_zmax_any=os.environ['PRESTO'], flag_verbose=0, flag_LOG_append=1, dict_flag_steps= {'flag_step_dedisperse':1 , 'flag_step_realfft': 1, 'flag_step_accelsearch': 1}, list_files_to_search="")


#accelsearch(infile, work_dir, log_abspath, numharm=8, zmax=0, wmax=0, other_flags="", dict_env = {}, flag_verbose=0, flag_LOG_append=1)


