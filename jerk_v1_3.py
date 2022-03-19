#!/usr/bin/env python2
#################### JERK SEARCH PIPELINE -- REQUIRES CALL_ACCEL.PY[with correct name of main pipeline] TO CREATE DM LIST FILE IN LOG TO PERFORM ACCELSEARCH ##########################
################### THIS CODE RUNS ACCELERATION SEARCH OF INDIVIDUAL TIME SERIES ON DIFFERENT NODES IN THE CLUSTER ############################
#################### VERSION 1.3 ############################################

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

string_version = "1.3 (18May2020)"

#Base pipeline is made by A. Ridolfi version 1.5(20May2019)
class Observation(object):
        def __init__(self, file_name, data_type="psrfits"):
                self.file_abspath = os.path.abspath(file_name)
                self.file_nameonly = self.file_abspath.split("/")[-1]
                self.file_basename, self.file_extension = os.path.splitext(self.file_nameonly)
                
                if data_type=="filterbank":
                        object_file = filterbank.FilterbankFile(self.file_abspath)

                        self.N_samples        = object_file.nspec
                        self.t_samp_s         = object_file.dt
                        self.T_obs_s          = self.N_samples * self.t_samp_s
                        self.nbits            = object_file.header['nbits']
                        self.nchan            = object_file.nchan
                        self.chanbw_MHz       = object_file.header['foff']
                        self.bw_MHz           = self.nchan * self.chanbw_MHz
                        self.freq_central_MHz = object_file.header['fch1'] + object_file.header['foff']*0.5*object_file.nchan
                        self.freq_high_MHz    = np.amax(object_file.freqs)
                        self.freq_low_MHz     = np.amin(object_file.freqs)
                        self.MJD_int          = int(object_file.header['tstart'])
                        self.MJD              = object_file.header['tstart']

                        self.source_name      = object_file.header['source_name'].strip()
                        #self.telescope        = object_file.header['']

                        self.dict_fields_and_types_properties = {
                                          'file_nameonly':     "TEXT",
                                          'bw_MHz':            "REAL",
                                          'N_samples':         "INTEGER",
                                          't_samp_s':          "REAL",
                                          'T_obs_s':           "REAL",
                                          'nbits':             "INTEGER",
                                          'chanbw_MHz':        "REAL",
                                          'freq_central_MHz':  "REAL",
                                          'freq_high_MHz':     "REAL",
                                          'freq_low_MHz':      "REAL",
                                          'MJD_int':           "INTEGER",
                                          'MJD':               "INTEGER",
                                          'nchan':             "INTEGER",
                                          'source_name':       "TEXT",
                                          'mask':              "TEXT",
                                          'ts_DM0':            "TEXT",
                                          'fft_DM0':           "TEXT",
                                          'fft_DM0_rednoise':  "TEXT",
                                          'fft_DM0_zapped':    "TEXT"
                        }


                if data_type=="psrfits":
                        print "Reading PSRFITS...."
                        if psrfits.is_PSRFITS(file_name) == False:
                                raise TypeError("ERROR: DATA_TYPE=\"psrfits\" but the input file \"%s\" does not seem to be a PSRFITS file!" % (file_name))

                        object_file = psrfits.PsrfitsFile(self.file_abspath)
                        self.bw_MHz           = object_file.specinfo.BW
                        self.N_samples        = object_file.specinfo.N
                        self.T_obs_s          = object_file.specinfo.T
                        self.backend          = object_file.specinfo.backend
                        self.nbits            = object_file.specinfo.bits_per_sample
                        self.date_obs         = object_file.specinfo.date_obs
                        self.dec_deg          = object_file.specinfo.dec2000
                        self.dec_str          = object_file.specinfo.dec_str
                        self.chanbw_MHz       = object_file.specinfo.df
                        self.t_samp_s         = object_file.specinfo.dt
                        self.freq_central_MHz = object_file.specinfo.fctr
                        self.receiver         = object_file.specinfo.frontend
                        self.freq_high_MHz    = object_file.specinfo.hi_freq
                        self.freq_low_MHz     = object_file.specinfo.lo_freq
                        self.MJD_int          = object_file.specinfo.mjd
                        self.nchan            = object_file.specinfo.num_channels
                        self.observer         = object_file.specinfo.observer
                        self.project          = object_file.specinfo.project_id
                        self.ra_deg           = object_file.specinfo.ra2000
                        self.ra_str           = object_file.specinfo.ra_str
                        self.seconds_of_day   = object_file.specinfo.secs
                        self.source_name      = object_file.specinfo.source
                        self.telescope        = object_file.specinfo.telescope

                        self.dict_fields_and_types_properties = {
                                          'file_nameonly':     "TEXT",
                                          'bw_MHz':            "REAL",
                                          'T_obs_s':           "REAL",
                                          'N_samples':         "INTEGER",
                                          'backend':           "TEXT",
                                          'nbits':             "INTEGER",
                                          'date_obs':          "TEXT",
                                          'dec_deg':           "INTEGER",
                                          'dec_str':           "TEXT",
                                          'chanbw_MHz':        "REAL",
                                          't_samp_s':          "REAL",
                                          'freq_central_MHz':  "REAL",
                                          'receiver':          "TEXT",
                                          'freq_high_MHz':     "REAL",
                                          'freq_low_MHz':      "REAL",
                                          'MJD_int':           "INTEGER",
                                          'nchan':             "INTEGER",
                                          'observer':          "TEXT",
                                          'project':           "TEXT",
                                          'ra_deg':            "TEXT",
                                          'ra_str':            "TEXT",
                                          'seconds_of_day':    "REAL",
                                          'source_name':       "TEXT",
                                          'telescope':         "TEXT",
                                          'mask':              "TEXT",
                                          'ts_DM0':            "TEXT",
                                          'fft_DM0':           "TEXT",
                                          'fft_DM0_rednoise':  "TEXT",
                                          'fft_DM0_zapped':    "TEXT"
                        }


                if data_type=="psrfits_header_only":
                        print "Reading PSRFITS (header only)...."
                        if psrfits.is_PSRFITS(file_name) == False:
                                raise TypeError("ERROR: DATA_TYPE=\"psrfits\" but the input file \"%s\" does not seem to be a PSRFITS file!" % (file_name))

                        self.bw_MHz           = np.float(get_command_output("vap -n -c bw %s" % (file_name)).split()[-1])
                        self.N_samples           = np.float(get_command_output_with_pipe("readfile %s" % (file_name), "grep Spectra").split("=")[-1])

                        self.T_obs_s          = np.float(get_command_output("vap -n -c length %s" % (file_name)).split()[-1])
                        
                        self.backend          = get_command_output("vap -n -c backend %s" % (file_name)).split()[-1]
                        self.nbits            = int(get_command_output_with_pipe("readfile %s" % (file_name), "grep bits").split("=")[-1])
                        #self.date_obs         = object_file.specinfo.date_obs   #NOT USED
                        #self.dec_deg          = object_file.specinfo.dec2000    #NOT USED
                        #self.dec_str          = object_file.specinfo.dec_str    #NOT USED
                        self.chanbw_MHz       = np.float(get_command_output_with_pipe("readfile %s" % (file_name), "grep Channel").split("=")[-1])
                        self.t_samp_s         = np.float(get_command_output("vap -n -c tsamp %s" % (file_name)).split()[-1])
                        self.freq_central_MHz = np.float(get_command_output("vap -n -c freq %s" % (file_name)).split()[-1])
                        self.receiver         = get_command_output("vap -n -c rcvr %s" % (file_name)).split()[-1]
                        self.freq_high_MHz    = np.float(get_command_output_with_pipe("readfile %s" % (file_name), "grep High").split("=")[-1])
                        self.freq_low_MHz     = np.float(get_command_output_with_pipe("readfile %s" % (file_name), "grep Low").split("=")[-1])
                        #self.MJD_int          = object_file.specinfo.mjd        #NOT USED
                        self.nchan            = int(get_command_output("vap -n -c nchan %s" % (file_name)).split()[-1])
                        #self.observer         = object_file.specinfo.observer   #NOT USED
                        #self.project          = object_file.specinfo.project_id #NOT USED
                        #self.ra_deg           = object_file.specinfo.ra2000
                        #self.ra_str           = object_file.specinfo.ra_str
                        #self.seconds_of_day   = object_file.specinfo.secs
                        #self.source_name      = object_file.specinfo.source
                        #self.telescope        = object_file.specinfo.telescope

                        self.dict_fields_and_types_properties = {
                                          'file_nameonly':     "TEXT",
                                          'bw_MHz':            "REAL",
                                          'T_obs_s':           "REAL",
                                          'N_samples':         "INTEGER",
                                          'backend':           "TEXT",
                                          'nbits':             "INTEGER",
                                          'date_obs':          "TEXT",
                                          'dec_deg':           "INTEGER",
                                          'dec_str':           "TEXT",
                                          'chanbw_MHz':        "REAL",
                                          't_samp_s':          "REAL",
                                          'freq_central_MHz':  "REAL",
                                          'receiver':          "TEXT",
                                          'freq_high_MHz':     "REAL",
                                          'freq_low_MHz':      "REAL",
                                          'MJD_int':           "INTEGER",
                                          'nchan':             "INTEGER",
                                          'observer':          "TEXT",
                                          'project':           "TEXT",
                                          'ra_deg':            "TEXT",
                                          'ra_str':            "TEXT",
                                          'seconds_of_day':    "REAL",
                                          'source_name':       "TEXT",
                                          'telescope':         "TEXT",
                                          'mask':              "TEXT",
                                          'ts_DM0':            "TEXT",
                                          'fft_DM0':           "TEXT",
                                          'fft_DM0_rednoise':  "TEXT",
                                          'fft_DM0_zapped':    "TEXT"
                        }


               # GMRT CASE
                elif data_type=="gmrt":
                        dict_properties = self.read_GMRT_header(self.file_abspath)

                        self.telescope                = dict_properties['Site']
                        self.backend                  = "GMRT_digital"
                        self.observer                 = dict_properties['Observer']
                        self.project                  = dict_properties['Proposal']
                        self.array_mode               = dict_properties['Array Mode']  #GMRT-only
                        self.observing_mode           = dict_properties['Observing Mode'] #GMRT-only
                        self.date_obs                 = dict_properties['Date']
                        self.num_antennas             = int(dict_properties['Num Antennas'])
                        self.antenna_list             = dict_properties['Antenna List']
                        self.nchan                    = int(dict_properties['Num Channels'])
                        self.chanbw_MHz               = np.fabs(np.float(dict_properties['Channel width']))
                        self.freq_low_MHz             = np.float(dict_properties['Frequency Ch.1'])
                        self.freq_central_MHz         = self.freq_low_MHz + (0.5*self.nchan)*self.chanbw_MHz
                        self.freq_high_MHz            = self.freq_low_MHz + (1.0*self.nchan)*self.chanbw_MHz
                        self.bw_MHz                   = self.chanbw_MHz*self.nchan
                        self.receiver                 = "%s-%sMHz" % ( str(int(self.freq_low_MHz)), str(int(self.freq_high_MHz)))
                        self.t_samp_s                 = np.float(dict_properties['Sampling Time'])*1.0e-6
                        self.nbits                    = int(dict_properties['Num bits/sample'])
                        self.data_format              = dict_properties['Data Format'] #GMRT-only                                                                                                                                                                                                                                                                   
                        self.polarization             = dict_properties['Polarizations']
                        self.MJD_int                  = int(dict_properties['MJD'].split(".")[0])
                        self.UTC                      = dict_properties['UTC']
                        self.source_name              = dict_properties['Source']
                        self.ra_str                   = dict_properties['Coordinates'].split(",")[0].strip()
                        self.dec_str                  = dict_properties['Coordinates'].split(",")[0].strip()
                        self.coordinate_sys           = dict_properties['Coordinate Sys']
                        self.drift_rate               = dict_properties['Drift Rate']
                        self.numbytes                 = os.path.getsize(self.file_abspath.replace(".gmrt_hdr",".gmrt_dat"))
                        self.N_samples                = int(   self.numbytes / ( (self.nchan)*(self.nbits/8.) )  )
                        self.T_obs_s                  = self.N_samples*self.t_samp_s
                        self.bad_channels             = dict_properties['Bad Channels']
                        self.bit_shift_value          = dict_properties['Bit shift value']
                        self.dict_fields_and_types_properties = {
                                          'file_nameonly':     "TEXT",
                                          'bw_MHz':            "REAL",
                                          'N_samples':         "INTEGER",
                                          'T_obs_s':           "REAL",
                                          'backend':           "TEXT",
                                          'nbits':             "INTEGER",
                                          'date_obs':          "TEXT",
                                          'dec_deg':           "INTEGER",
                                          'dec_str':           "TEXT",
                                          'chanbw_MHz':        "REAL",
                                          't_samp_s':          "REAL",
                                          'freq_central_MHz':  "REAL",
                                          'receiver':          "TEXT",
                                          'freq_high_MHz':     "REAL",
                                          'freq_low_MHz':      "REAL",
                                          'MJD_int':           "INTEGER",
                                          'nchan':             "INTEGER",
                                          'observer':          "TEXT",
                                          'project':           "TEXT",
                                          'ra_str':            "TEXT",
                                          'source_name':       "TEXT",
                                          'telescope':         "TEXT",
                                          'mask':              "TEXT",
                                          'ts_DM0':            "TEXT",
                                          'fft_DM0':           "TEXT",
                                          'fft_DM0_rednoise':  "TEXT",
                                          'fft_DM0_zapped':    "TEXT"
                        }

                #Processing properties
                self.mask             = ""
                self.ts_DM0           = ""
                self.fft_DM0          = ""
                self.fft_DM0_rednoise = ""
                self.zaplist_file     = ""
                self.fft_DM0_zapped   = ""
                self.rfifind          = 'pending'
                self.dedispersion     = 'pending'
                self.realfft          = 'pending'
                self.accelsearch      = 'pending'
                self.realfft_DM0      = 'pending'
                self.zaplist          = 'pending'
                self.prepdata_DM0     = 'pending'
                self.rednoise_DM0     = 'pending'
                self.zapbirds         = 'pending'

                self.dict_fields_and_types_processing = {
                                          'file_nameonly':     "TEXT",
                                          'prepdata_DM0':      "TEXT",
                                          'rfifind':           "TEXT",
                                          'dedispersion':      "TEXT",
                                          'realfft_DM0':       "TEXT",
                                          'realfft':           "TEXT",
                                          'zaplist':           "TEXT",
                                          'rednoise_DM0':      "TEXT",
                                          'accelsearch':       "TEXT",
                                          'zapbirds':          "TEXT",
                }


        def read_GMRT_header(self, gmrtdat_filename):
                gmrthdr_filename = gmrtdat_filename.replace(".gmrt_dat", ".gmrt_hdr")

                gmrthdr_file = open(gmrthdr_filename, "r")

                dict_properties = {}
                for line in gmrthdr_file:
                        if line != "\n" and (not line.startswith("#")):
                                key, value = line.split(" : ")
                                dict_properties[key.strip()] = value.strip()
                return dict_properties



def execute_and_log(command, work_dir, log_abspath, dict_envs={}, flag_append=0, flag_verbose=0):
        datetime_start = (datetime.datetime.now()).strftime("%Y/%m/%d  %H:%M")
        time_start = time.time()
        if flag_append == 1:
                flag_open_mode = "a"
        else:
                flag_open_mode = "w+"
        log_file = open("%s" % (log_abspath), flag_open_mode)
        executable = command.split()[0]
        

        log_file.write("****************************************************************\n")
        log_file.write("START DATE AND TIME: %s\n" % (datetime_start))
        log_file.write("\nCOMMAND:\n")
        log_file.write("%s\n\n" % (command))
        log_file.write("WORKING DIRECTORY: %s\n" % (work_dir))
        log_file.write("****************************************************************\n")
        log_file.flush()
        

        list_for_Popen = command.split()
        env_subprocess = os.environ.copy()
        if dict_envs: #If the dictionary is not empty                                                                                                                                                            
                for k in dict_envs.keys():
                        env_subprocess[k] = dict_envs[k]
        
        proc = subprocess.Popen(list_for_Popen, stdout=log_file, stderr=log_file, cwd=work_dir, env=env_subprocess)
        proc.communicate()  #Wait for the process to complete                                                                                                                                                    

        datetime_end = (datetime.datetime.now()).strftime("%Y/%m/%d  %H:%M")
        time_end = time.time()

        if flag_verbose==1:
                print "execute_and_log:: COMMAND: %s" % (command)
                print "execute_and_log:: which %s: "% (executable), get_command_output("which %s" % (executable))
                print "execute_and_log:: WORKING DIRECTORY = ", work_dir
                print "execute_and_log:: CHECK LOG WITH: \"tail -f %s\"" % (log_abspath); sys.stdout.flush()
                print "execute_and_log: list_for_Popen = ", list_for_Popen
                print "execute_and_log: log_file       = ", log_file
                print "execute_and_log: env_subprocess = ", env_subprocess

        log_file.write("\nEND DATE AND TIME: %s\n" % (datetime_end))
        log_file.write("\nTOTAL TIME TAKEN: %d s\n" % (time_end - time_start))
        log_file.close()



def sift_candidates(work_dir, LOG_basename,  dedispersion_dir, observation_basename, segment_label, chunk_label, list_zmax, list_wmax, flag_remove_duplicates, flag_DM_problems, flag_remove_harmonics, minimum_numDMs_where_detected, minimum_acceptable_DM=2.0, period_to_search_min_s=0.001, period_to_search_max_s=15.0, flag_verbose=0 ):
        work_dir_basename = os.path.basename(work_dir)
        string_ACCEL_files_dir = os.path.join(dedispersion_dir, observation_basename, segment_label, chunk_label)

        best_cands_filename = "%s/best_candidates_%s.siftedcands" % (work_dir, work_dir_basename)
        if flag_verbose == 1:
                print "sift_candidates:: best_cands_filename = %s" % (best_cands_filename)
                print "sift_candidates:: string_ACCEL_files_dir = %s" % (string_ACCEL_files_dir)
        
        list_ACCEL_files = []
        for w in list_wmax:
		for z in list_zmax:
			if int(w) == 0:
				list_ACCEL_files = list_ACCEL_files + glob.glob("%s/*ACCEL_%d" % (string_ACCEL_files_dir, z) )
			else:
               			list_ACCEL_files = list_ACCEL_files + glob.glob("%s/*ACCEL_%d_JERK_%d" % (string_ACCEL_files_dir, z, w) )
	#list_ACCEL_files = list_ACCEL_files + glob.glob("%s/*ACCEL_%d*JERK_%d" % (string_ACCEL_files_dir, z, w) )
        log_abspath = "%s/LOG_%s.txt" % (LOG_dir, LOG_basename)
        print "\033[1m >> TIP:\033[0m Check sifting output with '\033[1mcat %s\033[0m'" % (log_abspath)

        list_DMs = [x.split("_ACCEL")[0].split("DM")[-1] for x in list_ACCEL_files]
        candidates = sifting.read_candidates(list_ACCEL_files, track=True)

        if flag_verbose == 1:
                print "sift_candidates:: z = %d" % (z)
		print "sift_candidates:: w = %d" % (w)
                print "sift_candidates:: %s/*ACCEL_%d_JERK_%d" % (string_ACCEL_files_dir, z, w)
                print "sift_candidates:: list_ACCEL_files = %s" % (list_ACCEL_file)
                print "sift_candidates:: list_DMs = %s" % (list_DMs)
                print "sift_candidates:: candidates.cands = ", candidates.cands
                print "sift_candidates:: Original N_cands = ", len(candidates.cands)
                print "sift_candidates:: sifting.sigma_threshold = ", sifting.sigma_threshold

        sifting.short_period = period_to_search_min_s
        sifting.long_period = period_to_search_max_s
        print
        print "Selecting candidates with periods %.4f < P < %.4f seconds..." % (period_to_search_min_s, period_to_search_max_s), ; sys.stdout.flush()
        print
        candidates.reject_shortperiod()
        candidates.reject_longperiod()
        print "done!"
        

        if len(candidates.cands) >=1:
                
                if flag_remove_duplicates==1: 
                        candidates = sifting.remove_duplicate_candidates(candidates)                        
                        if flag_verbose == 1:    print "sift_candidates:: removed duplicates. N_cands = ", len(candidates.cands)
                if flag_DM_problems==1:
                        candidates = sifting.remove_DM_problems(candidates, minimum_numDMs_where_detected, list_DMs, minimum_acceptable_DM)
                        if flag_verbose == 1:    print "sift_candidates:: removed DM probems. N_cands = ", len(candidates.cands)
                if flag_remove_harmonics==1:
                        candidates = sifting.remove_harmonics(candidates)
                        if flag_verbose == 1:    print "sift_candidates:: removed harmonics. N_cands = ", len(candidates.cands)
        else:
                print "sift_candidates:: ERROR: len(candidates.cands) < 1!!! candidates = %s" % (candidates)
                exit()
        

        if flag_verbose == 1:                print "sift_candidates:: Sorting the candidates by sigma...", ; sys.stdout.flush()
        candidates.sort(sifting.cmp_sigma)
        if flag_verbose == 1:                print "done!"

        if flag_verbose == 1:                print "sift_candidates:: Writing down the best candidates on file '%s'..." % (best_cands_filename), ; sys.stdout.flush()
        sifting.write_candlist(candidates, best_cands_filename)
        if flag_verbose == 1:                print "done!"

        if flag_verbose == 1:                print "sift_candidates:: writing down report on file '%s'..." % (log_abspath), ; sys.stdout.flush()
        candidates.write_cand_report(log_abspath)
        if flag_verbose == 1:                print "done!"

        return candidates




#def arg_helper(args_fold):
#	return fold_candidate(*args_fold)

def check_fold_result(fold_file_pfd, fold_file_bestprof, fold_file_ps, work_dir, what_fold):
	if what_fold == "rawdata":
		fold_file_pfd= "%s/raw_%s" % (work_dir, fold_file_pfd)
		fold_file_bestprof="%s/raw_%s" % (work_dir, fold_file_bestprof)
		fold_file_ps="%s/raw_%s" % (work_dir, fold_file_ps)
	else:
		fold_file_pfd= "%s/ts_%s" % (work_dir, fold_file_pfd)
                fold_file_bestprof="%s/ts_%s" % (work_dir, fold_file_bestprof)
                fold_file_ps="%s/ts_%s" % (work_dir, fold_file_ps)
	try:
		if (os.path.getsize(fold_file_pfd) > 0) and (os.path.getsize(fold_file_bestprof) > 0) and (os.path.getsize(fold_file_ps) > 0):
                	print "folded candidate exist already: "
                        print "File exist with size is > 0! Skipping..."
                        check_result = True
		else:
			print "check_fold_result:: Files exists but at least one of them has size = 0!"
			check_result = False

	except OSError:
                #print ("SIZE of files: %d"%((os.path.getsize(ACCEL_filename)),(os.path.getsize(ACCEL_cand_filename)), (os.path.getsize(ACCEL_txtcand_filename))))
                        print "Folding has not been executed, performing.."
			check_result = False

        return check_result
	

def fold_candidate(sema, work_dir, LOG_basename, raw_datafile, dir_dedispersion, obs, seg, ck, candidate, T_obs_s, mask, other_flags_prepfold="", presto_env=os.environ['PRESTO'], flag_verbose=0, flag_LOG_append=1, what_fold="rawdata"):
        log_abspath = "%s/LOG_%s.txt" % (LOG_dir, LOG_basename)
        dict_env = {'PRESTO': presto_env, 'PATH': "%s/bin:%s" % (presto_env, os.environ['PATH']), 'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env, os.environ['LD_LIBRARY_PATH'])}
        cand = candidate
        dir_accelfile = "%s/%s/%s/%s" % (dir_dedispersion, obs, seg, ck)
	cand_wmax = 0
	if 'JERK' in cand.filename.split("_"):
		cand_zmax = cand.filename.split("_")[-3]
        	cand_wmax = cand.filename.split("_")[-1]
		fold_file_pfd = "fold_%s_%s_%s_DM%.2f_z%s_w%s_JERK_Cand_%s.pfd" % (obs, seg, ck, cand.DM, cand_zmax, cand_wmax, cand.candnum)
		fold_file_bestprof = "fold_%s_%s_%s_DM%.2f_z%s_w%s_JERK_Cand_%s.pfd.bestprof" % (obs, seg, ck, cand.DM, cand_zmax, cand_wmax, cand.candnum)
		fold_file_ps = "fold_%s_%s_%s_DM%.2f_z%s_w%s_JERK_Cand_%s.pfd.ps" % (obs, seg, ck, cand.DM, cand_zmax, cand_wmax, cand.candnum)
	else:
        	cand_zmax = cand.filename.split("_")[-1]
		fold_file_pfd = "fold_%s_%s_%s_DM%.2f_z%s_w%s_ACCEL_Cand_%s.pfd" % (obs, seg, ck, cand.DM, cand_zmax, cand_wmax, cand.candnum)
                fold_file_bestprof = "fold_%s_%s_%s_DM%.2f_z%s_w%s_ACCEL_Cand_%s.pfd.bestprof" % (obs, seg, ck, cand.DM, cand_zmax, cand_wmax, cand.candnum)
                fold_file_ps = "fold_%s_%s_%s_DM%.2f_z%s_w%s_ACCEL_Cand_%s.pfd.ps" % (obs, seg, ck, cand.DM, cand_zmax, cand_wmax, cand.candnum)
	print "HERE IS THE NAMES TO CHECK: %s %s %s" % (fold_file_pfd, fold_file_bestprof, fold_file_ps)
        if what_fold=="timeseries":
                file_to_fold = os.path.join(dir_dedispersion, obs, seg, ck, cand.filename.replace("2017_DM", "_%s_%s"% (seg, ck)).split("_ACCEL")[0] + ".dat" )
         
	        cmd_prepfold = "prepfold %s -noxwin -accelcand %d -accelfile %s/%s.cand -o ts_fold_%s_%s_%s_DM%.2f_z%s_w%s   %s" % (other_flags_prepfold, cand.candnum, dir_accelfile, cand.filename, obs, seg, ck, cand.DM, cand_zmax, cand_wmax, file_to_fold)
        	
		if check_fold_result(fold_file_pfd, fold_file_bestprof, fold_file_ps, work_dir, what_fold) == False:
                	print "PERFORMING FOLDING NOW!!"
                        if flag_verbose==1:
                                print "fold_candidates:: cand.filename: ",  cand.filename
                                print "file_to_fold = ", file_to_fold
                                print "fold_candidates:: cmd_prepfold = %s" % (cmd_prepfold)
                                print "MASK used ", mask
                        execute_and_log(cmd_prepfold, work_dir, log_abspath, dict_env, flag_LOG_append)
                else:
                        if flag_verbose==1:
                                print "Folding seems to have been already executed on file %s. Skipping..." % (fold_file_pfd)

                if flag_verbose==1:
                        print "folding:: NOW I CHECK THE RESULT OF THE EXECUTION!"
                        print "for file: %s" % (fold_file_pfd)
        #check_accelsearch_result(infile, int(zmax), int(wmax))
                #if check_fold_result(fold_file_pfd, fold_file_bestprof, fold_file_ps, work_dir, what_fold) == False:
                #        if flag_verbose==1:
                #                print "False! Candidate still not folded, something is wrong with code!"
                #else:
                if flag_verbose==1:
                	print "Folded .ps files for timeseries is done, CANDIDATES HAVE BEEN PRODUCED for %s!" % (fold_file_pfd)
	elif what_fold=="rawdata":
                print "rawdata:: seg = ", seg
                file_to_fold = raw_datafile

                if seg == "full":
                        cmd_prepfold = "prepfold %s -noxwin -accelcand %d -accelfile %s/%s.cand -dm %.2f -mask %s -o raw_fold_%s_%s_%s_DM%.2f_z%s_w%s    %s" % (other_flags_prepfold, cand.candnum, dir_accelfile, cand.filename, cand.DM, mask, obs, seg, ck, cand.DM, cand_zmax, cand_wmax, file_to_fold)
                else:
                        segment_min = np.float(seg.replace("m", ""))
                        i_chunk = int(ck.replace("ck", ""))
                        T_obs_min = T_obs_s / 60.
                        start_frac = (i_chunk * segment_min) / T_obs_min
                        end_frac   = ((i_chunk + 1) * segment_min) / T_obs_min
                        
                        cmd_prepfold = "prepfold %s -start %.5f -end %.5f -noxwin -accelcand %d -accelfile %s/%s.cand -dm %.2f -mask %s -o raw_fold_%s_%s_%s_DM%.2f_z%s_w%s    %s" % (other_flags_prepfold, start_frac, end_frac, cand.candnum, dir_accelfile, cand.filename, cand.DM, mask, obs, seg, ck, cand.DM, cand_zmax, cand_wmax, file_to_fold)
                        
        	if check_fold_result(fold_file_pfd, fold_file_bestprof, fold_file_ps, work_dir, what_fold) == False:
                	print "PERFORMING FOLDING NOW!!"
			if flag_verbose==1:
                		print "fold_candidates:: cand.filename: ",  cand.filename
                		print "file_to_fold = ", file_to_fold
                		print "fold_candidates:: cmd_prepfold = %s" % (cmd_prepfold)
                		print "MASK used ", mask
        		execute_and_log(cmd_prepfold, work_dir, log_abspath, dict_env, flag_LOG_append)
        	else:
                	if flag_verbose==1:
				print "Folding seems to have been already executed on file %s. Skipping..." % (fold_file_pfd)

        	#if flag_verbose==1:
                #	print "folding:: NOW I CHECK THE RESULT OF THE EXECUTION!"
                #	print "for file: %s" % (fold_file_pfd)
        #check_accelsearch_result(infile, int(zmax), int(wmax))
        	#if check_fold_result(fold_file_pfd, fold_file_bestprof, fold_file_ps, work_dir, what_fold) == False:
                #	if flag_verbose==1:
                #        	print "False! Candidate still not folded, something is wrong with code!"
        	#else:
                #	if flag_verbose==1:
                #        	print "RAW folded .ps files for CANDIDATES HAVE BEEN PRODUCED for %s!" % (fold_file_pfd)

        
	sema.release()

#def sortcands(work_dir, LOG_basename, what_sort="rawdata"):
#	if what_sort=="rawdata":
#		list_raw_files = []
#		for i in glob.glob("*.bestprof"):
#			#print(i)
#			#print("bestprofs reading")
#			list_raw_files = list_raw_files + i
#		 print "List of raw files: ", list_raw_files


def make_even_number(number_int):
        if int(number_int) % 2 == 1:
                return int(number_int)-1
        elif int(number_int) % 2 == 0: 
                return int(number_int)
        else:
                print "ERROR: make_even_number:: number does not appear neither EVEN nor ODD!"
                exit()

def get_command_output(command, shell_state=False, work_dir=os.getcwd()):
        list_for_Popen = command.split()
        
        #print "alex_processes::get_command_output: %s" % (command)                                                       
        #print list_for_Popen
        #print "work_dir=%s" % (work_dir)
        if shell_state==False:
                proc = subprocess.Popen(list_for_Popen, stdout=subprocess.PIPE, shell=shell_state, cwd=work_dir)
        else:
                proc = subprocess.Popen([command], stdout=subprocess.PIPE, shell=shell_state, cwd=work_dir)
        out, err = proc.communicate()
        #print "OUT: -%s-" % (out)                                                                                          
        return out

def get_command_output_with_pipe(command1, command2):

    list_for_Popen_cmd1 = command1.split()
    list_for_Popen_cmd2 = command2.split()

    p1 = subprocess.Popen(list_for_Popen_cmd1, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(list_for_Popen_cmd2, stdin=p1.stdout, stdout=subprocess.PIPE)
    p1.stdout.close() 

    out, err = p2.communicate()
    return out


def get_rfifind_result(file_mask, LOG_file, flag_verbose=0):
        rfifind_mask = rfifind.rfifind(file_mask)

        N_int                  = rfifind_mask.nint
        N_chan                 = rfifind_mask.nchan
        N_int_masked           = len(rfifind_mask.mask_zap_ints)
        N_chan_masked          = len(rfifind_mask.mask_zap_chans)
        fraction_int_masked    = np.float(N_int_masked/N_int)
        fraction_chan_masked   = np.float(N_chan_masked/N_chan)
        
        if flag_verbose==1:
                print "get_rfifind_result:: file_mask: %s" % file_mask
                print "get_rfifind_result:: LOG_file: %s" % LOG_file

        if (fraction_int_masked > 0.5) or (fraction_chan_masked > 0.5):
                return "!Mask>50%"

        
        #Check if there was a problem with the clipping in first block and get the filename with that problem. Otherwise return True.
        cmd_grep_problem_clipping = "grep -l 'problem with clipping' %s" % (LOG_file) #-l option returns the name of the file that contains the expression
        cmd_grep_inf_results = "grep -l ' inf ' %s" % (LOG_file)
        output = get_command_output(cmd_grep_problem_clipping, True).strip()
        if output != "":
                if flag_verbose==1:
                        print "WARNING: File '%s' contains a problem with clipping in first block!" % (LOG_file)
                return "!ProbFirstBlock"

        output = get_command_output(cmd_grep_inf_results, True).strip()
        if output != "":
                if flag_verbose==1:   
                        print "WARNING: File '%s' contains an infinite result!" % (LOG_file) 
                return "!ProbInfResult"


        return "done"
        


        

def check_prepdata_result(LOG_file, flag_verbose=0):
        #Check if there was a problem with the clipping in first block and get the filename with that problem. Otherwise return True.
        cmd_grep_problem_clipping = "grep -l 'problem with clipping' %s" % (LOG_file) #-l option returns the name of the file that contains the expression
        cmd_grep_inf_results = "grep -l ' inf ' %s" % (LOG_file)
        output = get_command_output(cmd_grep_problem_clipping, True).strip()
        print "check_prepdata_result::output: -%s-" % (output)
        if output != "":
                if flag_verbose==1:
                        print "WARNING: File '%s' contains a problem with clipping in first block!" % (LOG_file)
                return False

        return True


def check_rfifind_outfiles(out_dir, basename, flag_verbose=0):
        for suffix in ["bytemask", "inf", "mask", "ps", "rfi", "stats"]:
                file_to_check = "%s/%s_rfifind.%s" % (out_dir, basename, suffix)
                if not os.path.exists(file_to_check):
                        if flag_verbose == 1:
                                print "ERROR: file %s not found!" % (file_to_check)
                        return False
                elif os.stat(file_to_check).st_size == 0:   #If the file has size 0 bytes
                        print "ERROR: file %s has size 0!" % (file_to_check)
                        return False
        return True


def check_rednoise_outfiles(fftfile_rednoise_abspath, flag_verbose=0):
        inffile_rednoise_abspath = fftfile_rednoise_abspath.replace("_red.fft", "_red.inf")

        if os.path.exists( fftfile_rednoise_abspath ) and (os.path.getsize(fftfile_rednoise_abspath) > 0) and os.path.exists(inffile_rednoise_abspath) and (os.path.getsize(inffile_rednoise_abspath) > 0):
                return True
        else:
                return False

def check_accelsearch_result(fft_infile, zmax, wmax, flag_verbose=0):
        fft_infile_nameonly = os.path.basename(fft_infile)
        #print "check_accelsearch_result:: sono qui! type(zmax): ", type(zmax) 
        fft_infile_basename = os.path.splitext(fft_infile_nameonly)[0]
        
        if flag_verbose==1:
                print "check_accelsearch_result:: infile_basename: ", fft_infile_basename
                print "check_accelsearch_result:: ACCEL_filename = ", ACCEL_filename
                print "check_accelsearch_result:: ACCEL_cand_filename" , ACCEL_cand_filename
                print "check_accelsearch_result:: ACCEL_txtcand_filename = ", ACCEL_txtcand_filename
                print "check_accelsearch_result:: sono qui 2!"
        
	if int(wmax) == 0:
		ACCEL_filename                =  fft_infile.replace(".fft", "_ACCEL_%d" % (zmax))
        	ACCEL_cand_filename           =  fft_infile.replace(".fft", "_ACCEL_%d.cand" % (zmax))
        	ACCEL_txtcand_filename        =  fft_infile.replace(".fft", "_ACCEL_%d.txtcand" % (zmax))

		#print "Inside check accel, checking in file: ", os.path.getsize(ACCEL_filename) 
		#print "Inside check accel, checking in file: ", os.path.getsize(ACCEL_cand_filename)
		#print "Inside check accel, checking in file: ", os.path.getsize(ACCEL_txtcand_filename)
	else:
		ACCEL_filename                =  fft_infile.replace(".fft", "_ACCEL_%d_JERK_%d" % (zmax,wmax))
		ACCEL_cand_filename           =  fft_infile.replace(".fft", "_ACCEL_%d_JERK_%d.cand" % (zmax,wmax))
		ACCEL_txtcand_filename        =  fft_infile.replace(".fft", "_ACCEL_%d_JERK_%d.txtcand" % (zmax,wmax))

        try:
                if (os.path.getsize(ACCEL_filename) > 0) and (os.path.getsize(ACCEL_cand_filename) > 0) and (os.path.getsize(ACCEL_txtcand_filename) > 0):
			print "size of accelfile while checking here: ", os.path.getsize(ACCEL_filename)
                        result_message = "check_accelsearch_result:: Files exist and their size is > 0! Skipping..."
                        check_result = True
                else:
                        result_message = "check_accelsearch_result:: Files exists but at least one of them has size = 0!"
                        check_result = False
        except OSError:
		#print ("SIZE of files: %d"%((os.path.getsize(ACCEL_filename)),(os.path.getsize(ACCEL_cand_filename)), (os.path.getsize(ACCEL_txtcand_filename))))
                result_message = "check_accelsearch_result:: OSError: It seems accelsearch has not been executed!"
                check_result = False

        if flag_verbose==1:
                print result_message
	#print result_message
        return check_result


def accelsearch(infile, work_dir, log_abspath, numharm=8, zmax=0, wmax=0, other_flags="", dict_env = {}, flag_verbose=0, flag_LOG_append=1):
        infile_nameonly = os.path.basename(infile)
        infile_basename = os.path.splitext(infile_nameonly)[0]
	if int(wmax) == 0:
        	inffile_empty = infile.replace(".fft", "_ACCEL_%d_empty" % (zmax))
	else: 
		inffile_empty = infile.replace(".fft", "_ACCEL_%d_JERK_%d_empty" % (zmax,wmax))


        cmd_accelsearch = "accelsearch %s -zmax %s -wmax %s -numharm %s %s" % (other_flags, zmax, wmax, numharm, infile)
        
        if flag_verbose==1:
                print
                print "BEGIN ACCELSEARCH ----------------------------------------------------------------------"

                print "accelsearch:: cmd_accelsearch: ", cmd_accelsearch
                print "accelsearch:: AND THIS IS THE ENV: ", dict_env
                print "accelsearch:: check_accelsearch_result(infile, int(zmax), int(wmax)) :: %s" % (check_accelsearch_result(infile, int(zmax), int(wmax)) )
                print "accelsearch:: work_dir = %s" % (work_dir)
                print "accelsearch:: infile = %s" % (infile)


        if check_accelsearch_result(infile, int(zmax), int(wmax)) == False and check_accelsearch_result(inffile_empty, int(zmax), int(wmax)) == False:
                if flag_verbose==1:
                        print "accelsearch:: eseguo: %s" % (cmd_accelsearch)
		print "PERFORMING ACCELSEARCH NOW!!"
                execute_and_log(cmd_accelsearch, work_dir, log_abspath, dict_env, flag_LOG_append)
        else:
                if flag_verbose==1:
                        print "accelsearch:: WARNING: accelsearch with zmax=%d, wmax=%d seems to have been already executed on file %s. Skipping..." % (int(zmax), int(wmax), infile_nameonly)

        if flag_verbose==1:
                print "accelsearch:: NOW I CHECK THE RESULT OF THE EXECUTION!"
		print "infile, z and w are: %s, %d, %d" % (infile, int(zmax), int(wmax))
	#check_accelsearch_result(infile, int(zmax), int(wmax))
        if check_accelsearch_result(infile, int(zmax), int(wmax)) == False:
                if flag_verbose==1:
                        print "False! Then I create a _empty file!"
                file_empty = open(inffile_empty, "w")
                file_empty.write("ACCELSEARCH DID NOT PRODUCE ANY CANDIDATES!")
        else:
                if flag_verbose==1:
                        print "accelsearch: GOOD! CANDIDATES HAVE BEEN PRODUCED for %s!" % (infile)
                
        if flag_verbose==1:
                print "END ACCELSEARCH ---------------------------------------------------------------------- "

def split_into_chunks(list_datfiles_to_split, LOG_basename,  work_dir, segment_min, i_chunk, presto_env=os.environ['PRESTO'], flag_LOG_append=1 ):        
        segment_length_s = segment_min * 60
        dict_env = {'PRESTO': presto_env, 'PATH': "%s/bin:%s" % (presto_env, os.environ['PATH']), 'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env, os.environ['LD_LIBRARY_PATH'])}
        
        
        log_abspath = "%s/LOG_%s.txt" % (LOG_dir, LOG_basename)
        
        
        
        for datfile_name in list_datfiles_to_split:
                print "sono nel loop:: ", datfile_name
                datfile = datfile_pypulsar.Datfile(datfile_name)
                print "sono nel loop:: ho importato il file ", datfile_name
                info_datfile = datfile.inf
        
                print "sono nel loop:: qui sono info"
                t_samp_s = info_datfile.dt
                print "sono nel loop:: ho importato il file e ora ho dt"
                N_samp = info_datfile.N
                T_obs_s = t_samp_s * N_samp
                print "sono nel loop:: ho importato il file e ora ho Tobs"
                
                start_fraction = (i_chunk * segment_length_s )/T_obs_s
                numout = make_even_number(int(segment_length_s / t_samp_s))

                print "sono nel loop:: numout = %s, dt = %s" % ( numout, t_samp_s)
                print "start_fraction = ", start_fraction
                string_min =  "%dm" % int(segment_min)
                string_chunck = "ck%02d" % i_chunk
                path_old = datfile.basefn
                path_new = path_old.replace("full", string_min).replace("ck00", string_chunck)
                
                new_outfile_name = "%s" % (os.path.basename(path_new)) 
                
                print "sono nel loop:: new_outfile_name = ", new_outfile_name
                cmd_prepdata_split = "prepdata -nobary -o %s -start %.3f -numout %s %s" % (new_outfile_name, start_fraction, numout,  datfile_name)
                print
                print "split_into_chunks:: cmd_prepdata_split = ", cmd_prepdata_split
                print "with work_dir = ", work_dir
                
                output_datfile = "%s/%s.dat" % (work_dir, new_outfile_name)
                output_inffile = "%s/%s.dat" % (work_dir, new_outfile_name)
                if check_prepdata_outfiles(output_datfile.replace(".dat", "")) == False:
                        execute_and_log(cmd_prepdata_split, work_dir, log_abspath, dict_env, flag_LOG_append)
                else:
                        print "NOTE: %s already exists. Skipping" % (new_outfile_name)


def periodicity_search_FFT(sema,work_dir, log_dir, LOG_basename, zapfile, flag_use_cuda=0, list_cuda_ids=[0], numharm=8, list_zmax=[20], list_wmax=[50], period_to_search_min_s=0.001, period_to_search_max_s=20.0, other_flags_accelsearch="", flag_remove_fftfile=0, presto_env_zmax_0=os.environ['PRESTO'], presto_env_zmax_any=os.environ['PRESTO'], flag_verbose=0, flag_LOG_append=1, dict_flag_steps= {'flag_step_dedisperse':1 , 'flag_step_realfft': 1, 'flag_step_accelsearch': 1}, list_files_to_search=""):       

#        if flag_verbose==1:
#                print "periodicity_search_FFT:: Files to search: ", "%s/*DM*.*.dat, excluding red" % (work_dir)
#                print "periodicity_search_FFT:: presto_env_zmax_0 = ", presto_env_zmax_0
#                print "periodicity_search_FFT:: presto_env_zmax_any = ", presto_env_zmax_any
        
        
#        list_files_to_search = sorted([ x for x in glob.glob("%s/*DM*.*.dat" % (work_dir)) if not "red" in x ])
        
        print "INSIDE THE PERIODICITY SEARCH FUNCTION FROM FUNCTION CALL_ACCEL"
	print "period min inside periodicity function: ", float(period_to_search_min_s)
	print "log inside:", log_dir
	frequency_to_search_max = 1./period_to_search_min_s
        frequency_to_search_min = 1./period_to_search_max_s
#        if flag_verbose==1:
#                print "frequency_to_search_min, ", frequency_to_search_min
#                print "frequency_to_search_max, ", frequency_to_search_max

#                print "periodicity_search_FFT:: WARNING: -flo and -fhi CURRENTLY DISABLED"
        dict_env_zmax_0   = {'PRESTO': presto_env_zmax_0,   'PATH': "%s/bin:%s" % (presto_env_zmax_0, os.environ['PATH']),   'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env_zmax_0,   os.environ['LD_LIBRARY_PATH'])}
        dict_env_zmax_any = {'PRESTO': presto_env_zmax_any, 'PATH': "%s/bin:%s" % (presto_env_zmax_any, os.environ['PATH']), 'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env_zmax_any, os.environ['LD_LIBRARY_PATH'])}

#        if flag_verbose==1:
#                print "periodicity_search_FFT:: dict_env_zmax_0 = ", dict_env_zmax_0
#                print "periodicity_search_FFT:: dict_env_zmax_any = ", dict_env_zmax_any
#                print "periodicity_search_FFT:: LOG_basename = ", LOG_basename
#                print "periodicity_search_FFT:: list_files_to_search = ", list_files_to_search
        log_abspath = "%s/LOG_%s.txt" % (log_dir, LOG_basename)
#        print "\033[1m >> TIP:\033[0m Follow periodicity search with: \033[1mtail -f %s\033[0m" % (log_abspath)

        zapfile_nameonly = os.path.basename(zapfile)
         #for i in range(len(list_files_to_search)):
#	if flag_verbose==1:
#		print "periodicity_search_FFT: inside loop con i = %d / %d" % (i, len(list_files_to_search))
	dat_file = list_files_to_search
	dat_file_nameonly = os.path.basename(dat_file)
	fft_file = dat_file.replace(".dat", ".fft")
	fft_file_nameonly = os.path.basename(fft_file)
	print
	if dict_flag_steps['flag_step_realfft'] == 1:
		print "Doing realfft  of %s..." % (dat_file_nameonly), ; sys.stdout.flush()
		realfft(dat_file, work_dir, log_dir, LOG_basename, "", presto_env_zmax_0, 0, flag_LOG_append)
		print "done!" ; sys.stdout.flush()

		print "Doing rednoise of %s..." % (dat_file_nameonly), ; sys.stdout.flush()
		rednoise(fft_file, work_dir, log_dir, LOG_basename, "", presto_env_zmax_0, flag_verbose)
		print "done!" ; sys.stdout.flush()
		 
		print "Applying zapfile '%s' to '%s'..." % (zapfile_nameonly, fft_file_nameonly), ; sys.stdout.flush()
		zapped_fft_filename, zapped_inf_filename = zapbirds(fft_file, zapfile, work_dir, log_dir, LOG_basename, presto_env_zmax_0, flag_verbose)
		zapped_fft_nameonly = os.path.basename(zapped_fft_filename)
		print "done!" ; sys.stdout.flush()
	else:
		print "STEP_REALFFT = 0, skipping realfft, rednoise, zapbirds..."

	#print "\033[1m >> TIP:\033[0m Follow accelsearch with '\033[1mtail -f %s\033[0m'" % (log_abspath)

	if dict_flag_steps['flag_step_accelsearch'] == 1: 
		print "list_zmax:", list_zmax	
		
		for z in list_zmax:
			print "z ", z
			#print "Doing accelsearch of %s with zmax = %4d and wmax =%4d..." % (zapped_fft_nameonly, z, w), ; sys.stdout.flush()
			if (int(z) == 0 or int(z) < 200.0):
				w = 0
				print "Doing accelsearch of %s with zmax = %4d..." % (zapped_fft_nameonly, z), ; sys.stdout.flush()
				dict_env = copy.deepcopy(dict_env_zmax_0)
				if flag_verbose==1:
					print "accelsearch:: zmax == 0 ----> dict_env = %s" % (dict_env)
				flag_cuda = ""
				accelsearch_flags = other_flags_accelsearch + flag_cuda #+ " -flo %s -fhi %s" % (frequency_to_search_min, frequency_to_search_max) 
					
				accelsearch(fft_file, work_dir, log_abspath, numharm=numharm, zmax=z, wmax=w, other_flags=accelsearch_flags, dict_env=dict_env, flag_verbose=flag_verbose, flag_LOG_append=flag_LOG_append)
				ACCEL_filename = fft_file.replace(".fft", "_ACCEL_%s" % (int(z)))
				print "done!" ; sys.stdout.flush()



			else:
				for w in list_wmax:
					if int(w) == 0:
						print "Doing accelsearch of %s with zmax = %4d..." % (zapped_fft_nameonly, z), ; sys.stdout.flush()
						dict_env = copy.deepcopy(dict_env_zmax_any)
						if flag_use_cuda == 1:
							gpu_id = random.choice(list_cuda_ids)
							flag_cuda = " -cuda %d " % (gpu_id) 
						else:
							flag_cuda = ""

						if flag_verbose==1:
							print "periodicity_search_FFT:: zmax == %d ----> dict_env = %s" % (int(z), dict_env)
							print "periodicity_search_FFT:: Now check CUDA: list_cuda_ids = ", list_cuda_ids
							print "periodicity_search_FFT:: flag_use_cuda = ", flag_use_cuda
							print "periodicity_search_FFT:: flag_cuda = ", flag_cuda

						accelsearch_flags = other_flags_accelsearch + flag_cuda #+ " -flo %s -fhi %s" % (frequency_to_search_min, frequency_to_search_max) 
			
					#	pr_z = [ multiprocessing.Process(target=accelsearch, args=(fft_file, work_dir, log_abspath, numharm, z, w, accelsearch_flags, dict_env, flag_verbose, flag_LOG_append)) for i in range(n_cores)]
					#	for pr in pr_z:
					#		pr.start()
					#	i = 0
					#	for pr in pr_z:
					#		pr.join()
					#		print "Done", i	
					#		i += 1

						accelsearch(fft_file, work_dir, log_abspath, numharm=numharm, zmax=z, wmax=w, other_flags=accelsearch_flags, dict_env=dict_env, flag_verbose=flag_verbose, flag_LOG_append=flag_LOG_append)
						ACCEL_filename = fft_file.replace(".fft", "_ACCEL_%s" % (int(z)))
						print "done!" ; sys.stdout.flush()
					else:
						print "Doing accelsearch of %s with zmax = %4d, wmax = %4d..." % (zapped_fft_nameonly, z, w), ; sys.stdout.flush()
						dict_env = copy.deepcopy(dict_env_zmax_any)
						if flag_use_cuda == 1:
							gpu_id = random.choice(list_cuda_ids)
							flag_cuda = " -cuda %d " % (gpu_id)
						else:
							flag_cuda = ""

						if flag_verbose==1:
							print "periodicity_search_FFT:: zmax == %d ----> dict_env = %s" % (int(z), dict_env)
							print "periodicity_search_FFT:: Now check CUDA: list_cuda_ids = ", list_cuda_ids
							print "periodicity_search_FFT:: flag_use_cuda = ", flag_use_cuda
							print "periodicity_search_FFT:: flag_cuda = ", flag_cuda

						accelsearch_flags = other_flags_accelsearch + flag_cuda #+ " -flo %s -fhi %s" % (frequency_to_search_min, frequency_to_search_max) 
							
						accelsearch(fft_file, work_dir, log_abspath, numharm=4, zmax=z, wmax=w, other_flags=accelsearch_flags, dict_env=dict_env, flag_verbose=flag_verbose, flag_LOG_append=flag_LOG_append)
						ACCEL_filename = fft_file.replace(".fft", "_ACCEL_%s_JERK_%s" % (int(z),int(w)))
						print "done!" ; sys.stdout.flush()
					   

	sema.release()

#if flag_remove_fftfile==1:
#        print "Removing %s..." % (fft_file_nameonly), ; sys.stdout.flush()
#        os.remove(fft_file)
#        print "...done!"; sys.stdout.flush()





def make_birds_file(ACCEL_0_filename, out_dir, log_filename, width_Hz, flag_grow=1, flag_barycentre=0, sigma_birdies_threshold=4, flag_verbose=0):
        infile_nameonly = os.path.basename(ACCEL_0_filename)
        infile_basename = infile_nameonly.replace("_ACCEL_0", "")
        birds_filename = ACCEL_0_filename.replace("_ACCEL_0", ".birds")
        log_file = open(log_filename, "a")

        #Skip first three lines
        if flag_verbose==1:
                print "make_birds_file:: Opening the candidates: %s" % (ACCEL_0_filename)
        candidate_birdies = sifting.candlist_from_candfile(ACCEL_0_filename)
        candidate_birdies.reject_threshold(sigma_birdies_threshold)

        #Write down candidates above a certain sigma threshold        
        list_birdies = candidate_birdies.cands
        if flag_verbose==1:
                print "make_birds_file:: Number of birdies = %d" % (len(list_birdies))
        file_birdies = open(birds_filename, "w")
        if flag_verbose==1:
                print "make_birds_file:: File_birdies: %s" % (birds_filename)
        for cand in list_birdies:
                file_birdies.write("%.3f     %.20f     %d     %d     %d\n" % (cand.f, width_Hz, cand.numharm, flag_grow, flag_barycentre)  )
        file_birdies.close()
        
        return birds_filename


                
def get_Fourier_bin_width(fft_infile):
        FFT_spectrum = prestofft.PrestoFFT(fft_infile)
        fourier_bin_width_Hz = FFT_spectrum.freqs[1] - FFT_spectrum.freqs[0]
        return fourier_bin_width_Hz

def check_zaplist_outfiles(fft_infile, flag_verbose=0):
        birds_filename    = fft_infile.replace(".fft", ".birds")
        zaplist_filename  = fft_infile.replace(".fft", ".zaplist")
        try:
                if (os.path.getsize(birds_filename) > 0) and (os.path.getsize(zaplist_filename)>0): #checks if it exists and its
                        return True
                else:
                        return False
        except OSError:
                return False

def check_prepdata_outfiles(basename, flag_verbose=0):
        dat_filename  = basename + ".dat"
        inf_filename  = basename + ".inf"
        try:
                if (os.path.getsize(dat_filename) > 0) and (os.path.getsize(inf_filename)>0): #checks if it exists and its
                        return True
                else:
                        return False
        except OSError:
                return False


def make_zaplist(fft_infile, out_dir, LOG_basename, common_birdies_filename, birds_numharm=4, other_flags_accelsearch="", presto_env=os.environ['PRESTO'], flag_verbose=0):
        fft_infile_nameonly = os.path.basename(fft_infile)
        fft_infile_basename = os.path.splitext(fft_infile_nameonly)[0]
        log_abspath = "%s/LOG_%s.txt" % (LOG_dir, LOG_basename)
        #file_log = open(log_abspath, "w"); file_log.close()        
        dict_env = {'PRESTO': presto_env, 'PATH': "%s/bin:%s" % (presto_env, os.environ['PATH']), 'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env, os.environ['LD_LIBRARY_PATH'])}

        #accelsearch
        

        if check_zaplist_outfiles(fft_infile) == False:
                if flag_verbose==1:              
                        print "Doing accelsearch...", ; sys.stdout.flush()
                        print fft_infile, birds_numharm, 0, other_flags_accelsearch, presto_env, flag_verbose
                accelsearch(fft_infile, out_dir, log_abspath, birds_numharm, 0, other_flags_accelsearch, dict_env, flag_verbose)
                if flag_verbose==1:    print "Done accelsearch!"
                ACCEL_0_filename = fft_infile.replace(".fft", "_ACCEL_0")
                fourier_bin_width_Hz = get_Fourier_bin_width(fft_infile)
                if flag_verbose==1:
                        print "fourier_bin_width_Hz: ", fourier_bin_width_Hz
                        print "Doing make_birds_file"; sys.stdout.flush()
                birds_filename = make_birds_file(ACCEL_0_filename=ACCEL_0_filename, out_dir=out_dir, log_filename=log_abspath, width_Hz=fourier_bin_width_Hz, flag_grow=1, flag_barycentre=0, sigma_birdies_threshold=4, flag_verbose=0)
                if flag_verbose==1:
                        print "Done make_birds_file!"; sys.stdout.flush()
                
                
                file_common_birdies = open(common_birdies_filename, 'r')
                file_birds          = open(birds_filename, 'a') 
                for line in file_common_birdies:
                        file_birds.write(line)
                file_birds.close()
                
                cmd_makezaplist = "makezaplist.py %s" % (birds_filename)
                if flag_verbose==1:
                        print "***********************************************"; sys.stdout.flush()
                        print "Doing execute_and_log"; sys.stdout.flush()
                        print "cmd_makezaplist = ", cmd_makezaplist; sys.stdout.flush()
                execute_and_log(cmd_makezaplist, out_dir, log_abspath, dict_env, 0)
                if flag_verbose==1:
                        print "Done execute_and_log!"; sys.stdout.flush()
                        print "***********************************************"

        else:
                if flag_verbose==1:
                        print "Zaplist for %s already exists!" % (fft_infile)

        zaplist_filename = fft_infile.replace(".fft", ".zaplist")


def rednoise(fftfile, out_dir, log_dir, LOG_basename, other_flags="", presto_env=os.environ['PRESTO'], flag_verbose=0):
        #print "rednoise:: Inside rednoise"
        fftfile_nameonly = os.path.basename(fftfile)
        fftfile_basename = os.path.splitext(fftfile_nameonly)[0]
        log_abspath = "%s/LOG_%s.txt" % (log_dir, LOG_basename)
        

        dereddened_ffts_filename = "%s/dereddened_ffts.txt" % (out_dir)
        fftfile_rednoise_abspath = os.path.join(out_dir, "%s_red.fft" % (fftfile_basename) )
        inffile_original_abspath = os.path.join(out_dir, "%s.inf" % (fftfile_basename) )
        
        
        cmd_rednoise = "rednoise %s %s" % (other_flags, fftfile)


        if flag_verbose==1:
                print "rednoise:: dereddened_ffts_filename = ", dereddened_ffts_filename
                print "rednoise:: fftfile_rednoise_abspath = ", fftfile_rednoise_abspath
                print "rednoise:: cmd_rednoise = ", cmd_rednoise
                #print "%s | Running:" % (datetime.datetime.now()).strftime("%Y/%m/%d  %H:%M"); sys.stdout.flush()
                #print "%s" % (cmd_rednoise) ; sys.stdout.flush()
                print "rednoise:: opening '%s'" % (dereddened_ffts_filename)

        try:
                file_dereddened_ffts = open(dereddened_ffts_filename, 'r')
        except:
                if flag_verbose==1:           print "rednoise:: File '%s' does not exist. Creating it..." % (dereddened_ffts_filename), ; sys.stdout.flush()
                os.mknod(dereddened_ffts_filename)
                if flag_verbose==1:           print "done!", ; sys.stdout.flush()
                file_dereddened_ffts = open(dereddened_ffts_filename, 'r')

        # If the fftfile is already in the list of dereddened files...
        if "%s\n" % (fftfile) in file_dereddened_ffts.readlines():
                if flag_verbose==1:
                        print "rednoise:: NB: File '%s' is already in the list of dereddened files (%s)." % (fftfile, dereddened_ffts_filename)
                        # Then check is the file has size > 0...
                        print "rednoise:: Checking the size of '%s'" % (fftfile)

                if (os.path.getsize(fftfile) > 0):
                        operation="skip"
                        if flag_verbose==1:
                                print "rednoise:: size is > 0. Then skipping..."
                else:
                        operation="make_from_scratch"
                        if flag_verbose==1:
                                print "rednoise:: size is = 0. Making from scratch..."

        else:
                operation="make_from_scratch"
                if flag_verbose==1:
                        print "rednoise:: File '%s' IS NOT in the list of dereddened files (%s). I will make the file from scratch..." % (fftfile_basename, dereddened_ffts_filename)

                
        file_dereddened_ffts.close()

        if operation=="make_from_scratch":
                if flag_verbose==1:
                        print "rednoise:: making the file from scratch..."
                dict_env = {'PRESTO': presto_env, 'PATH': "%s/bin:%s" % (presto_env, os.environ['PATH']), 'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env, os.environ['LD_LIBRARY_PATH'])}
                execute_and_log(cmd_rednoise, out_dir, log_abspath, dict_env, 0)
                print "done!", ; sys.stdout.flush()
                file_dereddened_ffts = open(dereddened_ffts_filename, 'a')
                file_dereddened_ffts.write("%s\n" % (fftfile))
                file_dereddened_ffts.close()
                os.rename(fftfile_rednoise_abspath, fftfile_rednoise_abspath.replace("_red.", "."))

                



def realfft(infile, log_dir, out_dir, LOG_basename, other_flags="", presto_env=os.environ['PRESTO'], flag_verbose=0, flag_LOG_append=0):
        infile_nameonly = os.path.basename(infile)
        infile_basename = os.path.splitext(infile_nameonly)[0]
        log_abspath = "%s/LOG_%s.txt" % (log_dir, LOG_basename)
        fftfile_abspath = os.path.join(out_dir, "%s.fft" % (infile_basename) )
        cmd_realfft = "realfft %s %s" % (other_flags, infile)
        if flag_verbose==1:
                print "%s | realfft:: Running:" % (datetime.datetime.now()).strftime("%Y/%m/%d  %H:%M"); sys.stdout.flush()
                print "%s" % (cmd_realfft) ; sys.stdout.flush()

        if os.path.exists( fftfile_abspath ) and (os.path.getsize(fftfile_abspath) > 0):
                if flag_verbose==1:
                        print "WARNING: File %s already present. Skipping realfft..." % (fftfile_abspath)
        else:
                dict_env = {'PRESTO': presto_env, 'PATH': "%s/bin:%s" % (presto_env, os.environ['PATH']), 'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env, os.environ['LD_LIBRARY_PATH'])}
                execute_and_log(cmd_realfft, out_dir, log_abspath, dict_env, 0)
                if os.path.exists( fftfile_abspath ) and (os.stat(fftfile_abspath).st_size > 0):
                        if flag_verbose==1:
                                print "%s | realfft on \"%s\" completed successfully!" % (datetime.datetime.now().strftime("%Y/%m/%d  %H:%M"), infile_nameonly); sys.stdout.flush()
                else:
                        print "WARNING (%s) | could not find all the output files from realfft on \"%s\"!" % (datetime.datetime.now().strftime("%Y/%m/%d  %H:%M"), infile_nameonly); sys.stdout.flush()
                
        

# PREPDATA
def prepdata(infile, out_dir, LOG_basename, DM, Nsamples=0, mask="", downsample_factor=1, reference="barycentric", other_flags="", presto_env=os.environ['PRESTO'], flag_verbose=0):
        infile_nameonly = os.path.basename(infile)
        infile_basename = os.path.splitext(infile_nameonly)[0]
        log_abspath = "%s/LOG_%s.txt" % (LOG_dir, LOG_basename)
        #file_log = open(log_abspath, "w"); file_log.close()        
        outfile_basename = "%s_DM%05.2f" % (infile_basename, np.float(DM))
        datfile_abspath = os.path.join(out_dir, "%s.dat" % (outfile_basename))
        inffile_abspath = os.path.join(out_dir, "%s.inf" % (outfile_basename))


        if reference=="topocentric":
                flag_nobary = "-nobary "
        elif reference=="barycentric":
                flag_nobary = ""
        else:
                print "ERROR: Invalid value for barycentering option: \"%s\"" % (reference)
                exit()

        if Nsamples >= 0:
                flag_numout = "-numout %d " % ( make_even_number(int(Nsamples/np.float(downsample_factor))) )
        else:
                flag_numout = ""

        if mask!="":
                flag_mask = "-mask %s " % (mask)
        else:
                flag_mask = ""


        cmd_prepdata = "prepdata -o %s %s %s%s%s -dm %s -downsamp %s %s" % (outfile_basename, flag_numout, flag_mask, flag_nobary, other_flags, str(DM), downsample_factor, infile )

        if flag_verbose==1:
                print "%s | Running:" % (datetime.datetime.now()).strftime("%Y/%m/%d  %H:%M"); sys.stdout.flush()
                print "%s" % (cmd_prepdata) ; sys.stdout.flush()
        


        if os.path.exists( datfile_abspath ) and os.path.exists( inffile_abspath):
                if flag_verbose==1:
                        print "WARNING: File %s.dat and %s.inf already present. Skipping, checking results and setting SQL entry accordingly..." % (outfile_basename, outfile_basename)
        else:
                dict_env = {'PRESTO': presto_env, 'PATH': "%s/bin:%s" % (presto_env, os.environ['PATH']), 'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env, os.environ['LD_LIBRARY_PATH'])}

                execute_and_log(cmd_prepdata, out_dir, log_abspath, dict_env, 0)
                if os.path.exists( datfile_abspath ) and os.path.exists( inffile_abspath):
                        if flag_verbose==1:
                                print "%s | prepdata on \"%s\" completed successfully!" % (datetime.datetime.now().strftime("%Y/%m/%d  %H:%M"), infile_nameonly); sys.stdout.flush()
                else:
                        print "WARNING (%s) | could not find all the output files from prepdata on \"%s\"!" % (datetime.datetime.now().strftime("%Y/%m/%d  %H:%M"), infile_nameonly); sys.stdout.flush()
                        



def make_rfifind_mask(infile, out_dir, LOG_basename, time_interval=2.0, intfrac=0.3, chanfrac=0.7, chans_to_zap="", other_flags="", presto_env=os.environ['PRESTO'], flag_verbose=0):
        infile_nameonly = os.path.basename(infile)
        infile_basename = os.path.splitext(infile_nameonly)[0]

        log_abspath = "%s/LOG_%s.txt" % (LOG_dir, LOG_basename)

        cmd_rfifind = "rfifind %s -o %s -intfrac %s -chanfrac %s -zapchan %s -time %s %s" % (other_flags, infile_basename, intfrac, chanfrac, chans_to_zap, time_interval, infile)
        if flag_verbose==1:
                print "%s | Running:" % (datetime.datetime.now()).strftime("%Y/%m/%d  %H:%M"); sys.stdout.flush()
                print "%s" % (cmd_rfifind) ; sys.stdout.flush()
    
        flag_files_present = check_rfifind_outfiles(out_dir, infile_basename)
        
        print "\033[1m >> TIP:\033[0m Check rfifind progress with '\033[1mtail -f %s\033[0m'" % (log_abspath)
        
        if flag_files_present == True:
                if flag_verbose==1:
                        print "WARNING: File %s_rfifind.mask already present. Skipping and checking results..." % (infile_nameonly)
        else:
                dict_env = {'PRESTO': presto_env, 'PATH': "%s/bin:%s" % (presto_env, os.environ['PATH']), 'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env, os.environ['LD_LIBRARY_PATH'])}
                
                execute_and_log(cmd_rfifind, out_dir, log_abspath, dict_env, 0)

                if check_rfifind_outfiles(out_dir, infile_basename) == True:
                        if flag_verbose==1:
                                print "%s | rfifind on \"%s\" completed successfully!" % (datetime.datetime.now().strftime("%Y/%m/%d  %H:%M"), infile_nameonly); sys.stdout.flush()
                else:
                        print "WARNING (%s) | could not find all the output files from rfifind on \"%s\"!" % (datetime.datetime.now().strftime("%Y/%m/%d  %H:%M"), infile_nameonly); sys.stdout.flush()
                        raise Exception("Your STEP_RFIFIND flag is set to 0, but the rfifind files could not be found!")

        
        mask_file = "%s/%s_rfifind.mask" % (out_dir, infile_basename)
        result = get_rfifind_result(mask_file, log_abspath, flag_verbose)


def get_DD_scheme_from_DDplan_output(output_DDplan):
        list_dict_schemes = []

        output_DDplan_list_lines = output_DDplan.split("\n")
        index = output_DDplan_list_lines.index("  Low DM    High DM     dDM  DownSamp   #DMs  WorkFract")   +1

        #print
        #print "+++++++++++++++++++++++++++++++++"
        #print "type(output_DDplan):", type(output_DDplan)
        print output_DDplan
        #print output_DDplan_list_lines
        #print "+++++++++++++++++++++++++++++++++"

        flag_add_plans = 1
        while flag_add_plans == 1:
                if output_DDplan_list_lines[index] == "":
                        return list_dict_schemes
                else:
                        param = output_DDplan_list_lines[index].split()
                        low_DM      = np.float(param[0])
                        high_DM     = np.float(param[1])
                        dDM         = np.float(param[2])
                        downsamp    = int(param[3])
                        num_DMs     = int(param[4])
                        
                        if num_DMs > 1000 and num_DMs <= 2000:
                                diff_num_DMs = num_DMs - 1000
                                low_DM1   = low_DM
                                high_DM1  = low_DM + 1000*dDM
                   
                                low_DM2   = high_DM1
                                high_DM2  = high_DM
                                
                                dict_scheme1 = {'loDM': low_DM1, 'highDM': high_DM1, 'dDM': dDM, 'downsamp': downsamp, 'num_DMs': 1000 }
                                dict_scheme2 = {'loDM': low_DM2, 'highDM': high_DM2, 'dDM': dDM, 'downsamp': downsamp, 'num_DMs': diff_num_DMs }
                                list_dict_schemes.append(dict_scheme1)
                                list_dict_schemes.append(dict_scheme2)
                        else:
                                dict_scheme = {'loDM': low_DM, 'highDM': high_DM, 'dDM': dDM, 'downsamp': downsamp, 'num_DMs': num_DMs }
                                list_dict_schemes.append(dict_scheme)

                index = index + 1
                

def check_prepsubband_result(work_dir, list_DD_schemes, flag_verbose=1):
        N_schemes = len(list_DD_schemes)
        if flag_verbose==1:
                print "check_prepsubband_result:: list_DD_schemes = ", list_DD_schemes
                print "check_prepsubband_result:: work_dir = ", work_dir

        for i in range(N_schemes):
                for dm in np.arange(list_DD_schemes[i]['loDM'],   list_DD_schemes[i]['highDM'] - 0.5*list_DD_schemes[i]['dDM']        , list_DD_schemes[i]['dDM']):    
                        if flag_verbose==1:
                                print "check_prepsubband_result:: Looking for: ", os.path.join(work_dir, "*DM%.2f.dat"%(dm) ),  os.path.join(work_dir, "*DM%.2f.inf"%(dm) ) 
                                print "check_prepsubband_result:: This is what I found: %s, %s" % (  [ x for x in glob.glob(os.path.join(work_dir, "*DM%.2f.dat"%(dm))) if not "_red" in x]  , [ x for x in glob.glob(os.path.join(work_dir, "*DM%.2f.inf"%(dm))) if not "_red" in x]    )
                        if len( [ x for x in glob.glob(os.path.join(work_dir, "*DM%.2f.dat"%(dm))) if not "_red" in x]   + [ x for x in glob.glob(os.path.join(work_dir, "*DM%.2f.inf"%(dm))) if not "_red" in x] ) != 2:
                                if flag_verbose==1:
                                        print "check_prepsubband_result: False"
                                return False     
        if flag_verbose==1:
                print "check_prepsubband_result: True"

        return True

def get_DDplan_scheme(infile, out_dir, LOG_basename, loDM, highDM, DM_coherent_dedispersion, freq_central_MHz, bw_MHz, nchan, nsubbands, t_samp_s):
        infile_nameonly = os.path.basename(infile)
        infile_basename = os.path.splitext(infile_nameonly)[0]
        log_abspath = "%s/LOG_%s.txt" % (LOG_dir, LOG_basename)
 
        print "DM_coherent_dedispersion == ", DM_coherent_dedispersion
        if np.float(DM_coherent_dedispersion) == 0:
                # Implement subbanding calculation!
                #cmd_DDplan = "DDplan.py -o ddplan_%s -l %s -d %s -f %s -b %s -n %s -s %s -t %s" % (infile_basename, loDM, highDM, freq_central_MHz, np.fabs(bw_MHz), nchan, nsubbands, t_samp_s)
                cmd_DDplan = "DDplan.py -o ddplan_%s -l %s -d %s -f %s -b %s -n %s -t %s" % (infile_basename, loDM, highDM, freq_central_MHz, np.fabs(bw_MHz), nchan, t_samp_s)
        elif np.float(DM_coherent_dedispersion) > 0:
                # Implement subbanding calculation!
                #cmd_DDplan = "DDplan.py -o ddplan_%s -l %s -d %s -c %s -f %s -b %s -n %s -s %s -t %s" % (infile_basename, loDM, highDM, DM_coherent_dedispersion, freq_central_MHz, np.fabs(bw_MHz), nchan, nsubbands, t_samp_s)
                cmd_DDplan = "DDplan.py -o ddplan_%s -l %s -d %s -c %s -f %s -b %s -n %s -t %s" % (infile_basename, loDM, highDM, DM_coherent_dedispersion, freq_central_MHz, np.fabs(bw_MHz), nchan, t_samp_s)
                print "Coherent dedispersion enabled. Command would be: ", cmd_DDplan
        elif np.float(DM_coherent_dedispersion) < 0:
                print "ERROR: The DM of coherent dedispersion < 0! Exiting..."
                exit()

        print "Running: %s " % (cmd_DDplan)
        output_DDplan    = get_command_output(cmd_DDplan, shell_state=False, work_dir=out_dir)

        list_DD_schemes  = get_DD_scheme_from_DDplan_output(output_DDplan)


        return list_DD_schemes




def dedisperse(infile, out_dir, LOG_basename, segment_label, chunk_label, Nsamples, mask_file, list_DD_schemes, nchan, nsubbands=0, other_flags="", presto_env=os.environ['PRESTO'], flag_verbose=0):
        infile_nameonly = os.path.basename(infile)
        infile_basename = os.path.splitext(infile_nameonly)[0]
        dict_env = {'PRESTO': presto_env, 'PATH': "%s/bin:%s" % (presto_env, os.environ['PATH']), 'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env, os.environ['LD_LIBRARY_PATH'])}
        log_abspath = "%s/LOG_%s.txt" % (LOG_dir, LOG_basename)
        N_schemes = len(list_DD_schemes)

        string_mask = ""
        if mask_file != "":  string_mask = "-mask %s" % (mask_file)

        print "----------------------------------------------------------------------"
        print "prepsubband will be run %d times with the following DM ranges:" % (N_schemes)
        print
        print "%10s %10s %10s %10s %10s " % ("Low DM", "High DM", "dDM",  "DownSamp",   "#DMs")
        for i in range(N_schemes):
                print "%10s %10s %10s %10s %10s " % (list_DD_schemes[i]['loDM'], np.float(list_DD_schemes[i]['loDM']) + int(list_DD_schemes[i]['num_DMs'])*np.float(list_DD_schemes[i]['dDM']), list_DD_schemes[i]['dDM'] ,  list_DD_schemes[i]['downsamp'],  list_DD_schemes[i]['num_DMs'] )
        print; sys.stdout.flush()

        if nsubbands == 0:
                nsubbands = nchan
        elif (nchan % nsubbands != 0):
                print "ERROR: requested number of subbands is %d, which is not an integer divisor of the number of channels %d! " % (nsubbands, nchan)
                exit()

        print "Dedispersing with %d subbands (original number of channels: %d)..." % (nsubbands, nchan)

        if flag_verbose == 1:
                print "dedisperse::  Checking prepsubband results..."
        if check_prepsubband_result(out_dir, list_DD_schemes, flag_verbose=1) == True:
                print "dedisperse:: WARNING: all the dedispersed time series for %s are already there! Skipping." % (infile_basename)
        else:
                while check_prepsubband_result(out_dir, list_DD_schemes, flag_verbose=1) == False:
                        print "check_prepsubband_result(out_dir, list_DD_schemes, flag_verbose=0) = ", check_prepsubband_result(out_dir, list_DD_schemes, flag_verbose=0)
                        print "Checking results at: %s" % (out_dir)
                        print "\033[1m >> TIP:\033[0m Check prepsubband progress with '\033[1mtail -f %s\033[0m'" % (log_abspath)
                        print
                        for i in range(N_schemes):
                                #if Nsamples >= 0:
                                #        flag_numout = "-numout %d " % (make_even_number(int(Nsamples/np.float(list_DD_schemes[i]['downsamp']))))
                                #else:
                                flag_numout = ""
                                        
                                prepsubband_outfilename = "%s_%s_%s" % (infile_basename, segment_label, chunk_label)
                                cmd_prepsubband = "prepsubband %s %s -o %s %s -lodm %s -dmstep %s -numdms %s -downsamp %s -nsub %s %s" % (other_flags, flag_numout, prepsubband_outfilename, string_mask, list_DD_schemes[i]['loDM'], list_DD_schemes[i]['dDM'], list_DD_schemes[i]['num_DMs'], list_DD_schemes[i]['downsamp'], nsubbands, infile)
                                print "Running prepsubband with scheme %d/%d on observation '%s'..." % (i+1, N_schemes, infile), ; sys.stdout.flush()
                                
                                if flag_verbose == 1:
                                        print "dedisperse:: %d) RUNNING: %s" % (i, cmd_prepsubband)
                                execute_and_log("which prepsubband", out_dir, log_abspath, dict_env, 1)
                                execute_and_log(cmd_prepsubband, out_dir, log_abspath, dict_env, 1)
                                print "done!"; sys.stdout.flush()
                        


def check_zapbirds_outfiles2(zapped_fft_filename, flag_verbose=0):
        zapped_inf_filename = zapped_fft_filename.replace(".fft", ".inf")
        
        if ("zapped" in zapped_fft_filename) and ("zapped" in zapped_inf_filename):
                try:
                        if (os.path.getsize(zapped_fft_filename) > 0) and (os.path.getsize(zapped_inf_filename)>0): #checks if it exists and its size is > 0
                                return True
                        else:
                                return False
                except OSError:
                        return False
        else:
                return False

def check_zapbirds_outfiles(fftfile, list_zapped_ffts_abspath, flag_verbose=0):
        fftfile_nameonly = os.path.basename(fftfile)
        try:
                file_list_zapped_ffts = open(list_zapped_ffts_abspath, 'r')
                if "%s\n" % (fftfile_nameonly) in file_list_zapped_ffts.readlines():
                        if flag_verbose==1:
                                print "check_zapbirds_outfiles:: NB: File '%s' is already in the list of zapped files (%s)." % (fftfile_nameonly, list_zapped_ffts_abspath)
                        if (os.path.getsize(fftfile) > 0):
                                if flag_verbose==1:
                                        print "check_zapbirds_outfiles:: size is > 0. Returning True..."
                                return True
                        else:
                                if flag_verbose==1:
                                        print "rednoise:: size is = 0. Returning False..."
                                return False
                else:
                        if flag_verbose==1:
                                print "check_zapbirds_outfiles:: File '%s' IS NOT in the list of zapped files (%s). I will zap the file from scratch..." % (fftfile_nameonly, list_zapped_ffts_abspath)
                        return False
        except:
                if flag_verbose==1:
                        print "check_zapbirds_outfiles:: File '%s' does not exist. Creating it and returning False..." % (list_zapped_ffts_abspath)
                os.mknod(list_zapped_ffts_abspath)
                return False
        





def zapbirds(fft_infile, zapfile_name, work_dir, log_dir, LOG_basename, presto_env, flag_verbose=0):
        fft_infile_nameonly = os.path.basename(fft_infile)
        fft_infile_basename = os.path.splitext(fft_infile_nameonly)[0]
        inffile_filename = fft_infile.replace(".fft", ".inf")
        log_abspath = "%s/LOG_%s.txt" % (log_dir, LOG_basename)
        #file_log = open(log_abspath, "w"); file_log.close()        
        dict_env = {'PRESTO': presto_env, 'PATH': "%s/bin:%s" % (presto_env, os.environ['PATH']), 'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env, os.environ['LD_LIBRARY_PATH'])}

        cmd_zapbirds = "zapbirds -zap -zapfile %s %s" % (zapfile_name, fft_infile)
        zapped_fft_filename = fft_infile.replace(".fft", "_zapped.fft")
        zapped_inf_filename = inffile_filename.replace(".inf", "_zapped.inf")
        
        list_zapped_ffts_abspath = os.path.join(work_dir, "list_zapped_ffts.txt")
        if flag_verbose==1:
                print "zapbirds:: list_zapped_ffts_abspath = ", list_zapped_ffts_abspath

        if check_zapbirds_outfiles(fft_infile, list_zapped_ffts_abspath, flag_verbose=0) == False:
                if flag_verbose==1:
                        print "Running ZAPBIRDS: %s" % (cmd_zapbirds) ; sys.stdout.flush()
                execute_and_log(cmd_zapbirds, work_dir, log_abspath, dict_env, 0)
                file_list_zapped_ffts = open(list_zapped_ffts_abspath, 'a')
                file_list_zapped_ffts.write("%s\n" % (fft_infile))
                file_list_zapped_ffts.close()

        return zapped_fft_filename, zapped_inf_filename
        

def dedisperse_rednoise_and_periodicity_search_FFT(infile, out_dir, root_workdir, LOG_basename, segment_label, chunk_label, zapfile, Nsamples, mask_file, list_DD_schemes, nchan, subbands=0, other_flags_prepsubband="", presto_env_prepsubband=os.environ['PRESTO'], flag_use_cuda=0, list_cuda_ids=[0], numharm=8, list_zmax=[20], list_wmax=[50],period_to_search_min_s=0.001, period_to_search_max_s=20.0, other_flags_accelsearch="", flag_remove_fftfile=0, presto_env_accelsearch_zmax_0=os.environ['PRESTO'], presto_env_accelsearch_zmax_any=os.environ['PRESTO'], flag_verbose=0, dict_flag_steps = {'flag_step_dedisperse':1 , 'flag_step_realfft': 1, 'flag_step_accelsearch': 1}, n_cores=1):
        infile_nameonly = os.path.basename(infile)
        infile_basename = os.path.splitext(infile_nameonly)[0]

        if flag_verbose==1:
                print "dedisperse_rednoise_and_periodicity_search_FFT:: launching dedisperse"; sys.stdout.flush()
                print "dedisperse_rednoise_and_periodicity_search_FFT:: list_zmax = ", list_zmax
		print "dedisperse_rednoise_and_periodicity_search_FFT:: list_wmax = ", list_wmax


        if dict_flag_steps['flag_step_dedisperse'] == 1:
                if segment_label == "full":
                        dedisperse(infile, out_dir, LOG_basename, segment_label, chunk_label, Nsamples, mask_file, list_DD_schemes, nchan, subbands, other_flags_prepsubband, presto_env_prepsubband, flag_verbose)
                else:
                        print "dedisperse_rednoise_and_periodicity_search_FFT:: segment_label: '%s'" % (segment_label)
                        search_string = "%s/03_DEDISPERSION/%s/full/ck00/*.dat" % (root_workdir, infile_basename) 
                        print "search_string = ", search_string
                        list_datfiles_to_split = glob.glob(search_string)
                        
                        print "list_datfiles_to_split = ", list_datfiles_to_split
                        segment_min = np.float(segment_label.replace("m", ""))
                        i_chunk = int(chunk_label.replace("ck", ""))
                        split_into_chunks(list_datfiles_to_split, LOG_basename, out_dir, segment_min, i_chunk, presto_env=os.environ['PRESTO'], flag_LOG_append=1 )

                if flag_verbose==1:
                        print "dedisperse_rednoise_and_periodicity_search_FFT:: launching periodicity_search_FFT"; sys.stdout.flush()
                        print "dedisperse_rednoise_and_periodicity_search_FFT:: looking for %s/*DM*.dat" % (out_dir)
                        print "dedisperse_rednoise_and_periodicity_search_FFT:: list_cuda_ids = %s" % (list_cuda_ids)
        else:
                if flag_verbose==1:
                        print "dedisperse_rednoise_and_periodicity_search_FFT:: STEP_DEDISPERSE = 0, skipping prepsubband..."
#Fraction of periodicity search included here for convenience in multiprocessing code

	if flag_verbose==1:
                print "periodicity_search_FFT:: Files to search: ", "%s/*DM*.*.dat, excluding red" % (out_dir)
                print "periodicity_search_FFT:: presto_env_zmax_0 = ", presto_env_accelsearch_zmax_0
                print "periodicity_search_FFT:: presto_env_zmax_any = ", presto_env_accelsearch_zmax_any


        list_files_to_search = sorted([ x for x in glob.glob("%s/*DM*.*.dat" % (out_dir)) if not "red" in x ])

	
        frequency_to_search_max = 1./period_to_search_min_s
        frequency_to_search_min = 1./period_to_search_max_s
        if flag_verbose==1:
                print "frequency_to_search_min, ", frequency_to_search_min
                print "frequency_to_search_max, ", frequency_to_search_max

                print "periodicity_search_FFT:: WARNING: -flo and -fhi CURRENTLY DISABLED"
        dict_env_accelsearch_zmax_0   = {'PRESTO': presto_env_accelsearch_zmax_0,   'PATH': "%s/bin:%s" % (presto_env_accelsearch_zmax_0, os.environ['PATH']),   'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env_accelsearch_zmax_0,   os.environ['LD_LIBRARY_PATH'])}
        dict_env_accelsearch_zmax_any = {'PRESTO': presto_env_accelsearch_zmax_any, 'PATH': "%s/bin:%s" % (presto_env_accelsearch_zmax_any, os.environ['PATH']), 'LD_LIBRARY_PATH': "%s/lib:%s" % (presto_env_accelsearch_zmax_any, os.environ['LD_LIBRARY_PATH'])}

        if flag_verbose==1:
                print "periodicity_search_FFT:: dict_env_zmax_0 = ", dict_env_accelsearch_zmax_0
                print "periodicity_search_FFT:: dict_env_zmax_any = ", dict_env_accelsearch_zmax_any
                print "periodicity_search_FFT:: LOG_basename = ", LOG_basename
                print "periodicity_search_FFT:: list_files_to_search = ", list_files_to_search

        log_abspath = "%s/LOG_%s.txt" % (LOG_dir, LOG_basename)
        print "\033[1m >> TIP:\033[0m Follow periodicity search with: \033[1mtail -f %s\033[0m" % (log_abspath)

        zapfile_nameonly = os.path.basename(zapfile)

	dmlistfile="%s/DMlistfiles.txt" % (LOG_dir)
        print "Writing DM list to file: ", dmlistfile
        print "List of dms: ", list_files_to_search
	#I will always just append the dm file since different obs,seg,ck will make this code run every time
        with open(dmlistfile, 'a+') as filetowrite:
                filetowrite.write("\n".join(list_files_to_search))
		filetowrite.write("\n") 
	parametersfile="%s/parametersfile.txt" % (LOG_dir)
	with open(parametersfile, 'w+') as accelpars:
		accelargs = out_dir,LOG_basename, zapfile, flag_use_cuda, list_cuda_ids, numharm, list_zmax, list_wmax, period_to_search_min_s, period_to_search_max_s, other_flags_accelsearch, flag_remove_fftfile, presto_env_accelsearch_zmax_0, presto_env_accelsearch_zmax_any, flag_verbose, 1, dict_flag_steps
		#print(accelargs)
		for a in accelargs:
			#print "type:", type(a)
			accelpars.write(str(a))
			accelpars.write("\n")

	print "FROM HERE ONWARDS I WILL INVOKE THE CALL_ACCEL.PY FUNCTION FIRST WHICH WILL CALL PERIODICITY_SEARCH_FFT FUNCTION HERE AND RUN IT ON DIFFERENT NODES, MAKE SURE YOU HAVE DMlistfile.txt and parametersfile.txt IN LOG FOLDER"
	#/u/tasha/NGC6752/ /home1/NGC6752/meerkat_search.cfg /home1/NGC6752/5mins_t4.fil	
	#return dict_flag_steps

	#FROM HERE ONWARDS I WANT TO INVOKE THE CALL_ACCEL FUNCTION FIRST WHICH WILL CALL PERIODICITY_SEARCH_FFT FUNCTION HERE AND RUN IT ON DIFFERENT NODES
	
#periodicity_search_FFT(work_dir, LOG_basename, zapfile, flag_use_cuda=0, list_cuda_ids=[0], numharm=8, list_zmax=[20], list_wmax=[50], period_to_search_min_s=0.001, period_to_search_max_s=20.0, other_flags_accelsearch="", flag_remove_fftfile=0, presto_env_zmax_0=os.environ['PRESTO'], presto_env_zmax_any=os.environ['PRESTO'], flag_verbose=0, flag_LOG_append=1, dict_flag_steps= {'flag_step_dedisperse':1 , 'flag_step_realfft': 1, 'flag_step_accelsearch': 1}, n_cores = 1):

        #periodicity_search_partial = partial(periodicity_search_FFT, out_dir, LOG_basename, zapfile, flag_use_cuda, list_cuda_ids, numharm, list_zmax, list_wmax, period_to_search_min_s, period_to_search_max_s, other_flags_accelsearch, flag_remove_fftfile, presto_env_accelsearch_zmax_0, presto_env_accelsearch_zmax_any, flag_verbose, 1, dict_flag_steps)

#	use_MPI=0
	
#	if use_MPI==1:
#		arguments = list_files_to_search
#		run_data = None
		# mpi4py
#		comm = MPI.COMM_SELF.Spawn(sys.executable, args=['MPI_slave.py'], maxprocs = len(list_files_to_search))
#		comm.bcast(periodicity_search_partial, root=MPI.ROOT)
#		comm.scatter(list_files_to_search, root=MPI.ROOT)
#		comm.Barrier()
#		run_data = comm.gather(run_data, root=MPI.ROOT)	
	#else:
	#p = Pool(n_cores)
        #p.map(periodicity_search_partial, list_files_to_search)
	#p.close()
	#p.join()
	
	 
	# periodicity_search_FFT(out_dir, LOG_basename, zapfile, flag_use_cuda, list_cuda_ids, numharm, list_zmax, list_wmax, period_to_search_min_s, period_to_search_max_s, other_flags_accelsearch, flag_remove_fftfile, presto_env_accelsearch_zmax_0, presto_env_accelsearch_zmax_any, flag_verbose, 1, dict_flag_steps, n_cores)





class SurveyConfiguration(object):
        def __init__(self, config_filename, obsname=""):
                self.config_filename = config_filename

                config_file = open( config_filename, "r" )

                self.presto_gpu = ""
                
                for line in config_file:
                        if line != "\n" and (not line.startswith("#")): 
                                list_line = shlex.split(line)

                                if   list_line[0] == "FOLDER_DATAFILES":                              self.folder_datafiles                 = list_line[1]
                                elif list_line[0] == "ROOT_WORKDIR":                                  self.root_workdir                     = list_line[1]
                                elif list_line[0] == "FILE_LIST_DATAFILES":                           self.list_datafiles_filename          = list_line[1]
                                elif list_line[0] == "FILE_COMMON_BIRDIES":                           self.file_common_birdies              = list_line[1]
                                elif list_line[0] == "DATA_TYPE":                                     self.data_type                        = list_line[1]
                                elif list_line[0] == "PRESTO":                                        self.presto_env                       = list_line[1]
                                elif list_line[0] == "PRESTO_GPU":                                    self.presto_gpu_env                   = list_line[1]
                                elif list_line[0] == "SEARCH_LABEL":                                  self.search_label                     = list_line[1]
                                elif list_line[0] == "DM_MIN":                                        self.dm_min                           = list_line[1]
                                elif list_line[0] == "DM_MAX":                                        self.dm_max                           = list_line[1]
                                elif list_line[0] == "DM_COHERENT_DEDISPERSION":                      self.dm_coherent_dedispersion         = list_line[1]
                                elif list_line[0] == "N_SUBBANDS":                                    self.nsubbands                        = int(list_line[1])
                                elif list_line[0] == "ACCELSEARCH_LIST_ZMAX":                         self.accelsearch_list_zmax            = [int(x) for x in list_line[1].split(",")]
				elif list_line[0] == "ACCELSEARCH_LIST_WMAX":                         self.accelsearch_list_wmax            = [int(x) for x in list_line[1].split(",")]
                                elif list_line[0] == "ACCELSEARCH_NUMHARM":                           self.accelsearch_numharm              = list_line[1]
                                elif list_line[0] == "ACCELSEARCH_GPU_LIST_ZMAX":                     self.accelsearch_gpu_list_zmax        = [int(x) for x in list_line[1].split(",")]
				elif list_line[0] == "ACCELSEARCH_GPU_LIST_WMAX":                     self.accelsearch_gpu_list_wmax        = [int(x) for x in list_line[1].split(",")]
                                elif list_line[0] == "ACCELSEARCH_GPU_NUMHARM":                       self.accelsearch_gpu_numharm          = list_line[1]
                                elif list_line[0] == "PERIOD_TO_SEARCH_MIN":                          self.period_to_search_min             = np.float(list_line[1])
                                elif list_line[0] == "PERIOD_TO_SEARCH_MAX":                          self.period_to_search_max             = np.float(list_line[1])
				#elif list_line[0] == "ACCELSEARCH_CORES":                             self.accelsearch_cores		    	    = list_line[1]
                                elif list_line[0] == "RFIFIND_CHANS_TO_ZAP":                          self.rfifind_chans_to_zap             = list_line[1]
                                elif list_line[0] == "RFIFIND_TIME_INTERVAL":                         self.rfifind_time_interval            = list_line[1]
                                elif list_line[0] == "RFIFIND_INTFRAC":                               self.rfifind_intfrac                  = list_line[1]
                                elif list_line[0] == "RFIFIND_CHANFRAC":                              self.rfifind_chanfrac                 = list_line[1]
                                elif list_line[0] == "RFIFIND_FLAGS":                                 self.rfifind_flags                    = list_line[1]
                                elif list_line[0] == "PREPDATA_FLAGS":                                self.prepdata_flags                   = list_line[1]
                                elif list_line[0] == "PREPSUBBAND_FLAGS":                             self.prepsubband_flags                = list_line[1]
                                elif list_line[0] == "REALFFT_FLAGS":                                 self.realfft_flags                    = list_line[1]
                                elif list_line[0] == "REDNOISE_FLAGS":                                self.rednoise_flags                   = list_line[1]
                                elif list_line[0] == "ACCELSEARCH_FLAGS":                             self.accelsearch_flags                = list_line[1]
                                elif list_line[0] == "ACCELSEARCH_GPU_FLAGS":                         self.accelsearch_gpu_flags            = list_line[1]
                                elif list_line[0] == "FLAG_REMOVE_FFTFILES":                          self.flag_remove_fftfiles             = list_line[1]
                                elif list_line[0] == "USE_CUDA":                                      self.flag_use_cuda                    = int(list_line[1])
                                elif list_line[0] == "CUDA_IDS":                                      self.list_cuda_ids                    = [int(x) for x in list_line[1].split(",")]
                                elif list_line[0] == "LIST_SEGMENTS":                                 self.list_segments                    = list_line[1].split(",")
                                elif list_line[0] == "THREADS_MAX":                                   self.threads_max                      = int(list_line[1])
                                elif list_line[0] == "THREADS_MAX_FOLDING":                           self.threads_max_folding              = int(list_line[1])
                                elif list_line[0] == "SIFTING_FLAG_REMOVE_DUPLICATES" :               self.sifting_flag_remove_duplicates   = int(list_line[1])
                                elif list_line[0] == "SIFTING_FLAG_REMOVE_DM_PROBLEMS" :              self.sifting_flag_remove_dm_problems  = int(list_line[1])
                                elif list_line[0] == "SIFTING_FLAG_REMOVE_HARMONICS" :                self.sifting_flag_remove_harmonics    = int(list_line[1])
                                elif list_line[0] == "SIFTING_MINIMUM_NUM_DMS" :                      self.sifting_minimum_num_DMs          = int(list_line[1])
                                elif list_line[0] == "SIFTING_MINIMUM_DM" :                           self.sifting_minimum_DM               = np.float(list_line[1])
                                elif list_line[0] == "SIFTING_SIGMA_THRESHOLD" :                      self.sifting_sigma_threshold          = np.float(list_line[1])
                                elif list_line[0] == "PREPFOLD_FLAGS" :                               self.prepfold_flags                   = list_line[1]
                                elif list_line[0] == "FLAG_FOLD_TIMESERIES" :                         self.flag_fold_timeseries             = int(list_line[1])
                                elif list_line[0] == "FLAG_FOLD_RAWDATA" :                            self.flag_fold_rawdata                = int(list_line[1])
                                elif list_line[0] == "STEP_RFIFIND":                                  self.flag_step_rfifind                = int(list_line[1])
                                elif list_line[0] == "STEP_ZAPLIST":                                  self.flag_step_zaplist                = int(list_line[1])
                                elif list_line[0] == "STEP_ZAPLIST_PREPDATA":                         self.flag_step_zaplist_prepdata       = int(list_line[1])
                                elif list_line[0] == "STEP_ZAPLIST_REALFFT":                          self.flag_step_zaplist_realfft        = int(list_line[1])
                                elif list_line[0] == "STEP_ZAPLIST_REDNOISE":                         self.flag_step_zaplist_rednoise       = int(list_line[1])
                                elif list_line[0] == "STEP_ZAPLIST_ACCELSEARCH":                      self.flag_step_zaplist_accelsearch    = int(list_line[1])
                                elif list_line[0] == "STEP_ZAPLIST_ZAPFFT":                           self.flag_step_zaplist_zapfft         = int(list_line[1])
                                elif list_line[0] == "STEP_DEDISPERSE":                               self.flag_step_dedisperse             = int(list_line[1])
                                elif list_line[0] == "STEP_REALFFT":                                  self.flag_step_realfft                = int(list_line[1])
                                elif list_line[0] == "STEP_ACCELSEARCH":                              self.flag_step_accelsearch            = int(list_line[1])
                                elif list_line[0] == "STEP_ACCELSEARCH_GPU":                          self.flag_step_accelsearch_gpu        = int(list_line[1])
                                elif list_line[0] == "STEP_SIFTING":                                  self.flag_step_sifting                = int(list_line[1])
                                elif list_line[0] == "STEP_FOLDING":                                  self.flag_step_folding                = int(list_line[1])
#                               elif list_line[0] == "" :  self.  = list_line[1]

                config_file.close()

                self.log_filename               = "%s.log" % (self.search_label)
                
                if obsname=="":
                        self.list_datafiles             = self.get_list_datafiles(self.list_datafiles_filename)
                else:
                        print 
                        print "//////////////////////////////////////////////////////////////////////////////////"
                        self.list_datafiles             = [obsname]
                        
                print "Observations to search:"
                for j in range(len(self.list_datafiles)):
                        print "%2d) %s" % (j+1, self.list_datafiles[j])
                print
                print "//////////////////////////////////////////////////////////////////////////////////"

                self.list_datafiles_abspath     = [os.path.join(self.folder_datafiles, x) for x in self.list_datafiles]
                self.list_Observations          = [Observation(x, self.data_type) for x in self.list_datafiles_abspath]

                self.list_0DM_datfiles          = []
                self.list_0DM_fftfiles          = []
                self.list_0DM_fftfiles_rednoise = []
                self.list_segments_nofull       = copy.deepcopy(self.list_segments)
 		if "full" in self.list_segments_nofull: 
			self.list_segments_nofull.remove("full")
                self.dict_chunks                = {}      # {'filename': {'20m':   [0,1,2]}}
                self.dict_search_structure      = {}
                if self.presto_gpu_env == "":
                        self.presto_gpu_env = self.presto_env

                
        def get_list_datafiles(self, list_datafiles_filename):
                list_datafiles_file = open( list_datafiles_filename, "r" )
                list_datafiles = [ line.split()[0] for line in list_datafiles_file if not line.startswith("#") ] #Skip commented line
                list_datafiles_file.close()
                print "get_list_datafiles:: list_datafiles = ", list_datafiles
                
                return list_datafiles
                        
        def print_configuration(self):
                print "%40s: %-30s" % ("FOLDER_DATAFILES", self.folder_datafiles)
                print "%40s: %-30s" % ("ROOT_WORKDIR", self.root_workdir)
                print "%40s: %-30s" % ("FILE_LIST_DATAFILES", self.list_datafiles_filename)
                print "%40s: %-30s" % ("SEARCH_LABEL", self.search_label )
                print "%40s: %-30s" % ("DM_MIN", self.dm_min)
                print "%40s: %-30s" % ("DM_MAX", self.dm_max)
                print "%40s: %-30s" % ("ACCELSEARCH_GPU_LIST_ZMAX", self.accelsearch_gpu_list_zmax)
		print "%40s: %-30s" % ("ACCELSEARCH_GPU_LIST_WMAX", self.accelsearch_gpu_list_wmax)
                #print "%40s: %-30s" % ("ACCELSEARCH_CORES", self.accelsearch_cores)
		print "%40s: %-30s" % ("ACCELSEARCH_NUMHARM", self.accelsearch_numharm)
		print "%40s: %-30s" % ("PERIOD_TO_SEARCH_MIN", self.period_to_search_min)
                print "%40s: %-30s" % ("PERIOD_TO_SEARCH_MAX", self.period_to_search_max)
                print "%40s: %-30s" % ("SIFTING_FLAG_REMOVE_DUPLICATES", self.sifting_flag_remove_duplicates)
                print "%40s: %-30s" % ("N_SUBBANDS", self.nsubbands)

if __name__ == '__main__':

	flag_verbose = 1
	current_running_threads = 0
	obsname = ""
	#ARGOMENTI DA SHELL
	if (len(sys.argv) == 1):
		print "Usage: %s -config <configfile.cfg> -obs <observation_name>"  % (os.path.basename(sys.argv[0]))
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

	print
	print "#"*62
	print "#" + " "*20 + "%s" % ("JERK SEARCH PIPELINE") + " "*20 + "#"
	print "#" + " "*22 + "%s" % (string_version) + " "*23 + "#"
	print "#"*62
	print

	config                  = SurveyConfiguration(config_filename, obsname)

	print "SURVEY CONFIGURATION:"
	config.print_configuration()

	sifting.sigma_threshold = config.sifting_sigma_threshold

	if not os.path.exists(config.root_workdir):
		os.mkdir(config.root_workdir)


	print "SIFTING.sigma_threshold = ", sifting.sigma_threshold

	LOG_dir = os.path.join(config.root_workdir, "LOG")

	for i in range(len(config.list_Observations)):
		config.dict_search_structure[config.list_Observations[i].file_basename] = {}
		for s in config.list_segments:
			if flag_verbose==1:
				print "s = %s of %s" % (s, config.list_segments)
			if s == "full":
				segment_length_s      = config.list_Observations[i].T_obs_s
				segment_length_min    = config.list_Observations[i].T_obs_s /60.
				segment_label         = s
			else:
				segment_length_min  = np.float(s)
				segment_length_s    = np.float(s) * 60
				segment_label = "%dm" % (segment_length_min)
			config.dict_search_structure[config.list_Observations[i].file_basename][segment_label] = {}
			N_chunks = int(config.list_Observations[i].T_obs_s / segment_length_s)
			
			if flag_verbose==1:
				print
				print "Obs: %s" % (config.list_Observations[i].file_nameonly)
				print "Segment: %s" % (s)
				print "Length Obs = %.2f s" % (config.list_Observations[i].T_obs_s)
				print "N_chunks = %d" % (N_chunks)
			for ck in range(N_chunks):
				print "chunk_label = ck%02d" % (ck)
				chunk_label = "ck%02d" % (ck)
				config.dict_search_structure[config.list_Observations[i].file_basename][segment_label][chunk_label] = {'candidates': [] }
				
				
	print "config.dict_search_structure:"
	print config.dict_search_structure


	#For accelsearch using Hercules 1 node, 24 cores
	num_cores = 24

	print
	print "##################################################################################################"
	print "                                           STEP 1 - RFIFIND                                       "
	print "##################################################################################################"
	print


	if config.flag_step_rfifind == 1:
		rfifind_masks_dir = os.path.join(config.root_workdir, "01_RFIFIND")

		if not os.path.exists(rfifind_masks_dir):
			os.mkdir(rfifind_masks_dir)
		if not os.path.exists(LOG_dir):
			os.mkdir(LOG_dir)





		TP = ThreadPool(config.threads_max)
		for i in range(len(config.list_Observations)):
			time.sleep(0.2)
			print
			print "Creating rfifind mask of observation %3d/%d: '%s'..." % (i+1, len(config.list_Observations), config.list_Observations[i].file_nameonly) ; sys.stdout.flush()
			LOG_basename = "01_rfifind_%s" % (config.list_Observations[i].file_nameonly)
			

			TP.apply_async( make_rfifind_mask, (config.list_Observations[i].file_abspath,
							    rfifind_masks_dir,
							    LOG_basename,
							    config.rfifind_time_interval,
							    config.rfifind_intfrac,
							    config.rfifind_chanfrac,
							    config.rfifind_chans_to_zap,
							    config.rfifind_flags,
							    config.presto_env,
							    0)
			)
			config.list_Observations[i].mask = "%s/%s_rfifind.mask" % (rfifind_masks_dir, config.list_Observations[i].file_basename)
		TP.close()
		TP.join()

	else:
		print "STEP_RFIFIND = %s" % (config.flag_step_rfifind)
		print "I will skip the RFIFIND step."



	##################################################################################################
	# 2) BIRDIES AND ZAPLIST
	##################################################################################################

	print
	print "##################################################################################################"
	print "                                     STEP 2 - BIRDIES AND ZAPLIST                                 "
	print "##################################################################################################"
	print
	if flag_verbose == 1:
		print "STEP_ZAPLIST = %s" % (config.flag_step_zaplist)
		print "STEP_ZAPLIST_PREPDATA = %s" % (config.flag_step_zaplist_prepdata)
		print "STEP_ZAPLIST_REALFFT = %s" % (config.flag_step_zaplist_realfft)
		print "STEP_ZAPLIST_REDNOISE = %s" % (config.flag_step_zaplist_rednoise)
		print "STEP_ZAPLIST_ACCELSEARCH = %s" % (config.flag_step_zaplist_accelsearch)
		print "STEP_ZAPLIST_ZAPFFT = %s" % (config.flag_step_zaplist_zapfft)
		print

	dir_birdies = os.path.join(config.root_workdir, "02_BIRDIES")

	if config.flag_step_zaplist == 1:
		if config.flag_step_zaplist_prepdata == 1:
			#print "# ====================================================================================="
			#print "# a) Create a 0-DM TOPOCENTRIC time series for each of the files, using the mask."
			#print "# ====================================================================================="
			if not os.path.exists(dir_birdies):
				os.mkdir(dir_birdies)
			prepdata_0DM_TP = ThreadPool(config.threads_max)  
			for i in range(len(config.list_Observations)):
				time.sleep(0.1)
				print
				print "Running prepdata on observation  %3d/%d \"%s\"..." % (i+1, len(config.list_Observations), config.list_Observations[i].file_nameonly), ; sys.stdout.flush()
				LOG_basename = "02a_prepdata_%s" % (config.list_Observations[i].file_nameonly)
				prepdata_0DM_TP.apply_async( prepdata, (config.list_Observations[i].file_abspath,
									dir_birdies,
									LOG_basename,
									0,
									config.list_Observations[i].N_samples,
									config.list_Observations[i].mask,
									1,
									"topocentric",
									config.prepdata_flags,
									config.presto_env,
									0)
				)
				print "done!"; sys.stdout.flush()
				
			prepdata_0DM_TP.close()
			prepdata_0DM_TP.join()


		if config.flag_step_zaplist_realfft == 1:
			#print "# ==============================================="
			#print "# b) Fourier transform all the files"
			#print "# ==============================================="
			#print
			TP = ThreadPool(config.threads_max)

			config.list_0DM_datfiles = glob.glob("%s/*.dat" % dir_birdies)   # Collect the *.dat files in the 02_BIRDIES_FOLDERS
			print
			for i in range(len(config.list_0DM_datfiles)):
				time.sleep(0.1)

				print "Running realfft on observation %3d/%d '%s'..." % (i+1, len(config.list_0DM_datfiles), os.path.basename(config.list_0DM_datfiles[i])), ; sys.stdout.flush()
		
				LOG_basename = "02b_realfft_%s" % (os.path.basename(config.list_0DM_datfiles[i]))
				TP.apply_async( realfft, (config.list_0DM_datfiles[i],
								      dir_birdies,
								      LOG_basename,
								      config.realfft_flags,
								      config.presto_env,
								      0,
								      0)
				)
				print "done!"; sys.stdout.flush()
				
			TP.close()
			TP.join()

		if config.flag_step_zaplist_rednoise == 1:
			#print
			#print "# ==============================================="
			#print "# 02c) Remove rednoise"
			#print "# ==============================================="
			#print
			TP = ThreadPool(config.threads_max)
			config.list_0DM_fftfiles = [x for x in glob.glob("%s/*DM00.00.fft" % dir_birdies) if not "_red" in x ]  # Collect the *.fft files in the 02_BIRDIES_FOLDERS, exclude red files

			#print "len(config.list_0DM_datfiles), len(config.list_0DM_fftfiles) = ", len(config.list_0DM_datfiles), len(config.list_0DM_fftfiles)
			print
			for i in range(len(config.list_0DM_fftfiles)):
				time.sleep(0.1)                        
				print "Running rednoise on observation %3d/%d \"%s\"..." % (i+1, len(config.list_0DM_fftfiles), os.path.basename(config.list_0DM_datfiles[i])) , ; sys.stdout.flush()
				LOG_basename = "02c_rednoise_%s" % (os.path.basename(config.list_0DM_fftfiles[i]))
				TP.apply_async( rednoise, (config.list_0DM_fftfiles[i],
							   dir_birdies,
							   LOG_basename,
							   config.rednoise_flags,
							   config.presto_env,
							   0)
				)
				print "done!"; sys.stdout.flush()
			TP.close()
			TP.join()


		if config.flag_step_zaplist_accelsearch == 1:
			#print
			#print "# ==============================================="
			#print "# 02d) Accelsearch e zaplist"
			#print "# ==============================================="
			#print
			TP = ThreadPool(config.threads_max)
			config.list_0DM_fft_rednoise_files = glob.glob("%s/*_DM00.00.fft" % dir_birdies)
			print
			for i in range(len(config.list_0DM_fft_rednoise_files)):
				time.sleep(0.1)
				print "Making zaplist of observation %3d/%d \"%s\"..." % (i+1, len(config.list_0DM_fft_rednoise_files), os.path.basename(config.list_0DM_datfiles[i])), ; sys.stdout.flush() 
				LOG_basename = "02d_makezaplist_%s" % (os.path.basename(config.list_0DM_fft_rednoise_files[i]))
				TP.apply_async( make_zaplist, (config.list_0DM_fft_rednoise_files[i],
							       dir_birdies,
							       LOG_basename,
							       config.file_common_birdies,
							       2,
							       config.accelsearch_flags,
							       config.presto_env,
							       flag_verbose)
				)
				print "done!"; sys.stdout.flush()
			TP.close()
			TP.join()

	print
	print
	dir_dedispersion = os.path.join(config.root_workdir, "03_DEDISPERSION")
	if config.flag_step_dedisperse == 1:
		print "##################################################################################################"
		print "# 3) DEDISPERSION, DE-REDDENING AND PERIODICITY SEARCH"
		print "##################################################################################################"
		print
		
		if flag_verbose == 1:        print "3) DEDISPERSION DE-REDDENING AND PERIODICITY SEARCH: creating the working directories...",; sys.stdout.flush()
		if not os.path.exists(dir_dedispersion):
			os.mkdir(dir_dedispersion)
		if flag_verbose == 1:        print "done!"

		list_DDplan_scheme = get_DDplan_scheme(config.list_Observations[i].file_abspath,
						       dir_dedispersion,
						       LOG_basename,
						       config.dm_min,
						       config.dm_max,
						       config.dm_coherent_dedispersion,
						       config.list_Observations[i].freq_central_MHz,
						       config.list_Observations[i].bw_MHz,
						       config.list_Observations[i].nchan,
						       config.nsubbands,
						       config.list_Observations[i].t_samp_s)

		################################################################################
		# 1) LOOP OVER EACH OBSERVATION
		# 2)      LOOP OVER THE SEGMENT
		# 3)           LOOP OVER THE CHUNK

		threads_step_3 = config.threads_max
		if flag_verbose == 1:                print "3) DEDISPERSION DE-REDDENING AND PERIODICITY SEARCH: creating the ThreadPool with %d threads..." % (threads_step_3),; sys.stdout.flush()
		TP = ThreadPool(config.threads_max)
		if flag_verbose == 1:                print "done!"; sys.stdout.flush()





		
		dmlistfile="%s/DMlistfiles.txt" % (LOG_dir)
		with open(dmlistfile,'w'): pass
		# 1) LOOP OVER EACH OBSERVATION
		for i in range(len(config.list_Observations)):
			obs = config.list_Observations[i].file_basename
			time.sleep(1.0)
			work_dir_obs = os.path.join(dir_dedispersion, config.list_Observations[i].file_basename)
			if flag_verbose == 1:                print "3) DEDISPERSION, DE-REDDENING AND PERIODICITY SEARCH: creating the working directory %s" % (work_dir_obs),; sys.stdout.flush()
			if not os.path.exists(work_dir_obs):
				os.mkdir(work_dir_obs)
			if flag_verbose == 1:                print "done!"; sys.stdout.flush()




		# 2) LOOP OVER EACH SEGMENT
			if "full" in config.dict_search_structure[obs].keys():
				list_segments = ['full'] + ["%sm" % (x) for x in sorted(config.list_segments_nofull)]
			else:
				list_segments =  ["%sm" % (x) for x in sorted(config.list_segments_nofull)]


			for seg in list_segments:
				print "SEGMENT %s of %s" % (seg, sorted(config.dict_search_structure[obs].keys()))
				work_dir_segment = os.path.join(work_dir_obs, "%s" % seg)
				if flag_verbose == 1:          print "3) DEDISPERSION, DE-REDDENING AND PERIODICITY SEARCH: creating the working directory %s" % (work_dir_segment),; sys.stdout.flush()
				if not os.path.exists(work_dir_segment):
					os.makedirs(work_dir_segment)
				if flag_verbose == 1:                print "done!"; sys.stdout.flush()

		# 3) LOOP OVER THE CHUNK
				for ck in sorted(config.dict_search_structure[obs][seg].keys()):
					print "SEGMENT %s of %s  -- chunk %s of %s" % (seg, sorted(config.dict_search_structure[obs].keys()), ck, sorted(config.dict_search_structure[obs][seg].keys()) )
					work_dir_chunk = os.path.join(work_dir_segment, ck)
					if flag_verbose == 1:        print "3) DEDISPERSION, DE-REDDENING AND PERIODICITY SEARCH: creating the working directory %s" % (work_dir_chunk),; sys.stdout.flush()
					if not os.path.exists(work_dir_chunk):
						os.mkdir(work_dir_chunk)
					if flag_verbose == 1:                print "done!"; sys.stdout.flush()


				
					LOG_basename = "03_prepsubband_and_search_FFT_%s" % (config.list_Observations[i].file_nameonly)
					zapfile = "%s/%s_DM00.00.zaplist" % (dir_birdies, config.list_Observations[i].file_basename)
			

					if flag_verbose == 1:
						print "3) DEDISPERSION, DE-REDDENING AND PERIODICITY SEARCH: Adding file %d) \"%s\" to the queue..." % (i+1, config.list_Observations[i].file_nameonly), ; sys.stdout.flush()
						print "mask::: ", config.list_Observations[i].mask
					
						print
						print "**********************"
						print
						print "config.list_cuda_ids = ", config.list_cuda_ids
						print
						print "config.presto_env = ", config.presto_env
						print "config.presto_gpu_env = ", config.presto_gpu_env
						print "**********************"
				
					
					dict_flag_steps = {'flag_step_dedisperse': config.flag_step_dedisperse , 'flag_step_realfft': config.flag_step_realfft, 'flag_step_accelsearch': config.flag_step_accelsearch}



					dedisperse_rednoise_and_periodicity_search_FFT(config.list_Observations[i].file_abspath,
									work_dir_chunk,
									config.root_workdir,
									LOG_basename,
									seg,
									ck,
									zapfile,
									make_even_number(config.list_Observations[i].N_samples/1.0),
									config.list_Observations[i].mask,
									list_DDplan_scheme,
									config.list_Observations[i].nchan,
									config.nsubbands,
									config.prepsubband_flags,
									config.presto_env,
									config.flag_use_cuda,
									config.list_cuda_ids,
									config.accelsearch_gpu_numharm,
									config.accelsearch_gpu_list_zmax,
									config.accelsearch_gpu_list_wmax,
									config.period_to_search_min, 
									config.period_to_search_max, 
									config.accelsearch_flags, 
									config.flag_remove_fftfiles, 
									config.presto_env,
									config.presto_gpu_env,
									flag_verbose,
									dict_flag_steps, num_cores)
					#subprocess.call(['./working_call.sh', config_filename, config.list_Observations[i])
					print "Inputs for workingcall script: "
					print "Config file: ", config_filename
					print "Fil file: ", config.list_Observations[i].file_abspath
					print "Root working directory ", config.root_workdir
					root_workdir_usr = config.root_workdir.replace("/home1/", "/u/")
					print "user root working directory ", root_workdir_usr
		print "Checking if acceleration search have been performed.."
		try:
                	if (os.path.getsize(config.root_workdir+'/accel_check.txt') > 0):
                        	print "accel_check.txt exists! size of file: ", os.path.getsize(config.root_workdir+'/accel_check.txt')
                	else:
				print "accel_check.txt exist but has has size 0! seems call_accel.py did not work!!"
				exit()
        	except OSError:
				print "accel_check.txt does not exist"
				exit()
	
		TP.close()
		TP.join()



		print "A COMPLETE DM LIST FILE SHOULD HAVE BEEN CREATED AT THIS POINT"
	if config.flag_step_sifting == 1:
		print "##################################################################################################"
		print "# 4) CANDIDATE SIFTING "
		print "##################################################################################################"


		dir_sifting = os.path.join(config.root_workdir, "04_SIFTING")
		if flag_verbose == 1:                    print "4) CANDIDATE SIFTING: creating the working directories...",; sys.stdout.flush()
		if not os.path.exists(dir_sifting):
			os.mkdir(dir_sifting)
		if flag_verbose == 1:                    print "done!"


		dict_candidate_lists = {}

		for i in range(len(config.list_Observations)):
			obs = config.list_Observations[i].file_basename
			print "Sifting candidates for observation %3d/%d '%s'." % (i+1, len(config.list_Observations), obs) 
			print
			for seg in sorted(config.dict_search_structure[obs].keys()):
				work_dir_segment = os.path.join(dir_sifting, config.list_Observations[i].file_basename, "%s" % seg)
				if not os.path.exists(work_dir_segment):
					os.makedirs(work_dir_segment)

				for ck in sorted(config.dict_search_structure[obs][seg].keys()):
					work_dir_chunk = os.path.join(work_dir_segment, ck)
					if not os.path.exists(work_dir_chunk):
						os.makedirs(work_dir_chunk)
						
					LOG_basename = "04_sifting_%s_%s_%s" % (obs, seg, ck)
					work_dir_candidate_sifting = os.path.join(dir_sifting, obs, seg, ck)

					if flag_verbose == 1:        print "4) CANDIDATE SIFTING: creating the working directory %s..." % (work_dir_candidate_sifting),; sys.stdout.flush()
					if not os.path.exists(work_dir_candidate_sifting):
						os.mkdir(work_dir_candidate_sifting)
					if flag_verbose == 1:        print "done!"


					if flag_verbose == 1:
						print "4) CANDIDATE SIFTING: Adding file %d) \"%s\" / chunk %s%s to the queue..." % (i+1, obs, seg, ck), ; sys.stdout.flush()
					if flag_verbose == 1:        print "done!"


					config.dict_search_structure[obs][seg][ck]['candidates'] = sift_candidates( work_dir_chunk,
														    LOG_basename,
														    dir_dedispersion,
														    obs,
														    seg,
														    ck,
														    config.accelsearch_gpu_list_zmax,
														    config.accelsearch_gpu_list_wmax,	
														    config.sifting_flag_remove_duplicates,
														    config.sifting_flag_remove_dm_problems,
														    config.sifting_flag_remove_harmonics,
														    config.sifting_minimum_num_DMs,
														    config.sifting_minimum_DM,
														    config.period_to_search_min,
														    config.period_to_search_max
					) 


	if config.flag_step_folding == 1:
		print "##################################################################################################"
		print "# 5) FOLDING "
		print "##################################################################################################"
		print

		dir_folding = os.path.join(config.root_workdir, "05_FOLDING")
		if flag_verbose == 1: print "5) FOLDING: creating the working directories...",; sys.stdout.flush()
		if not os.path.exists(dir_folding):
			os.mkdir(dir_folding)
		if flag_verbose == 1: print "done!"

		for i in range(len(config.list_Observations)):
			obs = config.list_Observations[i].file_basename
			print "Folding observation '%s'" % (obs)
			print

			work_dir_candidate_folding = os.path.join(dir_folding, config.list_Observations[i].file_basename)
			if flag_verbose == 1:   print "5) CANDIDATE FOLDING: creating the working directory %s..." % (work_dir_candidate_folding),; sys.stdout.flush()
			if not os.path.exists(work_dir_candidate_folding):
				os.mkdir(work_dir_candidate_folding)
			if flag_verbose == 1:   print "done!"
			
			count_candidates_to_fold = 0
			for seg in sorted(config.dict_search_structure[obs].keys()):
				for ck in sorted(config.dict_search_structure[obs][seg].keys()):
					Ncands_to_fold = len(config.dict_search_structure[obs][seg][ck]['candidates'])
					print "%20s  |  %10s  ---> %4d candidates" % (seg, ck, Ncands_to_fold)
					count_candidates_to_fold = count_candidates_to_fold + Ncands_to_fold
			print
			print "TOT = %d candidates" % (count_candidates_to_fold)
			print


			count_folded_ts = 1 
			if config.flag_fold_timeseries == 1:
			       
				


				LOG_basename = "05_folding_%s_timeseries" % (obs)
				print
				print "Folding time series \033[1m >> TIP:\033[0m Follow folding progress with '\033[1mtail -f %s/LOG_%s.txt\033[0m'" % (LOG_dir, LOG_basename)

				ncores_fold = 24
				sema = Semaphore(ncores_fold)
				all_processes = []


				for seg in sorted(config.dict_search_structure[obs].keys()):
					for ck in sorted(config.dict_search_structure[obs][seg].keys()):
						for j in range(len(config.dict_search_structure[obs][seg][ck]['candidates'])):
							candidate = config.dict_search_structure[obs][seg][ck]['candidates'][j]
							
							print "FOLDING CANDIDATE TIMESERIES %d/%d of %s: seg %s / %s..." % (count_folded_ts, count_candidates_to_fold, obs, seg, ck), ; sys.stdout.flush()

							sema.acquire()
							p = Process(target= fold_candidate, args=(sema, work_dir_candidate_folding,
										     LOG_basename,
										     config.list_Observations[i].file_abspath,
										     dir_dedispersion,
										     obs,
										     seg,
										     ck,
										     candidate,
										     config.list_Observations[i].T_obs_s,
										     config.list_Observations[i].mask,
										     config.prepfold_flags,
										     config.presto_env,
										     flag_verbose,
										     1,
										     "timeseries"))
							all_processes.append(p)
							p.start()
							print "done!"
							count_folded_ts = count_folded_ts + 1
				for p in all_processes:
					p.join()



			count_folded_raw = 1 
			if config.flag_fold_rawdata == 1:
				LOG_basename = "05_folding_%s_rawdata" % (obs)
				print
				print "Folding raw data \033[1m >> TIP:\033[0m Follow folding progress with '\033[1mtail -f %s/LOG_%s.txt\033[0m'" % (LOG_dir,LOG_basename)
				
				ncores_fold = 24
				sema = Semaphore(ncores_fold)
				all_processes = []
			

				for seg in sorted(config.dict_search_structure[obs].keys(), reverse=True):
					print "FOLD_RAW = %s of %s" % (seg, sorted(config.dict_search_structure[obs].keys(), reverse=True))
					for ck in sorted(config.dict_search_structure[obs][seg].keys()):
						for j in range(len(config.dict_search_structure[obs][seg][ck]['candidates'])):
							candidate = config.dict_search_structure[obs][seg][ck]['candidates'][j]
							LOG_basename = "05_folding_%s_%s_%s_rawdata" % (obs, seg, ck)
							

							print "FOLDING CANDIDATE RAW %d/%d of %s: seg %s / %s ..." % (count_folded_raw, count_candidates_to_fold, obs, seg, ck), ; sys.stdout.flush()
							sema.acquire()
							p = Process(target= fold_candidate, args=(sema, work_dir_candidate_folding,
										     LOG_basename,
										     config.list_Observations[i].file_abspath,
										     dir_dedispersion,
										     obs,
										     seg,
										     ck,
										     candidate,
										     config.list_Observations[i].T_obs_s,
										     config.list_Observations[i].mask,
										     config.prepfold_flags,
										     config.presto_env,
										     flag_verbose,
										     1,
										     "rawdata"))
							all_processes.append(p)
							p.start()
							print "done!"
							count_folded_raw = count_folded_raw + 1
				for p in all_processes:
					p.join()
		#sortcands(work_dir_candidate_folding, LOG_basename,  "rawdata")


							#fold_candidate(work_dir_candidate_folding,  
							 #                                    LOG_basename, 
							  #                                   config.list_Observations[i].file_abspath,
							   #                                  dir_dedispersion,
							    #                                 obs,
							     #                                seg,
							      #                               ck,
							       #                              config.list_Observations[i].T_obs_s,
								#                             candidate,
								 #                            config.list_Observations[i].mask,
								  #                           config.prepfold_flags,
								   #                          config.presto_env,
								    #                         flag_verbose,
								     #                        1,
								      #                       "rawdata")
							
	#                                                print "done!"

	 #                                               count_folded_raw = count_folded_raw + 1



