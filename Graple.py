#!/usr/bin/env python
import os
from os import listdir
from os.path import isdir, isfile, join
import zipfile, tarfile
import subprocess
import argparse
import logging
import shutil
import time

CONFIG = {
    'SimsPerJob' : '5',
    'RunR' : 'False',
    'Rmain' : 'RunSimulation.R',
    'Rexe' : '/usr/bin/R',
    'LogFile' : 'graple.log',
    'SubmitMode' : 'SingleSubmit',
}

RUNCMD = '''#!/bin/sh\npython Graple.py -r\n'''
#RUNCMD = '''C:\Python27\python.exe Graple.py -r\n'''

SUBMIT = '''universe=vanilla
executable=Condor/run.sh
output=Scratch/sim{JobNumber}.out
error=Scratch/sim{JobNumber}.err
log=Scratch/sim{JobNumber}.log
requirements=(Memory >= 512) && (TARGET.Arch == "X86_64") && (TARGET.OpSys == "LINUX")
transfer_input_files=Scratch/job{JobNumber}.bz2.tar, Graple/Graple.py
transfer_output_files=Results.bz2.tar
transfer_output_remaps="Results.bz2.tar=Results/Results{JobNumber}.bz2.tar"
should_transfer_files=YES
when_to_transfer_output=ON_EXIT
notification=never
Queue {QueueCount}'''

class Graple:
    def __init__(self,):
        self.SetupLogger()
        self.ParseArgs()
        if 'SimRoot' in CONFIG:
            self.top_dir = CONFIG['SimRoot']
        else:
            #self.top_dir, base_dir = os.path.split(os.getcwd())
            self.top_dir = os.getcwd()
        self.ScriptsDir = 'Scripts'
        self.GlmDir = 'GLM'
        self.SimsDir = 'Sims'
        self.CondorDir = 'Condor'
        self.TempDir = 'Scratch'
        self.ResultsDir = 'Results'

        self.ZipBasename = join(self.top_dir, self.TempDir)
        self.ZipBasename = join(self.ZipBasename, 'job')

    def ParseArgs(self,):
        self.parser = argparse.ArgumentParser(prog='Graple', description='Helper program to prep, run & collect sim results')
        self.parser.add_argument('-p', '--prep', dest='prep', action='store_true', help='Creates a series archives each containing a batch of simulations')
        self.parser.add_argument('-r', '--run', dest='run', action='store_true', help='Unpacks archives and executes simulations')
        self.parser.add_argument('-f', '--fixup', dest='fixup', action='store_true', help='Puts the output of each Sim into its orignial source folder')
        self.parser.add_argument('-m', '--mkdirs', dest='mkdirs', action='store_true', help='Creates the working folder structure')
        
    def isCreateWorkingFolders(self):
        if self.parser.parse_args().mkdirs:
            return True

    def isSimPrep(self,):
        arg = self.parser.parse_args()
        if arg.prep or (not arg.prep and not arg.run and not arg.fixup and not arg.mkdirs):
            return True
        else: return False

    def isSimRun(self,):
        if self.parser.parse_args().run:
            return True
        else: return False

    def isSimFixup(self,):
        if self.parser.parse_args().fixup:
            return True
        else: return False

    def SetupLogger(self,):
        self.logger = logging.getLogger('GRAPLE')
        self.logger.setLevel(logging.DEBUG)
        # create file handler which logs even debug messages
        fh = logging.FileHandler(CONFIG['LogFile'])
        fh.setLevel(logging.DEBUG)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s:%(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        # add the handlers to logger
        self.logger.addHandler(ch)
        self.logger.addHandler(fh)

    def CreateWorkingFolders(self, NumSims=0):
        os.chdir(self.top_dir)
        if not isdir(self.ScriptsDir):
            os.mkdir(self.ScriptsDir)
        
        if not isdir(self.GlmDir):
            os.mkdir(self.GlmDir)
        
        if not isdir(self.TempDir):
            os.mkdir(self.TempDir)

        if not isdir(self.CondorDir):
            os.mkdir(self.CondorDir)

        if not isdir(self.SimsDir):
            os.mkdir(self.SimsDir)

        if not isdir(self.ResultsDir):
            os.mkdir(self.ResultsDir)

        os.chdir(self.SimsDir)
        for i in range(NumSims):
            if not isdir('Sim' + str(i)):
                os.mkdir('Sim' + str(i))

    def ZipDir(self, path, archiver):
        for root, dirs, files in os.walk(path):
            for file in files:
                archiver.write(os.path.join(root, file))

    def CreateJob(self, SimDirList, JobName):
        ## Creates a single job ZIP file
        os.chdir(self.top_dir)
        with tarfile.open(JobName, 'w:bz2', compresslevel=9) as tar:
            tar.add(self.ScriptsDir)
            tar.add(self.GlmDir)
            for adir in SimDirList:
                tar.add(adir)
        print JobName + ' created'

    def SubmitAJob(self, JobNum):
        submitFile = open(join(self.CondorDir, 'jobs.submit'), 'w')
        SubmitStr=SUBMIT.format(JobNumber=JobNum, QueueCount=1)   
        submitFile.write(SubmitStr)
        submitFile.close()
        runFile = open(join(self.CondorDir, 'run.sh'), 'w')
        runFile.write(RUNCMD)
        runFile.close()
        print 'Copying simulation {JobNumber} to HTCondor pool.'.format(JobNumber=JobNum)
        subprocess.call(['condor_submit', join(self.CondorDir, 'jobs.submit')])
        
    def SimPrepSingleSubmit(self,):
        print  'Preparing simulations for HTCondor submission...'
        ## Creates the series of job archives
        SimsForJob = []
        count = 0
        jobSuffix = 0
        os.chdir(self.top_dir)
        #build the list of Simxxx dirs to add to the job archives
        for dir in listdir(self.SimsDir):
            fqdn = join(self.SimsDir, dir)
            if isdir(fqdn):
                SimsForJob.append(fqdn)
                count += 1  #count is used to limit how many sims are packed into a job
            if count % int(CONFIG['SimsPerJob']) == 0:
                jn = self.ZipBasename + str(jobSuffix) + '.bz2.tar'
                self.CreateJob(SimsForJob, jn)
                self.SubmitAJob(jobSuffix)
                jobSuffix += 1
                SimsForJob = []
        if len(SimsForJob) > 0:
            jn = self.ZipBasename + str(jobSuffix) + '.bz2.tar'
            self.CreateJob(SimsForJob, jn)
            self.SubmitAJob(jobSuffix)
            jobSuffix += 1
            SimsForJob = []
    
    def SubmitJobs(self, NumberOfJobs):
        submitFile = open(join(self.CondorDir, 'jobs.submit'), 'w')
        SubmitStr=SUBMIT.format(JobNumber='$(Process)', QueueCount=NumberOfJobs) 
        submitFile.write(SubmitStr)
        submitFile.close()
        runFile = open(join(self.CondorDir, 'run.sh'), 'w')
        runFile.write(RUNCMD)
        runFile.close()
        print 'Copying simulations to HTCondor pool.'
        subprocess.call(['condor_submit', join(self.CondorDir, 'jobs.submit')])

    def SimPrepBatchSubmit(self):
        print  'Preparing simulations for HTCondor submission...'
        ## Creates the series of job archives
        SimsForJob = []
        count = 0
        jobSuffix = 0
        os.chdir(self.top_dir)
        #build the list of Simxxx dirs to add to the job archives
        for dir in listdir(self.SimsDir):
            fqdn = join(self.SimsDir, dir)
            if isdir(fqdn):
                SimsForJob.append(fqdn)
                count += 1  #count is used to limit how many sims are packed into a job
            if count % int(CONFIG['SimsPerJob']) == 0:
                jn = self.ZipBasename + str(jobSuffix) + '.bz2.tar'
                self.CreateJob(SimsForJob, jn)
                jobSuffix += 1
                SimsForJob = []
        if len(SimsForJob) > 0:
            jn = self.ZipBasename + str(jobSuffix) + '.bz2.tar'
            self.CreateJob(SimsForJob, jn)
            jobSuffix += 1
            SimsForJob = []
        self.SubmitJobs(jobSuffix)

    def SimPrep(self):
        start = end = 0
        if CONFIG['SubmitMode'] == 'SingleSubmit':
            start = time.clock()
            self.SimPrepSingleSubmit()
            end = time.clock()
        if CONFIG['SubmitMode'] == 'BatchSubmit':
            start = time.clock()
            self.SimPrepBatchSubmit()
            end = time.clock()
        print 'Duration for {0} is {1}'.format(CONFIG['SubmitMode'], end-start)
        #raw_input("Press Enter to continue...")
        
    def SimRun(self,):
        ## Runs a single job on the Condor execute node and packages the
        ## results that are returned to the client.
        rexe = CONFIG['Rexe']
        topdir = os.getcwd()
        glm = join(join(topdir, 'GLM'), 'glm',)
        rscript = join(join(topdir, 'Scripts'), CONFIG['Rmain'])
              
        for JobName in listdir('.'):
            if isfile(JobName) and JobName.endswith('.bz2.tar'):
                with tarfile.open(JobName, 'r') as tar:
                    tar.extractall()
                break
        for d in listdir(self.SimsDir):
            simdir = join(self.SimsDir, d)
            if isdir(simdir):
                os.chdir(simdir)
                os.mkdir('Results')
                subprocess.call("export LD_LIBRARY_PATH=.",shell=True)
                res = subprocess.call([glm])
                if CONFIG['RunR'] == 'True':
                    res = subprocess.call([rexe, rscript])
                for file in os.listdir("."):
                    if (os.path.isdir(file)==False):
                        shutil.copy(os.path.join(os.getcwd(),file),os.path.join(os.getcwd(),'Results'))
                os.chdir(topdir)
        
        with tarfile.open('Results.bz2.tar', 'w:bz2',compresslevel=9) as tar:
            for d in listdir('Sims'):
                #resultsdir = join(join(join(topdir, 'Sims'), d), 'Results')
                resultsdir = join(join('Sims', d), 'Results')
                if isdir(resultsdir):
                    tar.add(resultsdir)


    def SimFixup(self,):
        ## Unpacks the result archives on the client side and cleans 
        ## up unused directories.
        if isdir(join(self.top_dir, self.ResultsDir)):
            os.chdir(join(self.top_dir, self.ResultsDir))
        else :
             self.logger.error('Results directory does not exist')
             #raw_input("Press Enter to continue...")
             return
        for f in listdir('.'):
            try:
                if f.endswith('.bz2.tar'):
                    with tarfile.open(f, 'r') as tar:
                        tar.extractall()
                    print f + ' extracted'
                    os.remove(f)
            except Exception as e:
                self.logger.exception('Filed to open tarfile ' + f)
        if isdir(join(self.top_dir, self.TempDir)):
            shutil.rmtree(join(self.top_dir, self.TempDir))
        if isdir(join(self.top_dir, self.CondorDir)):
            shutil.rmtree(join(self.top_dir, self.CondorDir))
        
        #raw_input("Press Enter to continue...")

if __name__ == '__main__':
    sm = Graple()
    if sm.isCreateWorkingFolders():
        sm.CreateWorkingFolders()
    if sm.isSimPrep():
        sm.SimPrep()
    if sm.isSimRun():
        sm.SimRun()
    if sm.isSimFixup():
        sm.SimFixup()
