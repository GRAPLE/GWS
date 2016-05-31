#!/usr/bin/env python

import os, time, csv, signal, pandas, string, random, shutil, subprocess, tarfile, json, itertools, datetime, flask
import numpy as np
import celery.signals
from flask import Flask, request, redirect, url_for, jsonify, send_from_directory
from werkzeug import secure_filename
from distutils.dir_util import copy_tree
from celery import Celery
from pymongo import MongoClient
from os import listdir
from gwsconfig import gwsconf

# global variables and paths
base_working_path = gwsconf['base_working_path']
 
base_upload_path = os.path.join(base_working_path, 'Experiments')
base_graple_path = os.path.join(base_working_path, 'GRAPLE_SCRIPTS')
base_filter_path = os.path.join(base_working_path, 'Filters')
base_result_path = os.path.join(base_working_path, 'Results')

app = Flask(__name__, static_url_path = '/downloadFile', static_folder = base_result_path)
app.config['CELERY_BROKER_URL'] = 'amqp://'
app.config['CELERY_ACCEPT_CONTENT'] = ['json']
app.config['CELERY_TASK_SERIALIZER'] = 'json'
app.config['CELERY_RESULT_SERIALIZER'] = 'json'
#app.config['CELERY_RESULT_BACKEND'] = 'ampq'

celeryob = Celery(app.name, broker = app.config['CELERY_BROKER_URL'])
celeryob.conf.update(app.config)
db_client = MongoClient(connect = False) # running at default port
db = db_client[gwsconf['graple_db_name']]
collection = db[gwsconf['graple_coll_name']]

def_SimsPerJob = 5 # can automatically read from Graple.py if required
def_id_size = 40

# mongodb document
# { 'key': the uid of the experiment 
#   'status': see status codes below
#   'payload': list with integers? representing condor cluster IDs of experiment jobs
#   'submitted': datetime.datetime object representing time of submission (localtime)
#   'completed': datetime.datetime object representing time of completion
#   'expiry': datetime.datetime object representing when to delete the results
#   'progress': float representing progress of experiment
#   'retention': how long to keep files after completion
# }

# Description of mongodb document 'status' codes
# 1 user submitted experiment - GWS sets before returning to user on submit
# 2 condor submission done - GWS doesn't modify files after setting 2 - only condor places results
# 3 job completed and results consolidated - when all jobs are complete, EMS consolidates results, sets expiry and places output.tar.gz
# 4 job results downloaded - GWS sets 3 to 4 on first download; also sets expiry time if retention == 0
# 5 to delete - GWS sets on Abort. EMS executes condor_rm and sets to 6
# 6 job deleted/expired - FMS sets after deleting the experiment files

@celery.signals.worker_process_init.connect()
def seed_rand(**_): # makes sure different celery workers generate different random numbers
    np.random.seed()

@celeryob.task 
def doTask(task, rscript = None):
    func_dict = { # function pointers are not JSON serializable in celery. use a reference dict instead of eval for safety
            'handle_batch_job':handle_batch_job, 
            'handle_sweep_job':handle_sweep_job, 
            'handle_special_job':handle_special_job, 
            'generate_sweep_job':generate_sweep_job, 
            'generate_special_job':generate_special_job }
    if task[0] in func_dict:
        func_dict[task[0]](task, rscript)
    
def batch_id_generator(size = def_id_size, chars = string.ascii_uppercase + string.digits):
    bid = ''.join(random.choice(chars) for _ in range(size))
    while collection.find_one({'key':bid}) != None:
        bid = ''.join(random.choice(chars) for _ in range(size))
    return bid

def handle_batch_job(task, rscript):
    global base_graple_path, base_filter_path
    topdir = task[1]
    filename = task[2]
    copy_tree(base_graple_path, topdir)
    subprocess.call(['python' , os.path.join(topdir, 'CreateWorkingFolders.py')])
    inputfile = os.path.join(topdir, filename)
    subprocess.call(['tar','xzf', inputfile, '-C', os.path.join(topdir, 'Sims')])
    os.remove(inputfile)
    if(rscript):
        scripts_dir = os.path.join(topdir, 'Scripts')
        rscriptfn = os.path.join(base_filter_path, rscript)
        if os.path.isfile(rscriptfn):
            shutil.copy(rscriptfn, os.path.join(scripts_dir, 'PostProcessFilter.R'))
        filterParamsDir = os.path.join(topdir, 'Sims', 'FilterParams')
        if(os.path.exists(filterParamsDir)):
            shutil.copy(os.path.join(filterParamsDir, 'FilterParams.json'), os.path.join(topdir,'Scripts'))
            shutil.rmtree(filterParamsDir)
    execute_graple(topdir)
    
def execute_graple(topdir, SimsPerJob = def_SimsPerJob, force_gen = False):
    uid = os.path.basename(topdir)
    dbdoc = collection.find_one({'key':uid})
    if dbdoc['status'] == 1: # dont submit if aborted
        if SimsPerJob == def_SimsPerJob and (not force_gen):
            submit_response_string=subprocess.check_output(['python', os.path.join(topdir, 'SubmitGrapleBatch.py')])
        else:
            submit_response_string=subprocess.check_output(['python', os.path.join(topdir, 'SubmitGrapleBatch.py'), str(SimsPerJob)])

        submitIDList=[]
        for i in submit_response_string.split('\n'):
            if 'cluster' in i:
                submitIDList.append(i.split(' ')[5].split('.')[0])
        update_doc = {'payload':submitIDList, 'status':2}
        collection.update_one({'key':uid}, {'$set':update_doc})

def process_graple_results(uid):
    global base_upload_path, base_result_path
    res_dir = os.path.join(base_result_path, uid) # can change uid to random folder name for security
    tarfn = os.path.join(res_dir, 'output.tar.gz')
    if not os.path.isfile(tarfn):
        os.mkdir(res_dir)
        #subprocess.call(['tar', 'czf', tarfn, '-C', os.path.dirname(tarfn)])
        with tarfile.open(tarfn, 'w:gz', compresslevel=9) as tar:
            for f in listdir(os.path.join(base_upload_path, uid, 'Results')):
                if f.endswith('.tar.bz2') or f == 'sim_summary.csv':
                    tar.add(os.path.join(base_upload_path, uid, 'Results', f), f)
                    #os.remove(f) 
    return os.path.join(uid, 'output.tar.gz')
    
def check_Job_status(uid):
    response = {'errors':''}
    query = {'key':uid}
    dbdoc = collection.find_one(query)
    if dbdoc == None:
        response['errors'] += 'JobID ' + uid + ' not found in database'

    if len(response['errors']) > 0:
        return response

    response['curr_status'] = str(dbdoc['progress']) + '% complete'
    return response

def Abort_Job(uid):
    response = {'errors':''}
    query = {'key':uid}
    submitIDList = collection.find_one(query)
    if submitIDList == None:
        response['errors'] += 'JobID ' + uid + ' not found in database'
    else:
        update_doc = {'status':5}
        collection.update_one({'key':uid}, {'$set':update_doc})
        response['curr_status'] = 'All jobs marked for removal'

    return response
        
def ret_distribution_samples(distribution,samples,parameters):
    parameters = map(float,parameters)
        
    if distribution=='uniform':
        return np.random.uniform(parameters[0],parameters[1],samples).tolist()
    elif distribution=='binomial':
        return np.random.binomial(parameters[0],parameters[1],samples).tolist()
    elif distribution=='normal':
        return np.random.normal(parameters[0],parameters[1],samples).tolist()
    elif distribution=='poisson':
        return np.random.poisson(parameters[0],parameters[1]).tolist()
    elif distribution=='linear':
        return np.linspace(parameters[0],parameters[1],samples).tolist()

def generate_sweep_job(task, rscript):
    global base_filter_path, base_graple_path
    exp_root_path = task[1]
    filename = task[2]
    sims_per_job = int(task[3])
    copy_tree(base_graple_path, exp_root_path)
    subprocess.call(['python', os.path.join(exp_root_path, 'CreateWorkingFolders.py')])
    base_folder = os.path.join(exp_root_path, 'base_folder')
    subprocess.call(['tar','xfz', os.path.join(base_folder, filename), '-C', base_folder])
    os.remove(os.path.join(base_folder, filename))
    with open(os.path.join(base_folder, 'job_desc.json')) as data_file:
        jsondata = json.load(data_file)

    if(rscript):
        scripts_dir =  os.path.join(exp_root_path, 'Scripts')
        rscriptfn = os.path.join(base_filter_path, rscript)
        if os.path.isfile(rscriptfn):
            shutil.copy(rscriptfn, os.path.join(scripts_dir, 'PostProcessFilter.R'))
            with open(rscriptfn) as ppfilter_file:
                if 'CONSOLIDATE_COMPATIBLE' in ppfilter_file.read(1000):
                    shutil.move(os.path.join(exp_root_path, 'ConsolidateResults.py'), os.path.join(exp_root_path, 'Results'))
        filterParamsDir = os.path.join(base_folder, 'FilterParams') 
        if(os.path.exists(filterParamsDir)):
            shutil.copy(os.path.join(filterParamsDir, 'FilterParams.json'), scripts_dir)

    summary = []
    columns = {}
    noOfFiles = len(jsondata['ExpFiles'])
    base_iterations = 1
    for i in range(0, noOfFiles):
        base_file = jsondata['ExpFiles'][i]['driverfile']
        variables = jsondata['ExpFiles'][i]['variables'][0]
        columns[base_file] = {}
        for key, value in variables.iteritems():
            variable = key
            var_distribution = ''
            var_start_value = 0
            var_end_value = 0
            var_operation = ''
            var_steps = 0
            if('distribution' in value):
                var_distribution = value['distribution']
            if('operation' in value):
                var_operation = value['operation']
            if('start' in value):
                var_start_value = value['start']
            if('end' in value):
                var_end_value = value['end']
            if('steps' in value):
                var_steps = value['steps']
                var_steps += 1 
                base_iterations *= var_steps
            columns[base_file][variable]=[ret_distribution_samples(var_distribution,var_steps,[var_start_value, var_end_value])]
            columns[base_file][variable].append(var_operation)
            columns[base_file][variable].append(var_distribution)
            columns[base_file][variable].append(var_steps)
    Sims_dir=os.path.join(exp_root_path, 'Sims')
    varcomb = []
    numcomb = []
    sim_no = 1
    for jsonfilename, variables in columns.iteritems():
        for variable, stepValues in variables.iteritems():
            varcomb.append(jsonfilename +',' + variable + ',' + stepValues[2] + ',' + stepValues[1])
            numcomb.append(stepValues[0])

    iterprod = list(itertools.product(*numcomb))
        
    for cc in range(0, len(iterprod), sims_per_job):
        new_dir=os.path.join(Sims_dir, 'Sim' + str(sim_no))
        shutil.copytree(base_folder, new_dir)
        to_pack = [varcomb, iterprod[cc:cc+sims_per_job]]
        with open(os.path.join(new_dir, 'generate.json'), 'w') as pickf:
            json.dump(to_pack, pickf)
        for c in range(len(to_pack[1])):
            row = ['Sim' + str(sim_no) + '_' + str(c+1)]
            for i in range(len(varcomb)):
                var_list = varcomb[i].split(',')
                base_file = var_list[0]
                field = var_list[1]
                operation = var_list[3]
                row.append(field)
                row.append(columns[base_file][field][2]) 
                row.append(columns[base_file][field][1]) 
                row.append(str(to_pack[1][c][i]))
            summary.append(row)
        sim_no += 1
    # write summary of modifications to a file.
    result_summary = open(os.path.join(exp_root_path, 'Results', 'sim_summary.csv'),'wb')
    wr = csv.writer(result_summary,dialect='excel')
    for row in summary:
        wr.writerow(row)
    result_summary.close()

    # execute graple job
    execute_graple(exp_root_path, 1, True) # for a generate job, the bundled sims per job is 1, worker expands the single job into many as per sims_per_job
    return
        
# handles sweep string cases.    
def handle_sweep_job(task, rscript):
    global base_filter_path, base_graple_path
    exp_root_path = task[1]
    filename = task[2]
    copy_tree(base_graple_path, exp_root_path)
    subprocess.call(['python', os.path.join(exp_root_path, 'CreateWorkingFolders.py')])
    base_folder = os.path.join(exp_root_path, 'base_folder')
    subprocess.call(['tar','xfz', os.path.join(base_folder, filename), '-C', base_folder])
    os.remove(os.path.join(base_folder, filename))
    with open(os.path.join(base_folder, 'job_desc.json')) as data_file:
        jsondata = json.load(data_file)

    if(rscript):
        scripts_dir =  os.path.join(exp_root_path, 'Scripts')
        rscriptfn = os.path.join(base_filter_path, rscript)
        if os.path.isfile(rscriptfn):
            shutil.copy(rscriptfn, os.path.join(scripts_dir, 'PostProcessFilter.R'))
            with open(rscriptfn) as ppfilter_file:
                if 'CONSOLIDATE_COMPATIBLE' in ppfilter_file.read(1000):
                    shutil.move(os.path.join(exp_root_path, 'ConsolidateResults.py'), os.path.join(exp_root_path, 'Results'))
        filterParamsDir = os.path.join(base_folder, 'FilterParams') 
        if(os.path.exists(filterParamsDir)):
            shutil.copy(os.path.join(filterParamsDir, 'FilterParams.json'), scripts_dir)

    summary = []
    columns = {}
    noOfFiles = len(jsondata['ExpFiles'])
    base_iterations = 1
    for i in range(0, noOfFiles):
        base_file = jsondata['ExpFiles'][i]['driverfile']
        variables = jsondata['ExpFiles'][i]['variables'][0]
        columns[base_file] = {}
        for key, value in variables.iteritems():
            variable = key
            var_distribution = ''
            var_start_value = 0
            var_end_value = 0
            var_operation = ''
            var_steps = 0
            if('distribution' in value):
                var_distribution = value['distribution']
            if('operation' in value):
                var_operation = value['operation']
            if('start' in value):
                var_start_value = value['start']
            if('end' in value):
                var_end_value = value['end']
            if('steps' in value):
                var_steps = value['steps']
                var_steps += 1 
                base_iterations *= var_steps
            columns[base_file][variable]=[ret_distribution_samples(var_distribution,var_steps,[var_start_value, var_end_value])]
            columns[base_file][variable].append(var_operation)
            columns[base_file][variable].append(var_distribution)
            columns[base_file][variable].append(var_steps)
    Sims_dir=os.path.join(exp_root_path,'Sims')
    varcomb = []
    numcomb = []
    sim_no = 1
    for jsonfilename, variables in columns.iteritems():
        for variable, stepValues in variables.iteritems():
            varcomb.append(jsonfilename +',' + variable + ',' + stepValues[2] + ',' + stepValues[1])
            numcomb.append(stepValues[0])

    for comb in itertools.product(*numcomb):
        new_dir=os.path.join(Sims_dir, 'Sim'+ str(sim_no))
        summary.append(['Sim' + str(sim_no)])
        shutil.copytree(base_folder, new_dir)
        for i in range(len(varcomb)):
            var_list = varcomb[i].split(',')
            base_file = var_list[0]
            field = var_list[1]
            operation = var_list[3]
            delta = comb[i]
            data = pandas.read_csv(os.path.join(new_dir, base_file))
            data = data.rename(columns=lambda x: x.strip())
            if (((' '+field) in data.columns) or (field in data.columns)):
            # handle variations in filed names in csv file, some field names have leading spaces.
                if ' '+field in data.columns:
                    field_modified = ' '+field
                else:
                    field_modified = field
            if (operation=='add'):
                data[field_modified]=data[field_modified].apply(lambda val:val+delta)
            elif (operation=='sub'):
                data[base_file][field_modified]=data[field_modified].apply(lambda val:val-delta)
            elif (operation=='mul'):
                data[field_modified]=data[field_modified].apply(lambda val:val*delta)
            elif (operation=='div'):
                data[field_modified]=data[field_modified].apply(lambda val:val/delta)
            summary[sim_no - 1].append(field)
            summary[sim_no - 1].append(columns[base_file][field][2]) 
            summary[sim_no - 1].append(columns[base_file][field][1]) 
            summary[sim_no - 1].append(str(delta))
            data.to_csv(os.path.join(new_dir, base_file), index=False)
        sim_no += 1
    # write summary of modifications to a file.
    result_summary = open(os.path.join(exp_root_path, 'Results', 'sim_summary.csv'),'wb')
    wr = csv.writer(result_summary,dialect='excel')
    for row in summary:
        wr.writerow(row)
    result_summary.close()

    # execute graple job
    execute_graple(exp_root_path)
    return

def generate_special_job(task, rscript):
    global base_filter_path, base_graple_path
    exp_root_path = task[1]
    filename = task[2]
    sims_per_job = int(task[3])
    copy_tree(base_graple_path, exp_root_path)
    subprocess.call(['python', os.path.join(exp_root_path, 'CreateWorkingFolders.py')])
    base_folder = os.path.join(exp_root_path, 'base_folder')
    subprocess.call(['tar','xfz', os.path.join(base_folder, filename), '-C', base_folder])
    with open(os.path.join(base_folder, 'job_desc.json')) as data_file:    
        jsondata = json.load(data_file)

    if(rscript):
        scripts_dir =  os.path.join(exp_root_path, 'Scripts')
        rscriptfn = os.path.join(base_filter_path, rscript)
        if os.path.isfile(rscriptfn):
            shutil.copy(rscriptfn, os.path.join(scripts_dir, 'PostProcessFilter.R'))
            with open(rscriptfn) as ppfilter_file:
                if 'CONSOLIDATE_COMPATIBLE' in ppfilter_file.read(1000):
                    shutil.move(os.path.join(exp_root_path, 'ConsolidateResults.py'), os.path.join(exp_root_path, 'Results'))
        filterParamsDir = os.path.join(base_folder, 'FilterParams') 
        if(os.path.exists(filterParamsDir)):
            shutil.copy(os.path.join(filterParamsDir, 'FilterParams.json'), scripts_dir)

    summary = []
    columns = {}
    noOfFiles = len(jsondata['ExpFiles']) 
    base_iterations = jsondata['num_iterations']
    for i in range(0, noOfFiles):
        base_file = jsondata['ExpFiles'][i]['driverfile']
        variables = jsondata['ExpFiles'][i]['variables'][0]
        columns[base_file] = {}
        for key, value in variables.iteritems():
            variable = key
            var_distribution = ''
            var_start_value = 0
            var_end_value = 0
            var_operation = ''
            if('distribution' in value):
                var_distribution = value['distribution']
            if('operation' in value):
                var_operation = value['operation']                  
            if('start' in value):
                var_start_value = value['start']
            if('end' in value):
                var_end_value = value['end']    
            columns[base_file][variable]=[ret_distribution_samples(var_distribution,base_iterations,[var_start_value, var_end_value])]
            columns[base_file][variable].append(var_operation)
            columns[base_file][variable].append(var_distribution)

    Sims_dir=os.path.join(exp_root_path, 'Sims')
    varcomb = []
    numcomb = []
    sim_no = 1
    for jsonfilename, variables in columns.iteritems():
        for variable, stepValues in variables.iteritems():
            varcomb.append(jsonfilename + ',' + variable + ',' + stepValues[2] + ',' + stepValues[1])
            numcomb.append(stepValues[0])

    iterprod = list(zip(*numcomb))

    for cc in range(0, len(iterprod), sims_per_job):
        new_dir = os.path.join(Sims_dir, 'Sim' + str(sim_no))
        shutil.copytree(base_folder, new_dir)
        to_pack = [varcomb, iterprod[cc:cc+sims_per_job]]
        with open(os.path.join(new_dir, 'generate.json'), 'w') as pickf:
            json.dump(to_pack, pickf)
        for c in range(len(to_pack[1])):
            row = ['Sim' + str(sim_no) + '_' + str(c+1)]
            for i in range(len(varcomb)):
                var_list = varcomb[i].split(',')
                base_file = var_list[0]
                field = var_list[1]
                operation = var_list[3]
                row.append(field)
                row.append(columns[base_file][field][2]) 
                row.append(columns[base_file][field][1]) 
                row.append(str(to_pack[1][c][i]))
            summary.append(row)
        sim_no += 1
    # write summary of modifications to a file.
    result_summary = open(os.path.join(exp_root_path, 'Results', 'sim_summary.csv'),'wb')
    wr = csv.writer(result_summary,dialect='excel')
    for row in summary:
        wr.writerow(row)
    result_summary.close()

    # execute graple job
    execute_graple(exp_root_path, 1, True)
    return

# handles the core of distribution sweep jobs.
def handle_special_job(task, rscript):
    global base_filter_path, base_graple_path
    exp_root_path = task[1]
    filename = task[2]
    copy_tree(base_graple_path, exp_root_path)
    subprocess.call(['python', os.path.join(exp_root_path, 'CreateWorkingFolders.py')])
    base_folder = os.path.join(exp_root_path,'base_folder')
    subprocess.call(['tar','xfz', os.path.join(base_folder,filename), '-C', base_folder])
    with open(os.path.join(base_folder, 'job_desc.json')) as data_file:    
        jsondata = json.load(data_file)

    if(rscript):
        scripts_dir =  os.path.join(exp_root_path, 'Scripts')
        rscriptfn = os.path.join(base_filter_path, rscript)
        if os.path.isfile(rscriptfn):
            shutil.copy(rscriptfn, os.path.join(scripts_dir, 'PostProcessFilter.R'))
            with open(rscriptfn) as ppfilter_file:
                if 'CONSOLIDATE_COMPATIBLE' in ppfilter_file.read(1000):
                    shutil.move(os.path.join(exp_root_path, 'ConsolidateResults.py'), os.path.join(exp_root_path, 'Results'))
        filterParamsDir = os.path.join(base_folder, 'FilterParams') 
        if(os.path.exists(filterParamsDir)):
            shutil.copy(os.path.join(filterParamsDir, 'FilterParams.json'), scripts_dir)

    summary = []
    columns = {}
    noOfFiles = len(jsondata['ExpFiles']) 
    base_iterations = jsondata['num_iterations']
    for i in range(0, noOfFiles):
        base_file = jsondata['ExpFiles'][i]['driverfile']
        variables = jsondata['ExpFiles'][i]['variables'][0]
        columns[base_file] = {}
        for key, value in variables.iteritems():
            variable = key
            var_distribution = ''
            var_start_value = 0
            var_end_value = 0
            var_operation = ''
            if('distribution' in value):
                var_distribution = value['distribution']
            if('operation' in value):
                var_operation = value['operation']                  
            if('start' in value):
                var_start_value = value['start']
            if('end' in value):
                var_end_value = value['end']    
            columns[base_file][variable]=[ret_distribution_samples(var_distribution,base_iterations,[var_start_value, var_end_value])]
            columns[base_file][variable].append(var_operation)
            columns[base_file][variable].append(var_distribution)

    Sims_dir=os.path.join(exp_root_path,'Sims')                
    for j in range(1,base_iterations+1):
        new_dir = os.path.join(Sims_dir, 'Sim'+ str(j))
        summary.append(['Sim' + str(j)])
        shutil.copytree(base_folder, new_dir)
        for key in columns.keys():
            base_file = key             
            data = pandas.read_csv(os.path.join(new_dir, base_file))
            data = data.rename(columns=lambda x: x.strip())  
            for field in columns[base_file].keys():
                if (((' '+field) in data.columns) or (field in data.columns)):
                # handle variations in filed names in csv file, some field names have leading spaces.
                    if ' '+field in data.columns:
                        field_modified = ' '+field
                    else:
                        field_modified = field
                    delta = columns[base_file][field][0][j-1]
                    if (columns[base_file][field][1]=='add'):
                        data[field_modified]=data[field_modified].apply(lambda val:val+delta)
                    elif (columns[base_file][field][1]=='sub'):
                        data[base_file][field_modified]=data[field_modified].apply(lambda val:val-delta)
                    elif (columns[base_file][field][1]=='mul'):
                        data[field_modified]=data[field_modified].apply(lambda val:val*delta)
                    elif (columns[base_file][field][1]=='div'):
                        data[field_modified]=data[field_modified].apply(lambda val:val/delta)
                    # make note of modified changes in a list datastructure
                    summary[j-1].append(field) # append column_name
                    summary[j-1].append(columns[base_file][field][2]) # append distribution
                    summary[j-1].append(columns[base_file][field][1]) # append operation
                    summary[j-1].append(str(delta)) # append delta
                # at this point the dataframe has been modified, write back to csv.
            data.to_csv(os.path.join(new_dir, base_file), index=False)     
    # write summary of modifications to a file.
    result_summary = open(os.path.join(exp_root_path, 'Results', 'sim_summary.csv'),'wb')
    wr = csv.writer(result_summary,dialect='excel')
    for row in summary:
        wr.writerow(row)
    result_summary.close()

    # execute graple job
    execute_graple(exp_root_path)
    return
        
@app.route('/GrapleRunMetSample', methods= ['POST'])
def special_batch():
    global base_upload_path, base_filter_path
    response = {'errors' : ''}
    if not 'files' in request.files:
        response['errors'] += 'Did not receive experiment files \n'
    if 'filter' in request.form:
        filtername = request.form['filter'] + '.R'
        if not os.path.isdir(base_filter_path) or not filtername in os.listdir(base_filter_path):
            response['errors'] += 'Invalid Filter name \n'
    else:
        filtername = None
    if 'gen_per_job' in request.form:
        if not request.form['gen_per_job'].isdigit():
            response['errors'] += 'gen_per_job not an integer \n'
        else:
            gen_per_job = int(request.form['gen_per_job'])
    else:
        gen_per_job = None
    if 'retention' in request.form:
        if not request.form['retention'].isdigit():
            response['error'] += 'retention is not an integer \n'
        else:
            retention = int(request.form['retention'])
    else:
        retention = 10
    if 'expname' in request.form:
        expname = str(request.form['expname'])[:50]
    else:
        expname = ''
    if 'email' in request.form:
        email = str(request.form['email'])[:50]
    else:
        email = ''
    if len(response['errors']) > 0: 
        return jsonify(response)

    f = request.files['files']
    filename = f.filename
    response['uid'] = batch_id_generator()
    exp_root_path = os.path.join(base_upload_path, response['uid'])
    os.mkdir(exp_root_path)
    base_folder = os.path.join(exp_root_path, 'base_folder')
    os.mkdir(base_folder)
    f.save(os.path.join(base_folder, filename))
    if(gen_per_job):
        task_desc = ['generate_special_job', exp_root_path, filename, str(gen_per_job)]
    else:
        task_desc = ['handle_special_job', exp_root_path, filename]
    collection.insert_one({'key':response['uid'], 'submitted':datetime.datetime.now(), 'status':1, 'progress':0.0, 'retention':retention, 'expname':expname, 'email':email})
    doTask.delay(task_desc, filtername)
    response['status'] = 'Job submitted to task queue'
    return jsonify(response)

@app.route('/GrapleRunLinearSweep', methods= ['POST'])
def linear_sweep():
    global base_upload_path, base_filter_path
    response = {'errors' : ''}
    if not 'files' in request.files:
        response['errors'] += 'Did not receive experiment files \n'
    if 'filter' in request.form:
        filtername = request.form['filter'] + '.R'
        if not os.path.isdir(base_filter_path) or not filtername in os.listdir(base_filter_path):
            response['errors'] += 'Invalid Filter name \n'
    else:
        filtername = None
    if 'gen_per_job' in request.form:
        if not request.form['gen_per_job'].isdigit():
            response['errors'] += 'gen_per_job not an integer \n'
        else:
            gen_per_job = int(request.form['gen_per_job'])
    else:
        gen_per_job = None
    if 'retention' in request.form:
        if not request.form['retention'].isdigit():
            response['error'] += 'retention is not an integer \n'
        else:
            retention = int(request.form['retention'])
    else:
        retention = 10
    if 'expname' in request.form:
        expname = str(request.form['expname'])[:50]
    else:
        expname = ''
    if 'email' in request.form:
        email = str(request.form['email'])[:50]
    else:
        email = ''
    if len(response['errors']) > 0:
        return jsonify(response)

    f = request.files['files']
    filename = f.filename
    response['uid'] = batch_id_generator()
    exp_root_path = os.path.join(base_upload_path, response['uid'])
    os.mkdir(exp_root_path)
    base_folder = os.path.join(exp_root_path, 'base_folder')
    os.mkdir(base_folder)
    f.save(os.path.join(base_folder, filename))
    if(gen_per_job):
        task_desc = ['generate_sweep_job', exp_root_path, filename, str(gen_per_job)]
    else:
        task_desc = ['handle_sweep_job', exp_root_path, filename]
    collection.insert_one({'key':response['uid'], 'submitted':datetime.datetime.now(), 'status':1, 'progress':0.0, 'retention':retention, 'expname':expname, 'email':email})
    doTask.delay(task_desc, filtername)
    response['status'] = 'Job submitted to task queue'
    return jsonify(response)

@app.route('/GrapleRunStatus/<uid>', methods=['GET'])
def check_status(uid):
    status = check_Job_status(uid)
    return jsonify(status)
    
@app.route('/GrapleEnd/<uid>', methods=['GET'])
def abort_job(uid):
    status = Abort_Job(uid)
    return jsonify(status)

@app.route('/GrapleRun', methods= ['POST'])
def upload_file():
    global base_upload_path, base_filter_path
    response = {'errors' : ''}
    if not 'files' in request.files:
        response['errors'] += 'Did not receive experiment files \n'
    if 'filter' in request.form:
        filtername = request.form['filter'] + '.R'
        if not os.path.isdir(base_filter_path) or not filtername in os.listdir(base_filter_path):
            response['errors'] += 'Invalid Filter name \n'
    else:
        filtername = None
    if 'retention' in request.form:
        if not request.form['retention'].isdigit():
            response['error'] += 'retention is not an integer \n'
        else:
            retention = int(request.form['retention'])
    else:
        retention = 10
    if 'expname' in request.form:
        expname = str(request.form['expname'])[:50]
    else:
        expname = ''
    if 'email' in request.form:
        email = str(request.form['email'])[:50]
    else:
        email = ''
    if len(response['errors']) > 0:
        return jsonify(response)

    f = request.files['files']
    filename = f.filename
    response['uid'] = batch_id_generator()
    exp_root_path = os.path.join(base_upload_path, response['uid'])
    os.mkdir(exp_root_path)
    f.save(os.path.join(exp_root_path, filename))
    task_desc = ['handle_batch_job', exp_root_path, filename]
    collection.insert_one({'key':response['uid'], 'submitted':datetime.datetime.now(), 'status':1, 'progress':0.0, 'retention':retention, 'expname':expname, 'email':email})
    doTask.delay(task_desc, filtername)
    return jsonify(response)

@app.route('/GrapleRunResults/<uid>', methods=['GET'])
def return_consolidated_output(uid, plain = False):
    global base_upload_path
    response = {'errors':''}
    query = {'key':uid}
    dbdoc = collection.find_one(query)
    if dbdoc == None:
        response['errors'] = 'JobID ' + uid + ' not found in database'
    elif dbdoc['progress'] != 100 or dbdoc['status'] < 3:
        response['errors'] = str(dbdoc['progress']) + '% complete. Job under processing, please try after some time'
    elif dbdoc['status'] > 4: # job aborted or expired
        response['errors'] = 'Sorry, results not available anymore. Job expired'

    if len(response['errors']) > 0:
        return response if plain else jsonify(response)

    response['output_url'] = url_for('static', filename = os.path.join(uid, 'output.tar.gz')) 
    response['status'] = 'success'
    if dbdoc['status'] == 3: # need to update only once
        update_doc = {'status':4}
        if dbdoc['retention'] == 0:
            update_doc['expiry'] = datetime.datetime.now() + gwsconf['retention_after_dl']
        collection.update_one({'key':uid}, {'$set':update_doc}) # not yet downloaded though. download can be in progress.
    return response if plain else jsonify(response)
        
@app.route('/DownloadResults/<uid>', methods=['GET'])
def download_results(uid):
    response = return_consolidated_output(uid, True)
    if 'status' in response and response['status'] == 'success':
        return redirect(response['output_url'])
    else:
        return jsonify(response)

@app.route('/download_file/<request_string>', methods=['GET','POST'])
def return_requested_file(request_string):
    global base_upload_path
    parameters=request_string.split('*')
    uid = parameters[0]
    sim_no = parameters[1]
    file_name = parameters[2]
    ret_dict = {}
    if request.method == 'GET':
        dir_name = os.path.join(base_upload_path,uid)
        output_file =  os.path.join(uid,'Results','Sims','Sim'+sim_no,'Results',file_name)
        if (os.path.exists(os.path.join(base_upload_path,output_file))):
            url = url_for('static',filename=output_file)
            ret_dict['output_url']=url
        else:
            process_graple_results(dir_name)
            if (os.path.exists(os.path.join(base_upload_path,output_file))):
                url = url_for('static',filename=output_file)
                ret_dict['output_url']=url
            else:
                ret_dict['output_url']='file not found,please check input args.'
        return jsonify(ret_dict)
        
@app.route('/service_status', methods=['GET'])
def return_service_status():
    service_status = {}
    service_status['status']='I am alive, and at your service. '
    localtime = time.asctime( time.localtime(time.time()) )
    service_status['time']=localtime
    return jsonify(service_status)

@app.route('/GrapleListFilters', methods=['GET'])
def get_PPOLibrary_scripts():
    global base_graple_path, base_filter_path
    filesList = [] 
    if request.method == 'GET':
        if(os.path.exists(base_filter_path)):
            filesList = [os.path.splitext(filtername)[0] for filtername in os.listdir(base_filter_path)]
    return json.dumps(filesList)        

@app.route('/GrapleGetVersion', methods=['GET'])
def get_version():
    compatibleGRAPLEVersions = [] 
    #code for getting the compatible versions of GRAPLEr 
    compatibleGRAPLEVersions.append('2.0.0') 
    return json.dumps(compatibleGRAPLEVersions)

@app.route('/', methods=['GET'])
def index_redirect():
    return redirect('http://www.graple.org', code = 302)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')

