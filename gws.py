#!usr/bin/python
import os,time
import csv
import numpy as np
import signal
import pandas
from flask import Flask, request, redirect, url_for,jsonify,send_from_directory
from werkzeug import secure_filename
import string,random,shutil
from distutils.dir_util import copy_tree
from os.path import join
import subprocess,os
from flask import Flask
from flask import request
from celery import Celery
import celery.signals
import pymongo
import tarfile
from pymongo import MongoClient
from os import listdir
import json
import itertools

# global variables and paths
base_working_path = '/home/grapleadmin/grapleService/'
 
base_upload_path = base_working_path + 'static'
base_graple_path = base_working_path + 'GRAPLE_SCRIPTS'
base_filter_path = base_working_path + 'Filters'

app = Flask(__name__, static_url_path='', static_folder = base_upload_path)
app.config['CELERY_BROKER_URL'] = 'amqp://'
app.config['CELERY_ACCEPT_CONTENT'] = ['json']
app.config['CELERY_TASK_SERIALIZER'] = 'json'
app.config['CELERY_RESULT_SERIALIZER'] = 'json'
#app.config['CELERY_RESULT_BACKEND'] = 'ampq'
celeryob = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celeryob.conf.update(app.config)
db_client = MongoClient(connect = False) # running at default port
db = db_client.grapleDB
collection = db.graple_collection

def_SimsPerJob = 5 # can automatically read from Graple.py if required
def_id_size = 40

@celery.signals.worker_process_init.connect()
def seed_rand(**_): # makes sure different celery workers generate different random numbers
    np.random.seed()

@celeryob.task 
def doTask(task, rscript=''):
    if (task.split('$')[0] == "graple_run_batch"):
        dir_name = task.split('$')[1]
        filename = task.split('$')[2]
        setup_graple(dir_name,filename, rscript)
        execute_graple(dir_name)
    elif (task.split('$')[0] == "run_linear_sweep"):
        handle_linearsweep_run(task, rscript)
    elif (task.split('$')[0] == "graple_special_batch"):
        handle_special_job(task, rscript)
    elif (task.split('$')[0] == "gen_run_linear_sweep"):
        generate_linearsweep_run(task, rscript)
    elif (task.split('$')[0] == "gen_graple_special_batch"):
        generate_special_job(task, rscript)
        
def batch_id_generator(size=def_id_size, chars=string.ascii_uppercase + string.digits):
    bid = ''.join(random.choice(chars) for _ in range(size))
    while collection.find_one({'key':bid}) != None:
        bid = ''.join(random.choice(chars) for _ in range(size))
    return bid

def setup_graple(topdir, filename, rscript):
    copy_tree(base_graple_path, topdir)
    subprocess.call(['python' , os.path.join(topdir, 'CreateWorkingFolders.py')])
    # the contents of tar.gz input should be individual sim folders 
    # check if file ends in tar.gzip if so do tar xvfz filename
    #subprocess.call(['unzip' , filename])
    inputfile = os.path.join(topdir, filename)
    subprocess.call(['tar','xzf', inputfile, '-C', os.path.join(topdir, 'Sims')])
    os.remove(inputfile)
    if(rscript):
        scripts_dir = os.path.join(topdir, 'Scripts')
        rscriptfn = os.path.join(base_filter_path, rscript)
        if os.path.isfile(rscriptfn):
            shutil.copy(rscriptfn, os.path.join(scripts_dir, 'PostProcessFilter.R'))
        filterParamsDir = os.path.join(topdir, "Sims", "FilterParams")
        if(os.path.exists(filterParamsDir)):
            shutil.copy(os.path.join(filterParamsDir, "FilterParams.json"), os.path.join(topdir,"Scripts"))
            shutil.rmtree(filterParamsDir)
    
def execute_graple(topdir, SimsPerJob = def_SimsPerJob, force_gen = False):
    if SimsPerJob == def_SimsPerJob and (not force_gen):
        submit_response_string=subprocess.check_output(['python', os.path.join(topdir, 'SubmitGrapleBatch.py')])
    else:
        submit_response_string=subprocess.check_output(['python', os.path.join(topdir, 'SubmitGrapleBatch.py'), str(SimsPerJob)])

    submitIDList=[]
    for i in submit_response_string.split("\n"):
        if "cluster" in i:
            submitIDList.append(i.split(" ")[5].split(".")[0])
    try:
        payload = {'key':topdir[-def_id_size:],'payload':submitIDList}
        collection.insert_one(payload)
    except:
        print("Error in db access in execute_graple")
    
       
def process_graple_results(topdir):
    #subprocess.call(['python','ProcessGrapleBatchOutputs.py'])
    tarfn = os.path.join(topdir,'Results', 'output.tar.gz')
    # call tar czf output.tar.gz Sims
    #subprocess.call(['zip','-r','output.zip','Sims'])
    #subprocess.call(['tar','czf','output.tar.gz','Sims'])
    #subprocess.call(['tar', 'czf', tarfn, '-C', os.path.dirname(tarfn)])
    with tarfile.open(tarfn, 'w:gz', compresslevel=9) as tar:
        for f in listdir(os.path.join(topdir, 'Results')):
            if f.endswith('.tar.bz2') or f == 'sim_summary.csv':
                tar.add(os.path.join(topdir, 'Results', f), f)
            #os.remove(f) 
    
def check_Job_status(path):
    query = {'key':path[-def_id_size:]}
    try:
        submitIDList = collection.find_one(query)['payload']
    except:
        print ("error in db access in check_job_status")
        return "Job submission to Condor pool in progress."
    condor_command = ['condor_history'] + map(str, submitIDList) + ['-format', '%d', 'JobStatus']
    cout = subprocess.check_output(condor_command)
    compl = float(cout.count("4"))/len(submitIDList)*100
    return str(round(compl, 2)) + "% complete"

def Abort_Job(path):
    query = {'key':path[-def_id_size:]}
    try:
        submitIDList = collection.find_one(query)['payload']
    except:
        print ("error in db access in Abort")
        return "Job submission to Condor pool is in progress., try again once \
        all jobs are submitted"
    condor_command = ['condor_rm'] + map(str, submitIDList)
    cout = subprocess.check_output(condor_command)
    return "All jobs marked for removal"
        
def ret_distribution_samples(distribution,samples,parameters):
    parameters = map(float,parameters)
        
    if distribution=="uniform":
        return np.random.uniform(parameters[0],parameters[1],samples).tolist()
    elif distribution=="binomial":
        return np.random.binomial(parameters[0],parameters[1],samples).tolist()
    elif distribution=="normal":
        return np.random.normal(parameters[0],parameters[1],samples).tolist()
    elif distribution=="poisson":
        return np.random.poisson(parameters[0],parameters[1]).tolist()
    elif distribution=="linear":
        return np.linspace(parameters[0],parameters[1],samples).tolist()

def generate_linearsweep_run(task, rscript):
    if (task.split('$')[0] == "gen_run_linear_sweep"):
        exp_root_path = task.split('$')[1]
        filename = task.split('$')[2]
        sims_per_job = int(task.split('$')[3])
        base_folder = os.path.join(exp_root_path, 'base_folder')
        # unzip the zip file and read job_desc.csv
        #subprocess.call(['unzip' , filename])
        subprocess.call(['tar','xfz', os.path.join(base_folder, filename), '-C', base_folder])
        os.remove(os.path.join(base_folder, filename))
        # read job description from csv file
        with open(os.path.join(base_folder, 'job_desc.json')) as data_file:
            jsondata = json.load(data_file)

        if(rscript):
            scripts_dir =  os.path.join(exp_root_path, 'Scripts')
            rscriptfn = os.path.join(base_filter_path, rscript)
            if os.path.isfile(rscriptfn):
                shutil.copy(rscriptfn, os.path.join(scripts_dir, 'PostProcessFilter.R'))
            filterParamsDir = os.path.join(base_folder, "FilterParams") 
            if(os.path.exists(filterParamsDir)):
                shutil.copy(os.path.join(filterParamsDir, "FilterParams.json"), scripts_dir)

        summary = []
        columns = {}
        noOfFiles = len(jsondata["ExpFiles"])
        base_iterations = 1
        for i in range(0, noOfFiles):
            base_file = jsondata["ExpFiles"][i]["driverfile"]
            variables = jsondata["ExpFiles"][i]["variables"][0]
            columns[base_file] = {}
            for key, value in variables.iteritems():
                variable = key
                var_distribution = ""
                var_start_value = 0
                var_end_value = 0
                var_operation = ""
                var_steps = 0
                if("distribution" in value):
                    var_distribution = value["distribution"]
                if("operation" in value):
                    var_operation = value["operation"]
                if("start" in value):
                    var_start_value = value["start"]
                if("end" in value):
                    var_end_value = value["end"]
                if("steps" in value):
                    var_steps = value["steps"]
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
            new_dir=os.path.join(Sims_dir, "Sim"+ str(sim_no))
            shutil.copytree(base_folder, new_dir)
            to_pack = [varcomb, iterprod[cc:cc+sims_per_job]]
            with open(os.path.join(new_dir, "generate.json"), "w") as pickf:
                json.dump(to_pack, pickf)
            for c in range(len(to_pack[1])):
                row = ["sim_"+str(sim_no), str(c+1)]
                for i in range(len(varcomb)):
                    var_list = varcomb[i].split(",")
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
        result_summary = open(os.path.join(exp_root_path, "Results", "sim_summary.csv"),'wb')
        wr = csv.writer(result_summary,dialect='excel')
        for row in summary:
            wr.writerow(row)
        result_summary.close()

        # execute graple job
        execute_graple(exp_root_path, 1, True) # for a generate job, the bundled sims per job is 1, worker expands the single job into many as per sims_per_job
        return
        
# handles sweep string cases.    
def handle_linearsweep_run(task, rscript):
    if (task.split('$')[0] == "run_linear_sweep"):
        exp_root_path = task.split('$')[1]
        filename = task.split('$')[2]
        base_folder = os.path.join(exp_root_path, 'base_folder')
        # unzip the zip file and read job_desc.csv
        #subprocess.call(['unzip' , filename])
        subprocess.call(['tar','xfz', os.path.join(base_folder, filename), '-C', base_folder])
        os.remove(os.path.join(base_folder, filename))
        # read job description from csv file
        with open(os.path.join(base_folder, 'job_desc.json')) as data_file:
            jsondata = json.load(data_file)

        if(rscript):
            scripts_dir =  os.path.join(exp_root_path,'Scripts')
            rscriptfn = os.path.join(base_filter_path, rscript)
            if os.path.isfile(rscriptfn):
                shutil.copy(rscriptfn, os.path.join(scripts_dir, 'PostProcessFilter.R'))
            filterParamsDir = os.path.join(base_folder, "FilterParams") 
            if(os.path.exists(filterParamsDir)):
                shutil.copy(os.path.join(filterParamsDir, "FilterParams.json"), scripts_dir)

        summary = []
        columns = {}
        noOfFiles = len(jsondata["ExpFiles"])
        base_iterations = 1
        for i in range(0, noOfFiles):
            base_file = jsondata["ExpFiles"][i]["driverfile"]
            variables = jsondata["ExpFiles"][i]["variables"][0]
            columns[base_file] = {}
            for key, value in variables.iteritems():
                variable = key
                var_distribution = ""
                var_start_value = 0
                var_end_value = 0
                var_operation = ""
                var_steps = 0
                if("distribution" in value):
                    var_distribution = value["distribution"]
                if("operation" in value):
                    var_operation = value["operation"]
                if("start" in value):
                    var_start_value = value["start"]
                if("end" in value):
                    var_end_value = value["end"]
                if("steps" in value):
                    var_steps = value["steps"]
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
            new_dir=os.path.join(Sims_dir, "Sim"+ str(sim_no))
            summary.append(["sim_"+str(sim_no)])
            shutil.copytree(base_folder, new_dir)
            for i in range(len(varcomb)):
                var_list = varcomb[i].split(",")
                base_file = var_list[0]
                field = var_list[1]
                operation = var_list[3]
                delta = comb[i]
                data = pandas.read_csv(os.path.join(new_dir, base_file))
                data = data.rename(columns=lambda x: x.strip())
                if (((" "+field) in data.columns) or (field in data.columns)):
                # handle variations in filed names in csv file, some field names have leading spaces.
                    if " "+field in data.columns:
                        field_modified = " "+field
                    else:
                        field_modified = field
                if (operation=="add"):
                    data[field_modified]=data[field_modified].apply(lambda val:val+delta)
                elif (operation=="sub"):
                    data[base_file][field_modified]=data[field_modified].apply(lambda val:val-delta)
                elif (operation=="mul"):
                    data[field_modified]=data[field_modified].apply(lambda val:val*delta)
                elif (operation=="div"):
                    data[field_modified]=data[field_modified].apply(lambda val:val/delta)
                summary[sim_no - 1].append(field)
                summary[sim_no - 1].append(columns[base_file][field][2]) 
                summary[sim_no - 1].append(columns[base_file][field][1]) 
                summary[sim_no - 1].append(str(delta))
                data.to_csv(os.path.join(new_dir, base_file), index=False)
            sim_no += 1
        # write summary of modifications to a file.
        result_summary = open(os.path.join(exp_root_path, "Results", "sim_summary.csv"),'wb')
        wr = csv.writer(result_summary,dialect='excel')
        for row in summary:
            wr.writerow(row)
        result_summary.close()

        # execute graple job
        execute_graple(exp_root_path)
        return

def generate_special_job(task, rscript):
    if (task.split('$')[0] == "gen_graple_special_batch"):
        exp_root_path = task.split('$')[1]
        filename = task.split('$')[2]
        sims_per_job = int(task.split('$')[3])
        base_folder = os.path.join(exp_root_path, 'base_folder')
        # unzip the zip file and read job_desc.csv
        #subprocess.call(['unzip' , filename])
        subprocess.call(['tar','xfz', os.path.join(base_folder, filename), '-C', base_folder])
        # read job description from csv file
        with open(os.path.join(base_folder, 'job_desc.json')) as data_file:    
            jsondata = json.load(data_file)

        if(rscript):
            scripts_dir =  os.path.join(exp_root_path, 'Scripts')
            rscriptfn = os.path.join(base_filter_path, rscript)
            if os.path.isfile(rscriptfn):
                shutil.copy(rscriptfn, os.path.join(scripts_dir, 'PostProcessFilter.R'))
            filterParamsDir = os.path.join(base_folder, "FilterParams") 
            if(os.path.exists(filterParamsDir)):
                shutil.copy(os.path.join(filterParamsDir, "FilterParams.json"), scripts_dir)

        summary = []
        columns = {}
        noOfFiles = len(jsondata["ExpFiles"]) 
        base_iterations = jsondata["num_iterations"]
        for i in range(0, noOfFiles):
            base_file = jsondata["ExpFiles"][i]["driverfile"]
            variables = jsondata["ExpFiles"][i]["variables"][0]
            columns[base_file] = {}
            for key, value in variables.iteritems():
                variable = key
                var_distribution = ""
                var_start_value = 0
                var_end_value = 0
                var_operation = ""
                if("distribution" in value):
                    var_distribution = value["distribution"]
                if("operation" in value):
                    var_operation = value["operation"]                  
                if("start" in value):
                    var_start_value = value["start"]
                if("end" in value):
                    var_end_value = value["end"]    
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
            new_dir = os.path.join(Sims_dir, "Sim"+ str(sim_no))
            shutil.copytree(base_folder, new_dir)
            to_pack = [varcomb, iterprod[cc:cc+sims_per_job]]
            with open(os.path.join(new_dir, "generate.json"), "w") as pickf:
                json.dump(to_pack, pickf)
            for c in range(len(to_pack[1])):
                row = ["sim_"+str(sim_no), str(c+1)]
                for i in range(len(varcomb)):
                    var_list = varcomb[i].split(",")
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
        result_summary = open(os.path.join(exp_root_path, "Results", "sim_summary.csv"),'wb')
        wr = csv.writer(result_summary,dialect='excel')
        for row in summary:
            wr.writerow(row)
        result_summary.close()

        # execute graple job
        execute_graple(exp_root_path, 1, True)
        return

# handles the core of distribution sweep jobs.
def handle_special_job(task, rscript):
    if (task.split('$')[0] == "graple_special_batch"):
        exp_root_path = task.split('$')[1]
        filename = task.split('$')[2]
      
        base_folder = os.path.join(exp_root_path,'base_folder')
        # unzip the zip file and read job_desc.csv
        #subprocess.call(['unzip' , filename])
        subprocess.call(['tar','xfz', os.path.join(base_folder,filename), '-C', base_folder])
        # read job description from csv file
        with open(os.path.join(base_folder, 'job_desc.json')) as data_file:    
            jsondata = json.load(data_file)

        if(rscript):
            scripts_dir =  os.path.join(exp_root_path, 'Scripts')
            rscriptfn = os.path.join(base_filter_path, rscript)
            if os.path.isfile(rscriptfn):
                shutil.copy(rscriptfn, os.path.join(scripts_dir, 'PostProcessFilter.R'))
            filterParamsDir = os.path.join(base_folder, "FilterParams") 
            if(os.path.exists(filterParamsDir)):
                shutil.copy(os.path.join(filterParamsDir, "FilterParams.json"), scripts_dir)

        summary = []
        columns = {}
        noOfFiles = len(jsondata["ExpFiles"]) 
        base_iterations = jsondata["num_iterations"]
        for i in range(0, noOfFiles):
            base_file = jsondata["ExpFiles"][i]["driverfile"]
            variables = jsondata["ExpFiles"][i]["variables"][0]
            columns[base_file] = {}
            for key, value in variables.iteritems():
                variable = key
                var_distribution = ""
                var_start_value = 0
                var_end_value = 0
                var_operation = ""
                if("distribution" in value):
                    var_distribution = value["distribution"]
                if("operation" in value):
                    var_operation = value["operation"]                  
                if("start" in value):
                    var_start_value = value["start"]
                if("end" in value):
                    var_end_value = value["end"]    
                columns[base_file][variable]=[ret_distribution_samples(var_distribution,base_iterations,[var_start_value, var_end_value])]
                columns[base_file][variable].append(var_operation)
                columns[base_file][variable].append(var_distribution)

        Sims_dir=os.path.join(exp_root_path,'Sims')                
        for j in range(1,base_iterations+1):
            new_dir = os.path.join(Sims_dir, "Sim"+ str(j))
            summary.append(["sim_"+str(j)])
            shutil.copytree(base_folder, new_dir)
            for key in columns.keys():
                base_file = key             
                data = pandas.read_csv(os.path.join(new_dir, base_file))
                data = data.rename(columns=lambda x: x.strip())  
                for field in columns[base_file].keys():
                    if (((" "+field) in data.columns) or (field in data.columns)):
                    # handle variations in filed names in csv file, some field names have leading spaces.
                        if " "+field in data.columns:
                            field_modified = " "+field
                        else:
                            field_modified = field
                        delta = columns[base_file][field][0][j-1]
                        if (columns[base_file][field][1]=="add"):
                            data[field_modified]=data[field_modified].apply(lambda val:val+delta)
                        elif (columns[base_file][field][1]=="sub"):
                            data[base_file][field_modified]=data[field_modified].apply(lambda val:val-delta)
                        elif (columns[base_file][field][1]=="mul"):
                            data[field_modified]=data[field_modified].apply(lambda val:val*delta)
                        elif (columns[base_file][field][1]=="div"):
                            data[field_modified]=data[field_modified].apply(lambda val:val/delta)
                        # make note of modified changes in a list datastructure
                        summary[j-1].append(field) # append column_name
                        summary[j-1].append(columns[base_file][field][2]) # append distribution
                        summary[j-1].append(columns[base_file][field][1]) # append operation
                        summary[j-1].append(str(delta)) # append delta
                    # at this point the dataframe has been modified, write back to csv.
                data.to_csv(os.path.join(new_dir, base_file), index=False)     
        # write summary of modifications to a file.
        result_summary = open(os.path.join(exp_root_path, "Results", "sim_summary.csv"),'wb')
        wr = csv.writer(result_summary,dialect='excel')
        for row in summary:
            wr.writerow(row)
        result_summary.close()

        # execute graple job
        execute_graple(exp_root_path)
        return
        
@app.route('/GrapleRunMetSample', defaults={'filtername': None, 'sims_per_job':None}, methods= ['GET', 'POST'])
@app.route('/GrapleRunMetSample/<filtername>', defaults = {'sims_per_job':None}, methods= ['GET', 'POST'])
@app.route('/GrapleRunMetSample/<filtername>/<int:sims_per_job>', methods= ['GET', 'POST'])
def special_batch(filtername, sims_per_job):
    global base_upload_path
    if request.method == 'POST':
        f = request.files['files']
        filename = f.filename
        response = {"uid":batch_id_generator()}
        exp_root_path = os.path.join(base_upload_path, response["uid"])
        os.mkdir(exp_root_path)
        base_folder = os.path.join(exp_root_path, 'base_folder')
        os.mkdir(base_folder)
        f.save(os.path.join(base_folder, filename))
        copy_tree(base_graple_path, exp_root_path)
        subprocess.call(['python', os.path.join(exp_root_path, 'CreateWorkingFolders.py')])
        # have to submit job to celery here--below method has to be handled by celery worker
        if(sims_per_job):
            task_desc = "gen_graple_special_batch" + "$" + exp_root_path + "$" + filename + "$" + str(sims_per_job)
        else:
            task_desc = "graple_special_batch" + "$" + exp_root_path + "$" + filename
        if(filtername):
            filtername += '.R'
            doTask.delay(task_desc, filtername)
        else:
            doTask.delay(task_desc)
        response["status"] = "Job submitted to task queue"
        return jsonify(response)

@app.route('/GrapleRunLinearSweep', defaults={'filtername': None, 'sims_per_job': None}, methods= ['GET', 'POST'])
@app.route('/GrapleRunLinearSweep/<filtername>', defaults={'sims_per_job': None}, methods= ['GET', 'POST'])
@app.route('/GrapleRunLinearSweep/<filtername>/<int:sims_per_job>', methods=['GET', 'POST'])
def linear_sweep(filtername, sims_per_job):
    global base_upload_path
    if request.method == 'POST':
        f = request.files['files']
        filename = f.filename
        response = {"uid":batch_id_generator()}
        exp_root_path = os.path.join(base_upload_path, response["uid"])
        os.mkdir(exp_root_path)
        base_folder = os.path.join(exp_root_path, 'base_folder')
        os.mkdir(base_folder)
        f.save(os.path.join(base_folder, filename))
        copy_tree(base_graple_path, exp_root_path)
        subprocess.call(['python', os.path.join(exp_root_path, 'CreateWorkingFolders.py')])
        # have to submit job to celery here--below method has to be handled by celery worker
        if(sims_per_job):
            task_desc = "gen_run_linear_sweep"+"$"+exp_root_path+"$"+filename+"$"+str(sims_per_job)
        else:
            task_desc = "run_linear_sweep"+"$"+exp_root_path+"$"+filename
        if(filtername):
            filtername += '.R'
            doTask.delay(task_desc, filtername)
        else:
            doTask.delay(task_desc)
        response["status"] = "Job submitted to task queue"
        return jsonify(response)

@app.route('/GrapleRunStatus/<uid>', methods=['GET','POST'])
def check_status(uid):
    global base_upload_path
    
    if request.method == 'GET':
        dir_name = os.path.join(base_upload_path,str(uid))
        status ={}
        status["curr_status"]=check_Job_status(dir_name)
        return jsonify(status)
    
@app.route('/GrapleEnd/<uid>', methods=['GET','POST'])
def abort_job(uid):
    global base_upload_path
    
    if request.method == 'GET':
        dir_name = os.path.join(base_upload_path,str(uid))
        status ={}
        status["curr_status"]=Abort_Job(dir_name)
        return jsonify(status)

@app.route('/GrapleRun', defaults={'filtername': None}, methods= ['GET', 'POST'])
@app.route('/GrapleRun/<filtername>', methods= ['GET', 'POST'])
def upload_file(filtername):
    global base_upload_path
    if request.method == 'POST':
        f = request.files['files']
        filename = f.filename
        response = {"uid":batch_id_generator()}
        exp_root_path = os.path.join(base_upload_path, response["uid"])
        os.mkdir(exp_root_path)
        f.save(os.path.join(exp_root_path, filename))
        # should put the task in queue here and return.
        task_desc = "graple_run_batch"+"$"+exp_root_path+"$"+filename
        if (filtername): 
            filtername += '.R'
            doTask.delay(task_desc, filtername)
        else:  
            doTask.delay(task_desc)  
        return jsonify(response)

@app.route('/GrapleRunResults/<uid>', methods=['GET','POST'])
def return_consolidated_output(uid):
    global base_upload_path
    dir_name = os.path.join(base_upload_path,uid)
    ret_dict = {}
    if request.method == 'GET':
        # Sanity check to ensure that job processing is completed.
        status = {}
        status["curr_status"] = check_Job_status(dir_name)
        if not status["curr_status"].startswith("100"):
            ret_dict["status"]="Job under processing,please try agian after some time."
            return jsonify(ret_dict)
        process_graple_results(dir_name)
        #output_file = os.path.join(uid,'Results','output.zip')
        output_file = os.path.join(uid,'Results','output.tar.gz')
        url = url_for('static',filename=output_file) 
        ret_dict["output_url"]=url
        ret_dict["status"]="success"
        return jsonify(ret_dict)
        
@app.route('/download_file/<request_string>', methods=['GET','POST'])
def return_requested_file(request_string):
    global base_upload_path
    parameters=request_string.split("*")
    uid = parameters[0]
    sim_no = parameters[1]
    file_name = parameters[2]
    ret_dict = {}
    if request.method == 'GET':
        dir_name = os.path.join(base_upload_path,uid)
        output_file =  os.path.join(uid,'Results','Sims','Sim'+sim_no,'Results',file_name)
        if (os.path.exists(os.path.join(base_upload_path,output_file))):
            url = url_for('static',filename=output_file)
            ret_dict["output_url"]=url
        else:
            process_graple_results(dir_name)
            if (os.path.exists(os.path.join(base_upload_path,output_file))):
                url = url_for('static',filename=output_file)
                ret_dict["output_url"]=url
            else:
                ret_dict["output_url"]="file not found,please check input args."
        return jsonify(ret_dict)
        
@app.route('/service_status', methods=['GET'])
def return_service_status():
    if request.method == 'GET':
        service_status = {}
        service_status["status"]="I am alive, and at your service. "
        localtime = time.asctime( time.localtime(time.time()) )
        service_status["time"]=localtime
        return jsonify(service_status)

@app.route('/GrapleListFilters', methods=['GET'])
def get_PPOLibrary_scripts():
    global base_graple_path
    filesList = [] 
    if request.method == 'GET':
        if(os.path.exists(base_filter_path)):
            filesList = [os.path.splitext(filtername)[0] for filtername in os.listdir(base_filter_path)]
    return json.dumps(filesList)        

@app.route('/GrapleGetVersion', methods=['GET'])
def get_version():
    compatibleGRAPLEVersions = [] 
    if request.method == 'GET':
        #code for getting the compatible versions of GRAPLEr 
        compatibleGRAPLEVersions.append("2.0.0") 
        return json.dumps(compatibleGRAPLEVersions)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')

