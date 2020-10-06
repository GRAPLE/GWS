#!/usr/bin/env python

import os, time, csv, signal, pandas, string, random, shutil, subprocess, tarfile, json, itertools, datetime, flask
import numpy as np
import celery.signals
from flask import Flask, request, redirect, url_for, jsonify
from distutils.dir_util import copy_tree
from celery import Celery
from pymongo import MongoClient
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
apicoll = db[gwsconf['api_coll_name']]


# mongodb document
# { 'key': the uid of the experiment 
#   'status': see status codes below
#   'payload': list with integers representing condor cluster IDs of experiment jobs
#   'submitted': datetime.datetime object representing time of submission (localtime)
#   'completed': datetime.datetime object representing time of completion
#   'expiry': datetime.datetime object representing when to delete the results
#   'progress': float representing progress of experiment
#   'retention': how long to keep files after completion
#   'apikey': the api key used to submit the experiment
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
            'generate_sweep_job':generate_sweep_job, 
            'generate_special_job':generate_special_job }
    if task[0] in func_dict:
        func_dict[task[0]](task, rscript)
    
def batch_id_generator(size = 40, chars = string.ascii_uppercase + string.digits):
    bid = ''.join(random.choice(chars) for _ in range(size))
    while collection.find_one({'key':bid}) != None:
        bid = ''.join(random.choice(chars) for _ in range(size))
    return bid

def ret_distribution_samples(distribution,samples,parameters):
    parameters = map(float,parameters)
    if distribution == 'uniform':
        return np.random.uniform(parameters[0],parameters[1],samples).tolist()
    elif distribution == 'binomial':
        return np.random.binomial(parameters[0],parameters[1],samples).tolist()
    elif distribution == 'normal':
        return np.random.normal(parameters[0],parameters[1],samples).tolist()
    elif distribution == 'poisson':
        return np.random.poisson(parameters[0],parameters[1]).tolist()
    elif distribution == 'linear':
        return np.linspace(parameters[0],parameters[1],samples).tolist()

def add_ppfilter(rscript, exp_root_path):
    global base_filter_path
    if(rscript):
        scripts_dir =  os.path.join(exp_root_path, 'Scripts')
        rscriptfn = os.path.join(base_filter_path, rscript)
        if os.path.isfile(rscriptfn):
            shutil.copy(rscriptfn, os.path.join(scripts_dir, 'PostProcessFilter.R'))
            jsonfn = os.path.splitext(rscriptfn)[0] + '.json'
            if os.path.isfile(jsonfn):
                with open(jsonfn) as json_file:
                    ppopts = json.load(json_file)
                    if ppopts['Consolidate_Compatible']:
                        shutil.move(os.path.join(exp_root_path, ppopts['Consolidate_Script']), os.path.join(exp_root_path, 'Results'))
        filterParamsDir = os.path.join(exp_root_path, 'base_folder', 'FilterParams') 
        if(os.path.exists(filterParamsDir)):
            shutil.copy(os.path.join(filterParamsDir, 'FilterParams.json'), scripts_dir)

def execute_graple(topdir, SimsPerJob):
    uid = os.path.basename(topdir)
    dbdoc = collection.find_one({'key':uid})
    if dbdoc['status'] == 1: # dont submit if aborted
        submit_response_string=subprocess.check_output(['python', os.path.join(topdir, 'SubmitGrapleBatch.py'), str(SimsPerJob)])
        submitIDList=[]
        for i in submit_response_string.split('\n'):
            if 'cluster' in i:
                submitIDList.append(i.split(' ')[5].split('.')[0])
        update_doc = {'payload':submitIDList, 'status':2}
        collection.update_one({'key':uid}, {'$set':update_doc})

def handle_batch_job(task, rscript):
    global base_graple_path, base_filter_path
    topdir = task[1]
    filename = task[2]
    sims_per_job = task[3]
    copy_tree(base_graple_path, topdir)
    subprocess.call(['python' , os.path.join(topdir, 'CreateWorkingFolders.py')])
    inputfile = os.path.join(topdir, filename)
    subprocess.call(['tar','xzf', inputfile, '-C', os.path.join(topdir, 'Sims')])
    os.remove(inputfile)
    if os.path.isfile(os.path.join(topdir, 'Sims', '.keep_files')):
        shutil.move(os.path.join(topdir, 'Sims', '.keep_files'), os.path.join(topdir, '.keep_files'))
    if(rscript):
        scripts_dir = os.path.join(topdir, 'Scripts')
        rscriptfn = os.path.join(base_filter_path, rscript)
        if os.path.isfile(rscriptfn):
            shutil.copy(rscriptfn, os.path.join(scripts_dir, 'PostProcessFilter.R'))
        filterParamsDir = os.path.join(topdir, 'Sims', 'FilterParams')
        if(os.path.exists(filterParamsDir)):
            shutil.copy(os.path.join(filterParamsDir, 'FilterParams.json'), os.path.join(topdir,'Scripts'))
            shutil.rmtree(filterParamsDir)
    execute_graple(topdir, sims_per_job)

def generate_sweep_job(task, rscript):
    global base_filter_path, base_graple_path
    exp_root_path = task[1]
    filename = task[2]
    sims_per_job = task[3]
    copy_tree(base_graple_path, exp_root_path)
    subprocess.call(['python', os.path.join(exp_root_path, 'CreateWorkingFolders.py')])
    base_folder = os.path.join(exp_root_path, 'base_folder')
    subprocess.call(['tar','xfz', os.path.join(base_folder, filename), '-C', base_folder])
    os.remove(os.path.join(base_folder, filename))
    if os.path.isfile(os.path.join(base_folder, '.keep_files')):
        shutil.move(os.path.join(base_folder, '.keep_files'), os.path.join(exp_root_path, '.keep_files'))
    with open(os.path.join(base_folder, 'job_desc.json')) as data_file:
        jsondata = json.load(data_file)

    add_ppfilter(rscript, exp_root_path)

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
    Sims_dir = os.path.join(exp_root_path, 'Sims')
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
    execute_graple(exp_root_path, 1) # for a generate job, the bundled sims per condor job is 1, worker expands the single job into many as per sims_per_job
    return
        
def generate_special_job(task, rscript):
    global base_filter_path, base_graple_path
    exp_root_path = task[1]
    filename = task[2]
    sims_per_job = task[3]
    copy_tree(base_graple_path, exp_root_path)
    subprocess.call(['python', os.path.join(exp_root_path, 'CreateWorkingFolders.py')])
    base_folder = os.path.join(exp_root_path, 'base_folder')
    subprocess.call(['tar','xfz', os.path.join(base_folder, filename), '-C', base_folder])
    os.remove(os.path.join(base_folder, filename))
    if os.path.isfile(os.path.join(base_folder, '.keep_files')):
        shutil.move(os.path.join(base_folder, '.keep_files'), os.path.join(exp_root_path, '.keep_files'))
    with open(os.path.join(base_folder, 'job_desc.json')) as data_file:    
        jsondata = json.load(data_file)

    add_ppfilter(rscript, exp_root_path)

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

    Sims_dir = os.path.join(exp_root_path, 'Sims')
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
    execute_graple(exp_root_path, 1)
    return

def check_request():
    global base_filter_path
    response = {'errors' : '', 'warnings': ''}
    if request.files['files'].filename == '':
        response['errors'] += 'Did not receive experiment files \n'
    if 'apikey' in request.form:
        apikey = request.form['apikey']
        if apikey == '0':
            response['warnings'] += 'No API key provided \n'
        if apicoll.find_one({'key' : apikey}) == None:
            response['errors'] += 'Invalid API key \n'
    else:
        response['errors'] += 'No API key provided \n'
    if 'filter' in request.form:
        filtername = request.form['filter'] + '.R'
        if not os.path.isdir(base_filter_path) or not filtername in os.listdir(base_filter_path):
            response['errors'] += 'Invalid Filter name \n'
    else:
        filtername = None
    if 'simsperjob' in request.form:
        if not request.form['simsperjob'].isdigit():
            response['errors'] += 'simsperjob not an integer \n'
        else:
            sims_per_job = int(request.form['simsperjob'])
    else:
        sims_per_job = gwsconf['def_sims_per_job']
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
    email = ''
    if 'email' in request.form:
        if len(request.form['email']) != 0:
            email = str(request.form['email'])[:50]
        elif 'apikey' in request.form:
            found = apicoll.find_one({'key' : apikey})
            if found != None and 'email' in found:
                email = found['email']
    return response, apikey, filtername, sims_per_job, retention, expname, email

@app.route('/GrapleRun', methods= ['POST'])
def batch_job():
    global base_upload_path
    response, apikey, filtername, sims_per_job, retention, expname, email = check_request()
    if len(response['errors']) > 0:
        return jsonify(response)

    f = request.files['files']
    filename = f.filename
    response['uid'] = batch_id_generator()
    exp_root_path = os.path.join(base_upload_path, response['uid'])
    os.mkdir(exp_root_path)
    f.save(os.path.join(exp_root_path, filename))
    task_desc = ['handle_batch_job', exp_root_path, filename, sims_per_job]
    collection.insert_one({'key':response['uid'], 'submitted':datetime.datetime.now(), 'status':1, 'progress':0.0, 'retention':retention, 'expname':expname, 'email':email, 'apikey':apikey})
    doTask.delay(task_desc, filtername)
    return jsonify(response)

@app.route('/GrapleRunLinearSweep', methods= ['POST'])
def linear_sweep():
    global base_upload_path
    response, apikey, filtername, sims_per_job, retention, expname, email = check_request()
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
    task_desc = ['generate_sweep_job', exp_root_path, filename, sims_per_job]
    collection.insert_one({'key':response['uid'], 'submitted':datetime.datetime.now(), 'status':1, 'progress':0.0, 'retention':retention, 'expname':expname, 'email':email, 'apikey':apikey})
    doTask.delay(task_desc, filtername)
    response['status'] = 'Job submitted to task queue'
    return jsonify(response)

@app.route('/GrapleRunMetSample', methods= ['POST'])
def special_batch():
    global base_upload_path
    response, apikey, filtername, sims_per_job, retention, expname, email = check_request()
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
    task_desc = ['generate_special_job', exp_root_path, filename, sims_per_job]
    collection.insert_one({'key':response['uid'], 'submitted':datetime.datetime.now(), 'status':1, 'progress':0.0, 'retention':retention, 'expname':expname, 'email':email, 'apikey':apikey})
    doTask.delay(task_desc, filtername)
    response['status'] = 'Job submitted to task queue'
    return jsonify(response)

@app.route('/GrapleRunStatus/<uid>', methods=['GET'])
def check_status(uid):
    response = {'errors':'', 'warnings':''}
    if 'apikey' in request.args:
        apikey = request.args['apikey']
        if apikey == '0':
            response['warnings'] += 'No API key provided \n'
    else:
        response['errors'] += 'No API key provided \n'
    if len(response['errors']) > 0:
        return jsonify(response)
    query = {'key':uid, 'apikey':apikey}
    dbdoc = collection.find_one(query)
    if dbdoc == None:
        response['errors'] += 'JobID ' + uid + ' not found in database'
    if len(response['errors']) > 0:
        return jsonify(response)

    response['curr_status'] = str(dbdoc['progress']) + '% completed '
    return jsonify(response)
    
@app.route('/GrapleEnd/<uid>', methods=['GET'])
def abort_job(uid):
    response = {'errors':'', 'warnings':''}
    if 'apikey' in request.args:
        apikey = request.args['apikey']
        if apikey == '0':
            response['warnings'] += 'No API key provided \n'
    else:
        response['errors'] += 'No API key provided \n'
    query = {'key':uid, 'apikey':apikey}
    dbdoc = collection.find_one(query)
    if dbdoc == None:
        response['errors'] += 'JobID ' + uid + ' not found in database'
    elif dbdoc['status'] == 5:
        response['errors'] += 'Job already marked for removal'
    else:
        update_doc = {'status':5}
        collection.update_one({'key':uid}, {'$set':update_doc})
        response['curr_status'] = 'All jobs marked for removal'
    return jsonify(response)

@app.route('/GrapleRunResults/<uid>', methods=['GET'])
def return_consolidated_output(uid):
    global base_upload_path
    response = {'errors':'', 'warnings':''}
    if 'apikey' in request.args:
        apikey = request.args['apikey']
        if apikey == '0':
            response['warnings'] += 'No API key provided \n'
    else:
        response['errors'] += 'No API key provided \n'

    if len(response['errors']) > 0:
        return jsonify(response)

    query = {'key':uid, 'apikey':apikey}
    dbdoc = collection.find_one(query)
    
    if dbdoc == None:
        response['errors'] = 'JobID ' + uid + ' not found in database'
    elif dbdoc['progress'] != 100 or dbdoc['status'] < 3:
        response['errors'] = str(dbdoc['progress']) + '% completed. Job under processing, please try after some time'
    elif dbdoc['status'] > 4: # job aborted or expired
        response['errors'] = 'Sorry, results not available anymore. Job expired'

    if len(response['errors']) > 0:
        return jsonify(response)

    response['output_url'] = url_for('static', filename = os.path.join(uid, 'output.tar.gz')) 
    response['status'] = 'success'
    if dbdoc['status'] == 3: # need to update only once
        update_doc = {'status':4}
        if dbdoc['retention'] == 0:
            update_doc['expiry'] = datetime.datetime.now() + gwsconf['retention_after_dl']
        collection.update_one({'key':uid}, {'$set':update_doc}) # not yet downloaded though. download can be in progress.
    return jsonify(response)
        
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
    if(os.path.exists(base_filter_path)):
        filesList = [os.path.splitext(filtername)[0] for filtername in os.listdir(base_filter_path)]
    return json.dumps(filesList)        

@app.route('/GrapleGetVersion', methods=['GET'])
def get_version():
    #code for getting the compatible versions of GRAPLEr 
    compatibleGRAPLEVersions = ['3.1.0', '3.1.1', '3.1.2']
    return json.dumps(compatibleGRAPLEVersions)

@app.route('/', methods=['GET'])
def index_redirect():
    return redirect('http://www.graple.org', code = 302)

if __name__ == '__main__':
    app.debug = gwsconf['debug']
    app.run(host='0.0.0.0')
