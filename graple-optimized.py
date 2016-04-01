#!usr/bin/python
import os,time
import csv
import numpy as np
import signal
import pandas
from flask import Flask, request, redirect, url_for,jsonify,send_from_directory
from werkzeug import secure_filename
import string,random,shutil,pickle
from distutils.dir_util import copy_tree
from os.path import join
import subprocess,os
from flask import Flask
from flask import request
from celery import Celery
import pymongo
import tarfile
from pymongo import MongoClient
from netCDF4 import Dataset
from os import listdir
import json

# http://unix.stackexchange.com/questions/13093/add-update-a-file-to-an-existing-tar-gz-archive
# http://unix.stackexchange.com/questions/46969/compress-a-folder-with-tar
 
app = Flask(__name__, static_url_path='')
app.config['CELERY_BROKER_URL'] = 'amqp://'
#app.config['CELERY_RESULT_BACKEND'] = 'ampq'
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
db_client = MongoClient() # running at default port
db = db_client.grapleDB
collection = db.graple_collection

# global variables and paths

base_upload_path = '/home/grapler-cert/datadrive/static'
base_graple_path = '/home/grapler-cert/datadrive/GRAPLE_SCRIPTS'
base_GLM_path    = '/home/grapler-cert/datadrive/GLM_Bins'

@celery.task 
def doTask(task, rscript=''):
    if (task.split('$')[0] == "graple_run_batch"):
        dir_name = task.split('$')[1]
        filename = task.split('$')[2]
        setup_graple(dir_name,filename, rscript)
        execute_graple(dir_name)
    elif (task.split('$')[0] == "run_sweep"):
        dir_name = task.split('$')[1]
        sweepstring = task.split('$')[2]
        handle_sweep_run(dir_name,sweepstring)
    elif (task.split('$')[0] == "graple_special_batch"):
        handle_special_job(task, rscript)
        
def batch_id_generator(size=40, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def setup_graple(path,filename, rscript):
    copy_tree(base_graple_path,path)
    copy_tree(base_GLM_path,path)
    os.chdir(path)
    subprocess.call(['python' , 'CreateWorkingFolders.py'])
    topdir = path
    os.chdir("Sims")
    shutil.copy(os.path.join(topdir,filename),os.getcwd()) 
    # the contents of tar.gz input should be individual sim folders 
    # check if file ends in tar.gzip if so do tar xvfz filename
    #subprocess.call(['unzip' , filename])
    subprocess.call(['tar','xvfz',filename])
    os.remove(filename)
    os.chdir(topdir)
    if(rscript):
        filename = rscript 
        os.chdir("Scripts")
        shutil.copy(os.path.join(topdir, "Filters", filename),os.getcwd())
        os.rename(filename, 'PostProcessFilter.R')
    os.chdir(topdir) 
    
def execute_graple(path):
    os.chdir(path)
    submit_response_string=subprocess.check_output(['python','SubmitGrapleBatch.py'])
    submitIDList=[]
    for i in submit_response_string.split("\n"):
        if "cluster" in i:
            submitIDList.append(i.split(" ")[5].split(".")[0])
    try:
        payload = {'key':str(path),'payload':pickle.dumps(submitIDList)}
        collection.insert_one(payload)
    except:
        print("Error in db access in execute_graple")
    
       
def process_graple_results(path):
    os.chdir(path)
    #subprocess.call(['python','ProcessGrapleBatchOutputs.py'])
    os.chdir(os.path.join(path,'Results'))
    # call tar czf output.tar.gz Sims
    #subprocess.call(['zip','-r','output.zip','Sims'])
    #subprocess.call(['tar','czf','output.tar.gz','Sims'])
    with tarfile.open('output.tar.gz', 'w:gz', compresslevel=9) as tar:
        for f in listdir('.'):
            if f.endswith('.bz2.tar'):
                tar.add(f)
            #os.remove(f) 
    os.chdir(path)
    
def check_Job_status(path):
    completed=True
    os.chdir(path)
    query = {'key':str(path)}
    try:
        submitIDList = pickle.loads(collection.find_one(query)['payload'])
    except:
        print ("error in db access in check_job_status")
        return "Job submission to Condor pool in progress."
    for each in submitIDList:
        cout = subprocess.check_output(['condor_q', each])
        if cout.count("\n")!=4:
            completed=False
            break
    if (completed):
        return "completed"
    else:
        return "processing"
        
def Abort_Job(path):
    os.chdir(path)
    query = {'key':str(path)}
    try:
        submitIDList = pickle.loads(collection.find_one(query)['payload'])
    except:
        print ("error in db access in Abort")
        return "Job submission to Condor pool is in progress., try again once \
        all jobs are submitted"
    for each in submitIDList:
        try:
            cout = subprocess.check_output(['condor_rm', each])
        except:
            pass
    return "All jobs marked for removal"
        
def ret_distribution_samples(distribution,samples,parameters):
    parameters = map(float,parameters)
        
    for each in parameters:
        print ("value:%s Type:%s length:%s"%(each,type(each),len(parameters)))
        
    if distribution=="uniform":
        return np.random.uniform(parameters[0],parameters[1],samples).tolist()
    elif distribution=="binomial":
        return np.random.binomial(parameters[0],parameters[1],samples).tolist()
    elif distribution=="normal":
        return np.random.normal(parameters[0],parameters[1],samples).tolist()
    elif distribution=="poisson":
        return np.random.poisson(parameters[0],parameters[1]).tolist()
        
# handles sweep string cases.    
def handle_sweep_run(dir_name,sweepstring):
        os.chdir(dir_name)
        current_dir = os.getcwd() 
        base_params = sweepstring.split("*")
        # parse parameters
        base_file=base_params[1]
        base_column=base_params[2]
        base_start=float(base_params[3])
        base_end=float(base_params[4])
        base_iterations=int(base_params[5])
        base_steps = ((base_end-base_start)/base_iterations)
        # clean Sims directory
        Sims_dir=os.path.join(current_dir, 'Sims')
        shutil.rmtree(Sims_dir)
        os.mkdir(Sims_dir)
  
        if len(base_params) > 6:   
            base_filename = base_params[6] 
            os.chdir("Scripts")
            shutil.copy(os.path.join(current_dir, 'base_folder',"sim.tar.gz"),os.getcwd())
            subprocess.call(['tar','xvfz','sim.tar.gz'])
            os.remove("sim.tar.gz") 
            shutil.copy(os.path.join(current_dir,"Filters", base_filename),os.getcwd())
            os.rename(base_filename, 'PostProcessFilter.R')
            os.chdir(current_dir)

        base_column = base_column.strip()        
        for i in range(1,base_iterations+2):
            os.chdir(Sims_dir)
            new_dir="Sim"+ str(i)
            os.mkdir(new_dir)
            os.chdir(new_dir)
            # replace sim.zip with sim.tar.gz
            shutil.copy(os.path.join(dir_name,'base_folder',"sim.tar.gz"),os.getcwd())
            #subprocess.call(['unzip' , "sim.zip"])
            subprocess.call(['tar','xvfz','sim.tar.gz'])
            #os.remove("sim.zip")
            os.remove("sim.tar.gz")
            data=pandas.read_csv(base_file)
            data = data.rename(columns=lambda x: x.strip()) 
            delta=base_start + (i-1)*base_steps
            print(str(base_column))
            print ("i "+str(i)+" base_steps"+str(base_steps)+" base_start"+str(base_start)+" delta"+str(delta) + " file"+ str(base_column))
	    data[base_column] = data[base_column].apply(lambda val:val+delta) 
            data.to_csv(base_file,index=False)
        execute_graple(dir_name)

# handles the core of distribution sweep jobs.
def handle_special_job(task, rscript):
    if (task.split('$')[0] == "graple_special_batch"):
        dir_name = task.split('$')[1]
        filename = task.split('$')[2]
        print dir_name
        print filename
      
        current_dir = os.path.join(os.getcwd(), dir_name) 
        base_folder = os.path.join(dir_name,'base_folder')
        # unzip the zip file and read job_desc.csv
        os.chdir(base_folder)
        #subprocess.call(['unzip' , filename])
        subprocess.call(['tar','xvfz',filename])
        # read job description from csv file
        f = open("job_desc.csv")
        csv_f = csv.reader(f)
        # list to hold job description
        job_description = []
        for row in csv_f:
            job_description.append(row)
        # get job parameters set up
        base_file = job_description[0][1]
        base_iterations = int(job_description[1][1])
        # columns to be affected and there properties
        # {col1:[], col2:[],col3[]......}
        columns = {}
        for i in range(2,len(job_description)):
            columns[job_description[i][0]]=[ret_distribution_samples(job_description[i][2],base_iterations,job_description[i][3:])]
            columns[job_description[i][0]].append(job_description[i][1])
            columns[job_description[i][0]].append(job_description[i][2])
        print str(columns)
        # clean Sims directory
        Sims_dir=os.path.join(dir_name,'Sims')
        shutil.rmtree(Sims_dir)
        os.mkdir(Sims_dir)
        # this list contains the changes made to csv file for every sim
        summary = []
        # create new csv files as per job description
        for i in range(1,base_iterations+1):
            summary.append(["sim_"+str(i)])
            os.chdir(Sims_dir)
            new_dir="Sim"+ str(i)
            os.mkdir(new_dir)
            os.chdir(new_dir)
            shutil.copy(os.path.join(dir_name,'base_folder',filename),os.getcwd())
            #subprocess.call(['unzip' , filename])
            subprocess.call(['tar','xvfz',filename])
            os.remove(filename)
            os.remove("job_desc.csv")
            #shutil.rmtree(os.path.join(os.getcwd(), 'FilterParams')) 
            data=pandas.read_csv(base_file)
            data = data.rename(columns=lambda x: x.strip())  
            for field in columns.keys():
                if (((" "+field) in data.columns) or (field in data.columns)):
                    # handle variations in filed names in csv file, some field names have leading spaces.
                    if " "+field in data.columns:
                        field_modified = " "+field
                    else:
                        field_modified = field
                    delta = columns[field][0][i-1]
                    if (columns[field][1]=="add"):
                        data[field_modified]=data[field_modified].apply(lambda val:val+delta)
                    elif (columns[field][1]=="sub"):
                        data[field_modified]=data[field_modified].apply(lambda val:val-delta)
                    elif (columns[field][1]=="mul"):
                        data[field_modified]=data[field_modified].apply(lambda val:val*delta)
                    elif (columns[field][1]=="div"):
                        data[field_modified]=data[field_modified].apply(lambda val:val/delta)
                    # make note of modified changes in a list datastructure
                    summary[i-1].append(field) # append column_name
                    summary[i-1].append(columns[field][2]) # append distribution
                    summary[i-1].append(columns[field][1]) # append operation
                    summary[i-1].append(str(delta)) # append delta
            # at this point the dataframe has been modified, write back to csv.
            data.to_csv(base_file,index=False)
        print str(summary)
        # write summary of modifications to a file.
        result_summary = open(os.path.join(base_folder,"sim_summary.csv"),'wb')
        wr = csv.writer(result_summary,dialect='excel')
        for row in summary:
            wr.writerow(row)
        result_summary.close()


        if(rscript):
            filename = rscript
            scripts_dir =  os.path.join(current_dir,'Scripts')
            os.chdir(scripts_dir)
            shutil.copy(os.path.join(current_dir, "Filters", filename),os.getcwd())
            os.rename(filename, 'PostProcessFilter.R')
            os.chdir(current_dir)

        # execute graple job
        execute_graple(dir_name)
        return
        
def ExtractDataFrame(input_file,output_file,var_list):
    #open input file for reading
        if (not(os.path.exists(input_file))):
            return False
        infile  = Dataset(input_file)
        #oprn output file for reading
        outfile = Dataset(output_file, "w",format="NETCDF3_CLASSIC")
        #dimension variables
        dims = []
        #Copy dimensions
        for dname, the_dim in infile.dimensions.iteritems():
            print dname, len(the_dim)
            dims.append(dname)
            outfile.createDimension(dname, len(the_dim) if not the_dim.isunlimited() else None)
        # Copy variables
        for v_name, varin in infile.variables.iteritems():
            if (v_name in var_list or v_name in dims):
                outVar = outfile.createVariable(v_name, varin.datatype, varin.dimensions)
                print ("name %s type %s"%(v_name,varin.datatype))
                # Copy variable attributes
                outVar.setncatts({k: varin.getncattr(k) for k in varin.ncattrs()})
                outVar[:] = varin[:]
        # close the output file
        outfile.close() 
        return True

@app.route('/GrapleRunMetSample', defaults={'filtername': None}, methods= ['GET', 'POST'])        
@app.route('/GrapleRunMetSample/<filtername>', methods= ['GET', 'POST'])        
def special_batch(filtername):
    global base_upload_path
    if request.method == 'POST':
        f = request.files['files']
        filename = f.filename
        response = {"uid":batch_id_generator()}
        dir_name = os.path.join(base_upload_path,response["uid"])
        os.mkdir(dir_name)
        base_folder = os.path.join(dir_name,'base_folder')
        os.mkdir(base_folder)
        topdir = dir_name
        os.chdir(base_folder)
        f.save(filename)
        os.chdir(topdir)
        copy_tree(base_graple_path,topdir)
        copy_tree(base_GLM_path,topdir)
        subprocess.call(['python' , 'CreateWorkingFolders.py'])
        # have to submit job to celery here--below method has to be handled by celery worker
        task_desc = "graple_special_batch"+"$"+dir_name+"$"+filename
        if(filtername):
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
    
@app.route('/GrapleAbort/<uid>', methods=['GET','POST'])
def abort_job(uid):
    global base_upload_path
    
    if request.method == 'GET':
        dir_name = os.path.join(base_upload_path,str(uid))
        status ={}
        status["curr_status"]=Abort_Job(dir_name)
        return jsonify(status)

@app.route('/TriggerSimulation/<sweepstring>', methods=['GET','POST'])
def return_file(sweepstring):
    global base_upload_path
    base_params = sweepstring.split("*")
    uid=base_params[0]
    if request.method == 'GET':
        dir_name = os.path.join(base_upload_path,str(uid))
        os.chdir(dir_name)
        contents = {}
        for each in os.listdir(dir_name):
            contents[each] = os.path.getsize(each)
        return jsonify(contents)
    
    if request.method == 'POST':
        dir_name = os.path.join(base_upload_path,str(uid))
        # put the task in the queue in the required format
        task_desc = "run_sweep"+"$"+dir_name+"$"+sweepstring
        doTask.delay(task_desc)
        response = {"response":"Job put in the task queue"}    
        return jsonify(response)
        
@app.route('/GrapleRun', defaults={'filtername': None}, methods= ['GET', 'POST'])
@app.route('/GrapleRun/<filtername>', methods= ['GET', 'POST'])
def upload_file(filtername):
    global base_upload_path
    if request.method == 'POST':
        f = request.files['files']
        filename = f.filename
        response = {"uid":batch_id_generator()}
        dir_name = os.path.join(base_upload_path,response["uid"])
        os.mkdir(dir_name)
        os.chdir(dir_name)
        f.save(filename)
        # should put the task in queue here and return.
        task_desc = "graple_run_batch"+"$"+dir_name+"$"+filename
        if (filtername): 
            doTask.delay(task_desc, filtername)
        else:  
            doTask.delay(task_desc)  
        return jsonify(response)


@app.route('/GrapleRunMetOffset', defaults={'filtername': None}, methods= ['GET', 'POST'])
@app.route('/GrapleRunMetOffset/<filtername>', methods= ['GET', 'POST'])              
def run_sweep(filtername):
    global base_upload_path
    if request.method == 'POST':
        f = request.files['files']
        filename = f.filename
        response = {"uid":batch_id_generator()}
        dir_name = os.path.join(base_upload_path,response["uid"])
        os.mkdir(dir_name)
        base_folder = os.path.join(dir_name,'base_folder')
        os.mkdir(base_folder)
        topdir = dir_name
        os.chdir(base_folder)
        f.save(filename)
        os.chdir(topdir)
        copy_tree(base_graple_path,topdir)
        copy_tree(base_GLM_path,topdir)
        subprocess.call(['python' , 'CreateWorkingFolders.py'])
        if(filtername):      
            os.chdir("Scripts")
    	    shutil.copy(os.path.join(topdir,filtername),os.getcwd())      
            os.chdir(topdir)
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
        if (status["curr_status"] != "completed"):
            ret_dict["status"]="Job under processing,please try agian after some time."
            return jsonify(ret_dict)
        process_graple_results(dir_name)
        #output_file = os.path.join(uid,'Results','output.zip')
        output_file = os.path.join(uid,'Results','output.tar.gz')
        url = url_for('static',filename=output_file) 
        ret_dict["output_url"]=url
        ret_dict["status"]="success"
        return jsonify(ret_dict)
        
@app.route('/GrapleRunResultsMetSample/<uid>', methods=['GET','POST'])
def return_distributionJob_consolidated_output(uid):
    global base_upload_path
    ret_dict = {}
    if request.method == 'GET':
        dir_name = os.path.join(base_upload_path,uid)
        # Sanity check to ensure that job processing is completed.
        status = {}
        status["curr_status"] = check_Job_status(dir_name)
        if (status["curr_status"] != "completed"):
            ret_dict["status"]="Job under processing,please try agian after some time."
            return jsonify(ret_dict)
        process_graple_results(dir_name)
        #result_zip = os.path.join(dir_name,'Results','output.zip')
        result_gz = os.path.join(dir_name,'Results','output.tar.gz')
        result_ar = os.path.join(dir_name,'Results','output.tar')
        summary_file_path = os.path.join(dir_name,"base_folder","sim_summary.csv")
        # club summary_file to the results
        shutil.copy(summary_file_path,os.getcwd())
        # use gunzip to unzip
        # 
        #subprocess.call(['zip' ,'-r',result_zip,'sim_summary.csv'])
        subprocess.call(['gunzip',result_gz])
        subprocess.call(['tar','rf',result_ar,'sim_summary.csv'])
        subprocess.call(['gzip',result_ar])
        os.remove('sim_summary.csv')
        #output_file = os.path.join(uid,'Results','output.zip')
        output_file=os.path.join(uid,'Results','output.tar.gz')
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
        scriptsDir = os.path.join(base_graple_path, "Filters")
        if(os.path.exists(scriptsDir)):
            filesList = os.listdir(scriptsDir) 
    return json.dumps(filesList)        

@app.route('/GrapleGetVersion', methods=['GET'])
def get_version():
    version = {}
    if request.method == 'GET':
        #code for getting the current version of graple service
        version = {"version": "1.0.2"} 
    return jsonify(version)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')

