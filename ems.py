#!/usr/bin/env python

from pymongo import MongoClient
from os import listdir
import datetime, os, tarfile, time, subprocess, shutil, smtplib, logging, signal, sys, pytz, email
from email.mime.text import MIMEText
from email.header import Header
from gwsconfig import gwsconf

serv_dl_addr = gwsconf['serv_addr'] + gwsconf['download_endpoint']

base_working_path = gwsconf['base_working_path']
base_upload_path = os.path.join(base_working_path, 'Experiments')
base_graple_path = os.path.join(base_working_path, 'GRAPLE_SCRIPTS')
base_filter_path = os.path.join(base_working_path, 'Filters')
base_result_path = os.path.join(base_working_path, 'Results')

db_client = MongoClient() # running at default port
db = db_client[gwsconf['graple_db_name']]
collection = db[gwsconf['graple_coll_name']]
api_collection = db[gwsconf['api_coll_name']]

refresh_delay = gwsconf['refresh_delay']


email_template = '''
Dear {username}, 

Your experiment {expname} has completed processing and is available for download.
Please download your experiment results before it expires. 

Details:
    Experiment Name: {expname}
    Experiment ID: {expid}
    Submitted: {subtime}
    Completed: {comptime}
    Expiry: {exptime}
    Link to Results: {reslink}

Thanks!

NOTE: 
All times are according to {time_zone}.
Replies to this email are not monitored. 
'''
#while(True)
#   last_time = now()
#   query status == 2
#       for each cluster ID set
#       condor_q for each cluster ID set
#       if complete:
#           consolidate to output.tar.gz
#           delete experiment files (base_upload_path/uid)
#           set 'expiry' in DB
#           set 'status' = 3
#       set 'progress' in DB (all sets in one shot, to avoid concurrency issues)
#
#   query status == 5
#       condor_rm all cluster IDs in all sets
#       delete experiment files
#       delete uid from base_result_path
#       set status = 6
#
#   query 'expiry' <= current time
#       delete uid from base_result_path
#       set status = 6
#       
#   if now() < last_time + refresh_delay
#       sleep(last_time + refresh_delay - now())

class ignoreKbInt():
    def __enter__(self):
        self.KbIntWaiting = False
        self.prevhand = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self.dummyhandler)

    def dummyhandler(self, sig, frame):
        self.KbIntWaiting = (sig, frame)
        print "Kb interrupt waiting"

    def __exit__(self, type, value, traceback):
        signal.signal(signal.SIGTERM, self.prevhand)
        if self.KbIntWaiting:
            print "Exiting..."
            self.prevhand(*self.KbIntWaiting)

def convert_tz(local_time, new_tz):
    if new_tz == '':
        return local_time
    local_tz = pytz.timezone("US/Eastern")
    return local_tz.localize(local_time).astimezone(pytz.timezone(new_tz))

def process_graple_results(uid, retention):
    global base_upload_path, base_result_path
    res_dir = os.path.join(base_result_path, uid) # can change uid to random folder name for security
    tarfn = os.path.join(res_dir, 'output.tar.gz')
    if not os.path.isfile(tarfn):
        os.mkdir(res_dir)
        exp_res_dir = os.path.join(base_upload_path, uid, 'Results')
        bzlist = [bzfn for bzfn in listdir(exp_res_dir) if bzfn.endswith('.tar.bz2')]
        subprocess.call(['parallel', 'tar', 'jxf', ':::'] + bzlist, cwd = exp_res_dir)
        subprocess.call(['rm'] + bzlist, cwd = exp_res_dir)
        cons_script = os.path.join(exp_res_dir, 'ConsolidateResults.py')
        if os.path.isfile(cons_script):
            subprocess.call(['python', cons_script])
        subprocess.call(['tar', 'I', 'pigz', '-cf', tarfn] + listdir(exp_res_dir), cwd = exp_res_dir)
        #with tarfile.open(tarfn, 'w:gz', compresslevel=9) as tar:
        #    for f in listdir(os.path.join(base_upload_path, uid, 'Results')):
        #        if f == 'Sims' or f == 'sim_summary.csv':
        #            tar.add(os.path.join(exp_res_dir, f), f)
    shutil.rmtree(os.path.join(base_upload_path, uid))
    comp_time = datetime.datetime.now()
    update_doc = {'status':3, 'progress':100.0, 'completed':comp_time}
    if retention != 0:
        update_doc['expiry'] = (gwsconf['retention_unit'] * retention) + comp_time # days = retention
    collection.update_one({'key':uid}, {'$set':update_doc})
    dbdoc = collection.find_one({'key':uid})
    apidbdoc = api_collection.find_one({'key':dbdoc['apikey']})
    if 'email' in dbdoc and len(dbdoc['email']) > 0:
        expName = dbdoc['expname'] if len(dbdoc['expname']) > 0 else dbdoc['key']
        emailmsg = MIMEText(email_template.format(expname = expName,
            username = apidbdoc['name'].split()[0],
            time_zone = apidbdoc['tz'],
            expid = dbdoc['key'],
            subtime = convert_tz(dbdoc['submitted'], apidbdoc['tz']).ctime(),
            reslink = serv_dl_addr + dbdoc['key'],
            comptime = convert_tz(dbdoc['completed'], apidbdoc['tz']).ctime(),
            exptime = convert_tz(dbdoc['expiry'], apidbdoc['tz']).ctime() if 'expiry' in dbdoc else 'On first download'))
        emailmsg['From'] = email.utils.formataddr((str(Header('Graple Notifier', 'utf-8')), gwsconf['smtp_user']))
        emailmsg['To'] = dbdoc['email']
        emailmsg['Subject'] = "GRAPLE Experiment " + expName + " complete"
        smtpserv = smtplib.SMTP(gwsconf['smtp_server'])
        #smtpserv.ehlo()
        #smtpserv.starttls()
        #smtpserv.login(gwsconf['smtp_user'], gwsconf['smtp_pass'])
        smtpserv.sendmail(gwsconf['smtp_user'], [emailmsg['To']], emailmsg.as_string())
        smtpserv.quit()
    return os.path.join(uid, 'output.tar.gz')

def process_once():
    for dbdoc in collection.find({'status':2}):
        condor_command = ['condor_history'] + map(str, dbdoc['payload']) + ['-format', '%d', 'JobStatus']
        cout = subprocess.check_output(condor_command)
        prog = round(float(cout.count('4'))/len(dbdoc['payload'])*100, 2)
        if prog == 100:
            process_graple_results(dbdoc['key'], dbdoc['retention'])
        elif dbdoc['progress'] != prog:
            collection.update_one({'key':dbdoc['key']}, {'$set':{'progress':prog}})

    for dbdoc in collection.find({'status':5}):
        if 'payload' in dbdoc:
            condor_command = ['condor_rm'] + map(str, dbdoc['payload'])
            subprocess.call(condor_command)
        if os.path.isdir(os.path.join(base_upload_path, dbdoc['key'])):
            shutil.rmtree(os.path.join(base_upload_path, dbdoc['key']))
        if os.path.isdir(os.path.join(base_result_path, dbdoc['key'])):
            shutil.rmtree(os.path.join(base_result_path, dbdoc['key']))
        collection.update_one({'key':dbdoc['key']}, {'$set':{'status':6}})

    for dbdoc in collection.find({'expiry':{'$lt':datetime.datetime.now()}, 'status':{'$ne':6}}):
        if os.path.isdir(os.path.join(base_upload_path, dbdoc['key'])):
            shutil.rmtree(os.path.join(base_upload_path, dbdoc['key']))
        if os.path.isdir(os.path.join(base_result_path, dbdoc['key'])):
            shutil.rmtree(os.path.join(base_result_path, dbdoc['key']))
        collection.update_one({'key':dbdoc['key']}, {'$set':{'status':6}})


while True:
    last_check_time = datetime.datetime.now()
    with ignoreKbInt():
        process_once()
    try:
        if datetime.datetime.now() < last_check_time + refresh_delay:
            sleep_time = last_check_time + refresh_delay - datetime.datetime.now()
            time.sleep(sleep_time.total_seconds())
    except:
        print "Exited while sleeping.."
        sys.exit()
