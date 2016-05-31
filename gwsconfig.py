
import datetime

gwsconf = {
    'refresh_delay' : datetime.timedelta(seconds = 30),
    'smtp_server' : 'smtp.gmail.com:587',
    'smtp_user' : 'example',
    'smtp_pass' : 'example',
    'base_working_path' : '/home/grapleadmin/grapleService',
    'serv_addr' : 'http://graple.acis.ufl.edu',
    'download_endpoint' : '/DownloadResults/',
    'retention_unit' : datetime.timedelta(days = 1),
    'retention_after_dl' : datetime.timedelta(hours = 1),
    'graple_db_name' : 'grapleDB',
    'graple_coll_name' : 'grapleColl',
}
