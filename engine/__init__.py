from threading import Thread
import configparser

import storage

class running_activity:

    def __init__(self,db,activity):
        self.db = db
        self.activity = activity
        self.module =  __import__("modules."+activity.module(), fromlist=[''])
        print("START ACTIVITY:"+activity.module())

    def handle_object(self,o):
        print("HANDLE_OBJECT:"+self.activity.module() + "|" + str(o.oid()))
        self.module.handle_object(self.db,self.activity,o)

def start_job(configfile,job):
    print("Starting Combine Job: "+job.name() + "[" +str(job.jid())+"]")
    db = storage.opendb(configfile)
    todo = db.objects_todo(job)
    for ao in todo:
        a = running_activity(db,db.get_activity(ao[0]))
        a.handle_object(db.get_object(ao[1]))

def start(configfile):
    """
    Run the Combine engine
    """
    print("Running the Combine Web Harvesting engine!")
    db = storage.opendb(configfile)
    joblist = []
    for job in db.active_jobs():
        jobthread = Thread(name="Job:"+job.name(),target=start_job,args=(configfile,job,))
        joblist.append((job,jobthread))
        jobthread.start()
    # db.closedb() error
