from threading import Thread
import logging
import configparser

import storage

class activity_handler:

    def __init__(self,db,activity):
        self.db = db
        self.activity = activity
        self.module =  __import__(self.activity.module(), fromlist=[''])
        logging.info("activity_handler:"+self.activity.module() + " start")

    def handle_object(self,o):
        logging.info("activity_handler:"+self.activity.module() + " handle_object:"+self.activity.module() + "|" + str(o.oid()))
        self.module.handle_object(self.db,self.activity,o)

def run_job(configfile,job):
    db = storage.opendb(configfile)
    active = {}
    while True:
        todo = db.objects_todo(job)
        if len(todo) == 0:
            logging.info("run_job: no more todo")
            break
        else:
            logging.info("run_job: get todo: len="+str(len(todo)))
        for ao in todo:
            a = active.get(ao[0])
            if a is None:
                a = activity_handler(db,db.get_activity(ao[0]))
                active[ao[0]] = a
            a.handle_object(db.get_object(ao[1]))
    db.closedb()

def start(configfile):
    """
    Run the Combine engine
    """
    logging.info("Running the Combine Web Harvesting engine!")
    db = storage.opendb(configfile)
    joblist = []
    for job in db.active_jobs():
        jobthread = Thread(name="Job:"+job.name(),target=run_job,args=(configfile,job,))
        joblist.append((job,jobthread))
        jobthread.start()
    # db.closedb() error
