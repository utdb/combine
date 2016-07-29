import sys
import logging
import storage
import engine

def create_schedule(configfile):
    logging.info("Create new Schedule")
    db = storage.opendb(configfile)
    db.destroy()
    db.create()

    context = db.add_context("globalctx","Global Context")
    job     = db.add_job(context,"myfirstjob","This is my first job")
    db.add_activity(job,  "modules.copy", "argsxxx", (["mykind", ["tag1", "tag2"]], ))
    db.add_activity(job,"modules.doublecopy","argsyyy",(["copykind",["copytag"]],))
    db.add_object(job,None,"mykind",["tag1","tag2"],"application/text","Hello World 1")
    job.start()
    db.closedb()

#logging.basicConfig(filename='combine.log',level=logging.INFO)
logging.basicConfig(stream=sys.stdout,level=logging.INFO)

if __name__ == '__main__':
    configfile = "combine.local.cfg"
    create_schedule(configfile)
    engine.start(configfile)
