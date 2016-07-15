import logging
import storage
import engine

def create_schedule(configfile):
    logging.info("Create new Schedule")
    db = storage.opendb(configfile)
    db.destroy()
    db.create()

    cid   = db.add_context("globalctx","Global Context")
    jid   = db.add_job(cid,"myfirstjob","This is my first job")
    acopy = db.add_activity(jid,"copy",(["mykind",["tag1","tag2"]],))
    act = db.add_activation(acopy)
    o1 = db.add_object(jid,act,"mykind",["tag1","tag2"],"application/text","Hello World 1")
    #
    j = db.get_job(jid)
    j.start()
    #
    db.closedb()

logging.basicConfig(filename='combine.log',level=logging.INFO)

if __name__ == '__main__':
    configfile = "combine.local.cfg"
    create_schedule(configfile)
    engine.start(configfile)
