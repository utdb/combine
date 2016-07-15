import storage
import engine

def create_schedule(configfile):
    db = storage.opendb(configfile)
    db.destroy()
    db.create()

    cid   = db.add_context("globalctx","Global Context")
    jid   = db.add_job(cid,"myfirstjob","This is my first job")
    acopy = db.add_activity(jid,"copyjob",(["mykind",["tag1","tag2"]],))
    act = db.add_activation(acopy)
    o1 = db.add_object(jid,act,"mykind",["tag1","tag2"],"application/text","Hello World 1")
    o2 = db.add_object(jid,act,"mykind",["tag1","tag2"],"application/text","Hello World 2")
    db.set_activation_graph(act,(o1,),(o2,))
    db.add_log(act,"activation_finished","")
    #
    j = db.get_job(jid)
    j.start()
    #
    mod = __import__("modules.copy", fromlist=[''])
    mod.run_activation(db,(o1,o2))
    
    print("j.name="+str(j.name()))
    o = db.get_object(o1)
    print("o.kind="+str(o.kind()))
    # o.activity()
    #
    db.closedb()

if __name__ == '__main__':
    configfile = "combine.local.cfg"
    create_schedule(configfile)
    engine.start(configfile)
