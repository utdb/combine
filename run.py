import storage
import engine

db = storage.opendb("./combine.local.cfg")
db.destroy()
db.create()
aaa = db.add_activity(66,"A-MODULE",(["regular","tocopy"]))
act = db.add_activation(aaa)
xxx = db.add_object(act,"regular","application/text","Hello World 1")
yyy = db.add_object(act,"regular","application/text","Hello World 2")
db.set_activation_graph(act,(xxx,),(yyy,))
db.add_log(act,"activation_finished","")
#
engine.start("./combine.local.cfg",db)

mod = __import__("modules.copy", fromlist=[''])
mod.run_activation(db,(xxx,yyy))

o = db.get_object(xxx)
print("o.kind="+str(o.kind()))
# o.activity()
