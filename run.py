import storage

db = storage.opendb("./combine.local.cfg")
db.destroy()
db.create()
aaa = db.add_activity(66,"A-MODULE")
act = db.add_activation(aaa)
xxx = db.add_object(act,"KIND","application/text","Hello")
yyy = db.add_object(act,"KIND","application/text","Hello")
db.set_activation_graph(act,(xxx,),(yyy,))
db.add_log(act,"activation_finished","")
