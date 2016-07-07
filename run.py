import storage.initialize

db = storage.initialize.opendb("./combine.local.cfg")
db.create()
xxx = db.add_object("XXX")
yyy = db.add_object("YYY")
db.add_provenance(xxx,yyy)
db.destroy()

