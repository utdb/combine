import storage.postgres

db = storage.postgres.opendb("./combine.local.cfg")
db.init()
db.create()
db.destroy()

