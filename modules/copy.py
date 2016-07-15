
def handle_object(db,activity,o):
    print("copy.py: handle_object: "+str(o.oid()))
    #
    avid = db.add_activation(activity.aid())
    ocopy = db.add_object(activity.jid(),avid,"copykind",["copytag"],"application/text","Copy World 2")
    db.set_activation_graph(avid,(o.oid(),),(ocopy,))
    db.add_log(avid,"activation_finished","")
