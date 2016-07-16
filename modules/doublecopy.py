import logging

def handle_object(db,job,activity,o):
    logging.info(__name__+": handle_object(aid="+str(activity.aid())+",oid="+str(o.oid())+") start")
    #
    activation = db.add_activation(activity.aid())
    ocopy = db.add_object(job,activation,"doublecopykind",["doublecopytag"],"application/text","DOUBLECOPY("+str(o.content())+")")
    db.set_activation_graph(activation,(o,),(ocopy,))
    db.add_log(activation.avid(),"activation_finished","")
    logging.info(__name__+": handle_object(aid="+str(activity.aid())+",oid="+str(o.oid())+") finish")
