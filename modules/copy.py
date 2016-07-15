import logging

def handle_object(db,activity,o):
    logging.info(__name__+": handle_object(aid="+str(activity.aid())+",oid="+str(o.oid())+") start")
    #
    avid = db.add_activation(activity.aid())
    ocopy = db.add_object(activity.jid(),avid,"copykind",["copytag"],"application/text","COPY("+str(o.content())+")")
    db.set_activation_graph(avid,(o.oid(),),(ocopy,))
    db.add_log(avid,"activation_finished","")
    logging.info(__name__+": handle_object(aid="+str(activity.aid())+",oid="+str(o.oid())+") finish")
