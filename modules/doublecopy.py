import logging

def handle_object(db,job,activity,o):
    logging.info(__name__+": handle_object(aid="+str(activity.aid())+",oid="+str(o.oid())+") start")
    #
    # Doing it the easy way
    #
    activity.start_activation()
    newobj = activity.add_object("doublecopykind",["doublecopytag"],"application/text","DOUBLECOPY("+str(o.content())+")")
    activity.add_activation_in(o)
    activity.finish_activation()
    #
    logging.info(__name__+": handle_object(aid="+str(activity.aid())+",oid="+str(o.oid())+") finish")
