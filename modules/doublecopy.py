import logging
import engine

class copy_handler(engine.basic_handler):

    def __init__(self,context):
        super(copy_handler,self).__init__(context)
        # print("* Handle args here: "+context['args'])

    def handle_object(self,o):
        # 1/0 # exception testing
        newobj = self.create_object("doublecopykind",["doublecopytag"],"application/text","DOUBLECOPY("+str(o.content())+")")
        self.add2in(o)
        self.add2out(newobj)

#
#
#

def get_handler(context):
    return copy_handler(context)
