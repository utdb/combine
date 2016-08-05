import logging
import engine


class DoubleCopyHandler(engine.Activity):

    def setup(self, args):
        print("* Handle DoubleCopy args here: "+str(args))

    def handle(self, activation, obj):
        activation.input(obj)
        res = engine.LwObject("doublecopykind", ["doublecopytag"], "application/text", "DOUBLECOPY("+str(obj.text())+")", None)
        activation.output(res)


def get_handler(context):
    return DoubleCopyHandler(context)
