import logging
import engine


class CopyHandler(engine.Activity):

    # def setup(self, args):
        # print("* Handle args here: "+args)

    def handle(self, activation, obj):
        # print("Activity triggers are "+str(self.triggers()))
        activation.input(obj)
        res = engine.LwObject("copykind", ["copytag"], "application/text", "COPY("+str(obj.text())+")", None)
        activation.output(res)


def get_handler(context):
    return CopyHandler(context)
