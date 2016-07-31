import logging
import engine


class CopyHandler(engine.Activity):

    # def setup(self, args):
        # print("* Handle args here: "+args)

    def handle(self, activation, obj):
        activation.input(obj)
        res = engine.LwObject("copykind", ["copytag"], "application/text", "COPY("+str(obj.content())+")")
        activation.output(res)


def get_handler(context):
    return CopyHandler(context)
