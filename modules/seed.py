import logging
import argparse
import engine


class SeedHandler(engine.Activity):

    def setup(self, args):
        parser = argparse.ArgumentParser()
        parser.add_argument('-k', '--kind', required=True)
        parser.add_argument('-t', '--tag', required=True)
        args = vars(parser.parse_args(args))
        self.kind = args['kind']
        self.tags = [args['tag']]
        # print("Activity triggers are "+str(self.triggers()))

    def handle(self, activation, obj):
        activation.input(obj)
        # read the seeds from the file in the start seed file
        with open(obj.text(), "r") as f:
            for seed in f:
                activation.output(engine.LwObject(self.kind, self.tags, "application/text", seed.strip(), None))


def get_handler(context):
    return SeedHandler(context)
