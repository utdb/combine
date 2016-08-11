import logging
import argparse
import json
import engine


class SeedJsonHandler(engine.Activity):

    def setup(self, args):
        parser = argparse.ArgumentParser()
        parser.add_argument('-k', '--kind', required=True)
        parser.add_argument('-t', '--tag', required=True)
        args = vars(parser.parse_args(args))
        self.kind = args['kind']
        tag = args['tag']
        if len(tag) == 0:
            self.tags = []
        else:
            self.tags = [tag]
        # print("Activity triggers are "+str(self.triggers()))

    def handle(self, activation, obj):
        activation.input(obj)
        # read the seeds from the file in the start seed file
        with open(obj.text()) as data_file:    
            data = json.load(data_file)
        for item in data:
            json_rfc_fields = json.dumps(item, indent='   ')
            activation.output(engine.LwObject(self.kind, self.tags, "application/text", json_rfc_fields, None))


def get_handler(context):
    return SeedJsonHandler(context)
