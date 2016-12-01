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

    def handle_simple(self, obj):
        # activation.input(obj)
        # read the seeds from the file in the start seed file
        with open(obj.str_data()) as data_file:
            data = json.load(data_file)
        result = []
        for item in data:
            # print("seed_json: generate: "+json.dumps(item, indent='   '))
            result.append(engine.LwObject(self.kindtags_default, {'Content-Type': 'application/text', 'encoding': 'utf-8'}, "", None, item, obj.sentence))
        return result


def get_handler(context):
    return SeedJsonHandler(context)
