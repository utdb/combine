import logging
import argparse
import json
import engine


class BookTop1000Seed(engine.Activity):

    def handle_simple(self, obj):
        # activation.input(obj)
        # read the seeds from the file in the start seed file
        result = []
        print("top1000_json: "+json.dumps(obj.json_data, indent='   '))
        url = obj.json_data.get('url')
        return [ engine.LwObject(self.kindtags_default, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, url, None, obj.json_data, obj.sentence) ]


def get_handler(context):
    return BookTop1000Seed(context)
