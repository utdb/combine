import logging
import argparse
import json
import engine
from modules import extract_analyze


class BearingsCollector(engine.Activity):

    def setup(self, args):
        # print("BEARING COLLECTOR STARTED")
        self

    def handle_simple(self, obj):
        jd = obj.json_data
        ef = jd['extraction_fields']
        if False:
            print('(C): '+str(jd['extraction_kindtags']['kind'])+'[x<'+jd['extraction_module']+'> - '+str(jd['url']))
            print(json.dumps(ef, indent='   ') )
        return []


def get_handler(context):
    return BearingsCollector(context)
