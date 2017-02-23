import logging
import argparse
from collections import defaultdict
import json
import util.flowcontrol
import engine


MAX_FAILURES = 1

class ExtractAnalyze(engine.Activity):

    def setup(self, args):
        self.fail = defaultdict(lambda: 0)

    def is_fail(self, ef):
        if len(ef) <= 1:
            return True
        for k in ef.keys():
            if ( len(k) > 128 or len(k) == 0 ):
                return True
        return False

    def analyze_entity(self, obj, do_compare):
        jd = obj.json_data
        # print('(A): '+str(jd['extraction_kindtags']['kind'])+'[x<'+jd['extraction_module']+'> - '+str(jd['url']))
        extractor = jd['extraction_id']
        if self.fail[extractor] < 0:
            # already stopped
            return None
        if self.is_fail(obj.json_data['extraction_fields']):
            # print("EXTRACT_FAIL")
            self.fail[extractor] += 1
            if self.fail[extractor] >= MAX_FAILURES:
                self.fail[extractor] = -1
                return util.flowcontrol.stop_object(extractor, jd['extraction_kindtags'])
        return None

    def handle_complex(self, obj):
        newobj = self.analyze_entity(obj, True)
        if newobj is not None:
            activation = self.new_activation([obj, ], newobj)


def get_handler(context):
    return ExtractAnalyze(context)
