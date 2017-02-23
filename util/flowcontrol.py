import json
import engine
from collections import defaultdict


class FlowControl():

    def __init__(self, name, sentence_id):
        self.name = name
        self.sentence_id = sentence_id
        self.stopped = defaultdict(lambda: False)

    def is_active(self, obj): 
        kind = obj.kindtags['kind']
        if kind == 'extract_notify':
            jd = obj.json_data
            if jd['notify'] == 'stop':
                notify_kind = jd['extraction_kindtags']['kind']
                if jd['extraction_id'] == self.sentence_id:
                    print('STOPPING '+ self.name + ' EXTRACTOR id = '+str(self.sentence_id)+' for kind: '+notify_kind)
                    self.stopped[notify_kind] = True
            return False
        if self.stopped[kind]:
            # print('STOPPED ignoring '+kind+' object')
            return False
        return True


def stop_object(extraction_id, extraction_kindtags):
    return [engine.LwObject({'kind': "extract_notify", 'tags': []}, None, "", None, {'notify': 'stop', 'extraction_id': extraction_id, 'extraction_kindtags': extraction_kindtags}, None), ]

