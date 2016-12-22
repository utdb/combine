import json
import logging
import storage
import engine
from engine import Engine


class Configurator:

    def __init__(self, configfile, initialize=False):
        self.configfile = configfile
        self.db = storage.opendb(configfile)
        if initialize:
            # initialize a fresh database
            self.db.destroy()
            self.db.create()
            self.db.commit()

    def close(self):
        self.db.close()
        self.db = None

    def load_configuration(self, configuration):
        logging.info("Create new JSON Bearings Schedule")
        with open(configuration) as data_file:
            for d in json.load(data_file):
                (k, v) = d.popitem()
                if k == 'create_context':
                    context = self.db.add_context(v['name'], v['description'])
                elif k == 'create_job':
                    context = self.db.get_context(v['context'])
                    job = self.db.add_job(context, v['name'], v['description'])
                    for a in v['activities']:
                        activity = a['activity']
                        job.add_activity(activity['module'], activity['args'],
                                         activity['kindtags_in'],
                                         activity['kindtags_out'])
                    seedobj = []
                    for o in v['seed_data']['objects']:
                        obj = o['object']
                        seedobj.append(engine.LwObject(obj['kindtags'],
                                       obj['metadata'], obj['str_data'],
                                       obj['bytes_data'], obj['json_data'],
                                       obj['sentence']))
                    job.add_seed_data(seedobj)
                    job.start()
                else:
                    raise ValueError('unknown JSON object: ' + k)
        self.db.commit()
