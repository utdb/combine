import sys
import os.path
import argparse
import logging
import json
import storage
import engine
from engine import Engine


JOBNAME = 'bearings-crawl'


def open_bearings_scenario(configfile):
    logging.info("Open new Bearings Schedule")
    combine_engine = Engine(configfile)
    job = combine_engine.db.get_job(name=JOBNAME)
    module = 'modules.abf_extract_fields'
    print('RESETTING module: ' + module)
    activities = job.activities(module)
    combine_engine.scheduler.reset_activity(next(activities),job)
    combine_engine.scheduler.commit()
    combine_engine.run()
    combine_engine.stop()

def load_scenario(configfile, scenario):
    logging.info("Create new JSON Bearings Schedule")
    db = storage.opendb(configfile)
    if True:
        # initialize a fresh database
        db.destroy()
        db.create()
        db.commit()
    with open(scenario) as data_file:    
        for d in json.load(data_file):
            (k, v) = d.popitem()
            if k == 'create_context':
                context = db.add_context(v['name'], v['description'])
            elif k == 'create_job':
                context = db.get_context(v['context'])
                job = db.add_job(context, v['name'], v['description'])
                for a in v['activities']:
                    activity = a['activity']
                    job.add_activity(activity['module'], activity['args'], activity['kindtags_in'], activity['kindtags_out'])
                seedobj = []
                for o in v['seed_data']['objects']:
                    obj = o['object']
                    seedobj.append(engine.LwObject(obj['kindtags'], obj['metadata'], obj['str_data'], obj['bytes_data'], obj['json_data'], obj['sentence']))
                job.add_seed_data(seedobj)
                job.start()
            else:
                raise ValueError('unknown JSON object: ' + k)
    db.commit()
    db.closedb()
    #
    combine_engine = Engine(configfile)
    combine_engine.run()
    combine_engine.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', metavar="configfile",  default='./master.local.cfg', help="specify path of the config file", required=False)
    parser.add_argument('-l', '--logfile', metavar="logfile",  default='./combine.log', help="specify path of the log file", required=False)
    parser.add_argument("--slave", action="store_true", help="always run as slave")
    args = vars(parser.parse_args())
    configfile = args['config']
    logfile = args['logfile']
    #
    logging.basicConfig(filename=logfile, level=logging.INFO)
    # logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    #
    if not os.path.isfile(configfile):
        print("Bad configfile: "+configfile)
        sys.exit()
    #
    if args['slave']:
        combine_engine = Engine(configfile)
        combine_engine.run()
        combine_engine.stop()
    else:
        # storage.postgres.test_listener(configfile)
        load_scenario(configfile, './bearing_crawl.json')
        # open_bearings_scenario(configfile)

