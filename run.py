import sys
import os.path
import argparse
import logging
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
    combine_engine.scheduler.reset_activity(next(activities))
    combine_engine.scheduler.commit()
    combine_engine.run()
    combine_engine.stop()


def create_bearings_scenario(configfile):
    logging.info("Create new Bearings Schedule")
    db = storage.opendb(configfile)
    if True:
        # initialize a fresh database
        db.destroy()
        db.create()
        db.commit()
    context = db.add_context(JOBNAME, "Bearing Crawl Context")
    job = db.add_job(context, JOBNAME, "Bearings Crawl Job")
    job.add_activity("modules.seed_json",
                     "--kind=rfc_entity,--tag=",
                     ([{'kind': 'rfc_entity_seed'}, ]),
                     ([{'kind': 'rfc_entity'}, ]))
    job.add_activity("modules.abf_detail_url", "",
                     ([{'kind': 'rfc_entity'}, ]),
                     ([{'kind': 'abf_detail_url'}, ]))
    job.add_activity("modules.abf_fetch", "",
                     ([{'kind': "abf_detail_url"}, ]),
                     ([{'kind': 'abf_detail_page'}, ]))
    job.add_activity("modules.abf_extract_fields", "",
                     ([{'kind': "abf_detail_page"}, ]),
                     ([{'kind': 'abf_entity'}, ]))
    job.add_activity("modules.rfc-x-abf-cmp", "",
                     ([{'kind': "rfc_entity"}, {'kind': "abf_entity"}]),
                     ([{'kind': 'UNKOWN'}, ]),
                     False)
    job.add_seed_data([engine.LwObject({'kind': 'rfc_entity_seed', 'tags': []}, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, "./data/rfc.in.test.json", None, None), ])
    job.start()
    db.commit()
    #
    combine_engine = Engine(configfile)
    combine_engine.run()
    combine_engine.stop()
    #
    # open_bearings_scenario(configfile)
    #
    db.closedb()


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
        create_bearings_scenario(configfile)
        # open_bearings_scenario(configfile)

