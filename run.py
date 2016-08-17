import sys
import logging
import storage
import engine


def create_example_schedule(configfile):
    logging.info("Create new Example Schedule")
    db = storage.opendb(configfile)
    if True:
        # initialize a fresh database
        db.destroy()
        db.create()
    context = db.add_context("globalctx", "Global Context")
    job = db.add_job(context, "myfirstjob", "This is my first job")
    db.add_activity(job,  "modules.copy", "argsxxx", (["mykind", ["tag1", "tag2"]], ))
    db.add_activity(job, "modules.doublecopy", "argsyyy", (["copykind", ["copytag"]], ))
    db.add_object(job, None, "mykind", ["tag1", "tag2"], "application/text", "Hello World 1", None)
    job.start()
    db.closedb()

JOBNAME = 'bearings-crawl'


def create_bearings_schedule(configfile):
    logging.info("Create new Bearings Schedule")
    db = storage.opendb(configfile)
    if True:
        # initialize a fresh database
        db.destroy()
        db.create()
    context = db.add_context(JOBNAME, "Bearing Crawl Context")
    job = db.add_job(context, JOBNAME, "Bearings Crawl Job")
    db.add_activity(job,
                    "modules.seed_json",
                    "--kind=rfc_entity,--tag=",
                    ([{'kind': 'rfc_entity_seed', 'tags': []}, ] ))
    db.add_activity(job,
                    "modules.abf_detail_url", "",
                    ([{'kind': 'rfc_entity', 'tags': []}, ] ))
    # db.add_activity(job,
                    # "modules.abf_fetch", "",
                    # ([{'kind': "abf_detail_url", 'tags': []}, ] ))
    # db.add_activity(job,
                    # "modules.abf_extract_fields", "",
                    # ([{'kind': "abf_detail_page", 'tags': []}, ] ))
    # db.add_activity(job,
                    # "modules.rfc-x-abf-cmp", "",
                    # ([{'kind': "rfc_entity", 'tags': []}, {'kind': "abf_entity", 'tags': []} ))
    db.add_seed_data(job, [engine.LwObject({'kind':'rfc_entity_seed', 'tags': []}, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, "./data/rfc.in.test.json", None),])
    job.start()
    db.closedb()


def open_bearings_schedule(configfile):
    logging.info("Open new Bearings Schedule")
    db = storage.opendb(configfile)
    job = db.get_job(name=JOBNAME)
    job.delete_objects(activity="modules.abf_extract_fields")

logging.basicConfig(filename='combine.log', level=logging.INFO)
# logging.basicConfig(stream=sys.stdout, level=logging.INFO)

if __name__ == '__main__':
    configfile = "combine.local.cfg"
    # create_example_schedule(configfile)
    create_bearings_schedule(configfile)
    # open_bearings_schedule(configfile)
    engine.start(configfile)
