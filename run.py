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
                    "modules.seed",
                    "--kind=bearing_seed_id,--tag=",
                    (["bearing_seed", []], ))
    db.add_activity(job,
                    "modules.abf_detail_url", "",
                    (["bearing_seed_id", []], ))
    db.add_activity(job,
                    "modules.abf_fetch", "",
                    (["abf_detail_url", []], ))
    db.add_activity(job,
                    "modules.abf_extract_fields", "",
                    (["abf_detail_page", []], ))
    db.add_activity(job,
                    "modules.abf_deduplicate", "",
                    (["abf_entity_fields", []], ))
    #
    #
    db.add_object(job, None, "bearing_seed", [], "application/text", "./cache/bearing_seed.txt", None)
    job.start()
    db.closedb()

def open_bearings_schedule(configfile):
    logging.info("Open new Bearings Schedule")
    db = storage.opendb(configfile)
    job = db.get_job(name=JOBNAME)
    # job.delete_objects(activity="modules.abf_extract_fields")
    job.delete_objects(activity="modules.abf_deduplicate")

logging.basicConfig(filename='combine.log', level=logging.INFO)
# logging.basicConfig(stream=sys.stdout, level=logging.INFO)

if __name__ == '__main__':
    configfile = "combine.local.cfg"
    # create_example_schedule(configfile)
    # create_bearings_schedule(configfile)
    open_bearings_schedule(configfile)
    engine.start(configfile)
