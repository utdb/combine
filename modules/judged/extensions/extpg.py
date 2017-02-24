import modules.judged
from judged import Constant, Literal, Predicate, Clause
from judged.extensions import Extension, ExtensionError

ext = Extension(__name__)

@ext.predicate('ok', 2, needs_context=True)
def ok(pred, oid, kind,  *, context=None):
    jdh = context.jd_handler
    for row in jdh.pg_table(
            ['oid', 'kindtags->\'kind\''], [oid, kind], 'object'):
        yield Clause(Literal(pred, [Constant.number(row[0]),Constant.string(str(row[1]))]))

@ext.predicate('ot', 2, needs_context=True)
def ok(pred, oid, tags,  *, context=None):
    jdh = context.jd_handler
    for row in jdh.pg_table(
            ['oid', 'kindtags->\'tags\''], [oid, tags], 'object'):
        yield Clause(Literal(pred, [Constant.number(row[0]),Constant.string(str(row[1]))]))


@ext.predicate('log', 2, needs_context=True)
def log(pred, time, event,  *, context=None):
    jdh = context.jd_handler
    for row in jdh.pg_table(
            ['time', 'event'], [time, event], 'log'):
        yield Clause(Literal(pred, [Constant.string(str(row[0])),Constant.string(str(row[1]))]))


@ext.setup
def init(context, db=None, **rest):
    """Initialise the extpg extension, which takes one parameter 'db'.

    The other parameters are ignored, and an error is thrown if we get them.
    """
    if rest:
        raise ExtensionError("extpg only supports the 'db' parameter")

    # We store the configuration globally in this module, which works well
    # enough for most use-cases.
    context.db = db


@ext.before_ask
def prepare_something(context):
    # print("extpg sees the start of a query!")
    pass


@ext.after_ask
def clear_out_mess(context):
    # print("extpg sees the end of a query!")
    pass
