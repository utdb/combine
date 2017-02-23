import modules.judged
from judged import Constant, Literal, Predicate, Clause
from judged.extensions import Extension, ExtensionError

ext = Extension(__name__)

@ext.predicate('say', 2)
def say(pred, a, b):
    # For very simple predicates we can ignore the terms we are given, judged
    # will attempt unification after receiving the clauses anyway.
    head = Literal(pred, [Constant.string('hello'), Constant.string('db')])
    body = []
    yield Clause(head, body)


@ext.predicate('complex', 1, needs_context=True)
def complex(pred, a, *, context=None):
    # More complex or involved predicates can request that they are given the
    # query context. This makes it possible to set up a query-level cache in an
    # @ext.before_ask function, use the cache in the predicates, and tear down
    # the cache in an #ext.after_ask function.
    yield Clause(Literal(pred, [Constant.number(1337)]))


@ext.predicate('ok', 2, needs_context=True)
def ok(pred, oid, kind,  *, context=None):
    jdh = context.handler
    for row in jdh.pg_table(
            ['oid', 'kindtags->\'kind\''], [oid, kind], 'object'):
        yield Clause(Literal(pred, [Constant.number(row[0]),Constant.string(str(row[1]))]))


@ext.predicate('log', 2, needs_context=True)
def log(pred, time, event,  *, context=None):
    jdh = context.handler
    for row in jdh.pg_table(
            ['time', 'event'], [time, event], 'log'):
        yield Clause(Literal(pred, [Constant.string(row[0]),Constant.string(str(row[1]))]))


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
