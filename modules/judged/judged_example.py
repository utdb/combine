import io
import engine
import judged.context
from judged import formatting
from judged import parser
import modules.judged.extensions.extpg

current_context = judged.context.ExactContext()

action_handlers = {}

def default_handler(action):
    if False:
        print(formatting.comment("% ") + "{}".format(action))
    return action.perform(current_context)

def jd(s):
    """
    Processes all statements in a single reader. Errors in the handling of an
    action will be furnished with context information based on the context
    information of the parsed action.
    """
    lastres = None
    reader = io.StringIO(s)
    for action in parser.parse(reader):
        try:
            handler = action_handlers.get(type(action), default_handler)
            lastres = handler(action)
        except judged.JudgedError as e:
            e.context = action.source
            raise e
    return lastres

def p(result):
    for a in result.answers:
        print(str(a.clause))


class JudgeD(engine.Activity):

    def run_jd(self):
        jd("ancestor(A, B) :- parent(A, B).")
        jd("ancestor(A, B) :- parent(A, C), D = C,  ancestor(D, B).")
        jd("parent(john, douglas).")
        jd("parent(bob, john).")
        jd("parent(ebbon, bob).")
        p(jd("ancestor(A, B)?"))
        #
        jd('@use "modules.judged.extensions.extpg" with db="INCOMPLETE".')
        jd('@from "modules.judged.extensions.extpg" use all.')
        jd('@from "modules.judged.extensions.extpg" use complex as example_predicate.')
        modules.judged.extensions.extpg.global_db = self.db
        p(jd('say(A, B)?'))
        p(jd('log(T,E)?'))
        # p(jd('log(I,\"context.create\")?'))

    def setup(self, args):
        print("JUDGED MODULE STARTED")

    def handle_simple(self, obj):
        print("JUDGED MODULE HANDL SIMPLE")
        self.run_jd()
        return []


def get_handler(context):
    return JudgeD(context)

if __name__ == '__main__':
    run_jd()
