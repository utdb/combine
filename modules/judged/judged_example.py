import io
import engine
import judged.context
from judged import formatting
from judged import parser
from modules.judged import *
import modules.judged.extensions.extpg


class JudgeD(engine.Activity):

    def run_jd_script(self):
        self.jd.handle("ancestor(A, B) :- parent(A, B).")
        self.jd.handle("ancestor(A, B) :- parent(A, C), D = C,  ancestor(D, B).")
        self.jd.handle("parent(john, douglas).")
        self.jd.handle("parent(bob, john).")
        self.jd.handle("parent(ebbon, bob).")
        p(self.jd.handle("ancestor(A, B)?"))
        #
        self.jd.handle('@use "modules.judged.extensions.extpg" with db="INCOMPLETE".')
        self.jd.handle('@from "modules.judged.extensions.extpg" use all.')
        self.jd.handle('@from "modules.judged.extensions.extpg" use complex as example_predicate.')
        modules.judged.extensions.extpg.global_db = self.db
        p(self.jd.handle('say(A, B)?'))
        p(self.jd.handle('ok(7,K)?'))
        # p(self.jd.handle('log(I,\"context.create\")?'))

    def setup(self, args):
        print("JUDGED MODULE STARTED")
        self.jd = JudgeDhandler(self.db)

    def handle_simple(self, obj):
        print("JUDGED MODULE HANDL SIMPLE")
        self.run_jd_script()
        return []


def get_handler(context):
    return JudgeD(context)
