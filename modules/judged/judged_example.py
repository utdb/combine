import io
import engine
import judged.context
from judged import formatting
from judged import parser
from modules.judged import *
import modules.judged.extensions.extpg


class JudgeD(engine.Activity):

    def run_jd_script(self):
        # self.jd.handle("ancestor(A, B) :- parent(A, B).")
        self.jd.handle('@use "modules.judged.extensions.extpg" with db="INCOMPLETE".')
        self.jd.handle('@from "modules.judged.extensions.extpg" use all.')
        p(self.jd.handle('ok(7,K)?'))
        p(self.jd.handle('ot(O,T)?'))
        p(self.jd.handle('log(I,E)?'))
        p(self.jd.handle('log(I,\"context.create\")?'))
        return []

    def setup(self, args):
        self.jd = JudgeDhandler(self.db)

    def handle_simple(self, obj):
        return self.run_jd_script()


def get_handler(context):
    return JudgeD(context)
