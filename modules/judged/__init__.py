import io
import engine
import judged.context
from judged import formatting
from judged import parser
from judged import Constant, Literal, Predicate, Clause

from judged.extensions import Extension, ExtensionError
import modules.judged.extensions.extpg

class JudgeDhandler():

    def __init__(self, db=None):
        self.db = db
        self.current_context = judged.context.ExactContext()
        self.action_handlers = {}
        self.current_context.jd_handler = self

    def handle(self, s):
        """
        Processes all statements in a single reader. Errors in the handling of an
        action will be furnished with context information based on the context
        information of the parsed action.
        """
        lastres = None
        reader = io.StringIO(s)
        for action in parser.parse(reader):
            try:
                # handler = self.action_handlers.get(type(action),default_handler)
                # lastres = handler(action)
                lastres = action.perform(self.current_context)
            except judged.JudgedError as e:
                e.context = action.source
                raise e
        return lastres

    def pg_table(self, t_attr, t_val, t_name):
        cur = self.db.conn.cursor()
        a_list = ''
        c_list = ''
        for a, v in zip(t_attr,t_val):
            a_list = a if len(a_list)==0 else a_list + ', ' + a
            if isinstance(v,Constant):
                # v = str(v).translate(str.maketrans("\"", "\'"))
                cond = a + '=' + str(v)
                c_list = ' WHERE '+cond if len(c_list)==0 else c_list+' AND '+cond
        sql_stat = 'SELECT ' + a_list + ' FROM '+ t_name + c_list + ';'
        sql_stat = sql_stat.translate(str.maketrans("\"", "\'"))
        print(sql_stat)
        cur.execute(sql_stat)
        rows = cur.fetchall()
        return rows


def get_handler(context):
    return JudgeD(context)


def p(result):
    for a in result.answers:
        print(str(a.clause))
