import logging
import argparse
import json
import engine


class RfcXAbfCompare(engine.Activity):

    def setup(self, args):
        self.abf = []
        self.rfc = []
        # now read the objects which were already read in prev session
        for obj in self.objects_in():
            self.handle_entity(None, obj, False)

    def compare(self, activation, e_rfc, e_abf):
        #print(e_rfc[2], e_abf['oid'])
        self
 
    def handle_entity(self, activation, obj, do_compare):
        # print(obj.kind(),str(self.rfc))
        fields = json.loads(obj.text())
        # print(fields)
        kind = obj.kind()
        if kind == "rfc_entity":
            fields.append(obj.oid())
            self.rfc.append(fields)
            if do_compare:
                for e_abf in self.abf:
                    compare(activation, fields, e_abf)
        elif kind == "abf_entity":
            fields['oid'] = obj.oid()
            self.abf.append(fields)
            if do_compare:
                for e_rfc in self.rfc:
                    self.compare(activation, e_rfc, fields)
        else:
            raise Exception('Should not happen:-)')

    def handle(self, activation, obj):
        activation.input(obj)
        return self.handle_entity(activation, obj, True)

def get_handler(context):
    return RfcXAbfCompare(context)
