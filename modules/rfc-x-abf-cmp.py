import logging
import argparse
import json
import engine


class RfcXAbfCompare(engine.Activity):

    def setup(self, args):
        self.abf = {}
        self.rfc = {}
        # now read the objects which were already read in prev session
        for obj in self.objects_in():
            self.handle_entity(obj, False)

    def compare(self, rfc_oid, e_rfc, abf_oid, e_abf):
        rfc_in = self.get_object(rfc_oid)
        abf_in = self.get_object(abf_oid)
        if True:
            print("COMPARE TODO:")
            print(str(e_rfc))
            print("<<<>>>")
            print(str(e_abf))
        out_obj = engine.LwObject({'kind': "rfc_x_abf", 'tags': []}, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, "INCOMPLETE", None, {})
        self.new_activation([rfc_in, abf_in], [out_obj, ])

    def handle_entity(self, obj, do_compare):
        oid = obj.oid()
        kindtags = obj.kindtags()
        kind = kindtags['kind']
        fields = obj.json_data()
        # TODO: objects are not duplcate anymore
        # print("INCOMING: ", obj.kindtags())
        if kind == "rfc_entity":
            if oid not in self.rfc:
                self.rfc[oid] = fields
                if do_compare:
                    for abf_oid in self.abf:
                        self.compare(oid, fields, abf_oid, self.abf[abf_oid])
        elif kind == "abf_entity":
            if oid not in self.abf:
                self.abf[oid] = fields
                if do_compare:
                    for rfc_oid in self.rfc:
                        self.compare(rfc_oid, self.rfc[rfc_oid], oid, fields)
        else:
            raise Exception('Should not happen:-)')

    def handle_complex(self, obj):
        return self.handle_entity(obj, True)


def get_handler(context):
    return RfcXAbfCompare(context)
