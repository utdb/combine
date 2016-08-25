import logging
import argparse
import json
import engine


class RfcXAbfCompare(engine.Activity):

    def setup(self, args):
        self.rsrc = self.get_resource(self.activity_label()+'_x_tables', create=True)
        #
        self.rsrc.abf = {}
        self.rsrc.rfc = {}
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
        self.new_activation([rfc_in, abf_in], [out_obj, ], [self.rsrc, ], [])

    def handle_entity(self, obj, do_compare):
        oid = obj.oid
        kindtags = obj.kindtags
        kind = kindtags['kind']
        fields = obj.json_data
        # TODO: objects are not duplcate anymore
        # print("INCOMING: ", obj.kindtags())
        if kind == "rfc_entity":
            if oid not in self.rsrc.rfc:
                self.rsrc.rfc[oid] = fields
                if do_compare:
                    for abf_oid in self.rsrc.abf:
                        self.compare(oid, fields, abf_oid, self.rsrc.abf[abf_oid])
        elif kind == "abf_entity":
            if oid not in self.rsrc.abf:
                self.rsrc.abf[oid] = fields
                if do_compare:
                    for rfc_oid in self.rsrc.rfc:
                        self.compare(rfc_oid, self.rsrc.rfc[rfc_oid], oid, fields)
        else:
            raise Exception('Should not happen:-)')
        self.new_activation([obj], [], [self.rsrc, ], [self.rsrc, ])

    def handle_complex(self, obj):
        return self.handle_entity(obj, True)


def get_handler(context):
    return RfcXAbfCompare(context)
