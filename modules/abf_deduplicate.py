import logging
import json
import engine

#
#
#

class AbfDeduplicate(engine.Activity):

    def setup(self, args):
        self.all_url = {}
        self.all_entities = {}

    def compare_to_one(self, activation, new_oid, new_dict, old_oid, old_dict):
        # print(new_oid, old_oid)
        self

    def compare_to_all(self, activation, new_oid, new_dict):
        for ent_oid in self.all_entities:
            ent_dict = self.all_entities[ent_oid]
            self.compare_to_one(activation,new_oid, new_dict, ent_oid, ent_dict)

    def handle(self, activation, obj):
        activation.input(obj)
        # print("DEDUPLICATE: "+str(obj.oid()))
        oid = obj.oid()
        json_fields = json.loads(obj.text())
        url = json_fields['url']
        # url = url[:16]
        t_url = self.all_url.get(url)
        if t_url is None:
            t_url = {'cluster_id' : ('c'+str(oid)), 'oids' : [oid,] }
            self.all_url[url] = t_url
            self.compare_to_all(activation, oid, json_fields)
            self.all_entities[oid] = json_fields
        else:
            # duplicate url
            t_url['oids'].extend([oid])
        # for k in json_fields.keys():
            # print(str(oid),k,json_fields[k])
        json_out = json.dumps([t_url['cluster_id'], oid], indent='   ')
        # print(json_out)
        activation.output(engine.LwObject("abf_entity_cluster", [], "application/json", json_out, None))


def get_handler(context):
    return AbfDeduplicate(context)
