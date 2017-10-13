import logging
import argparse
import json
import engine
import sys


class BookTop1000BookGenerator(engine.Activity):

    def setup(self, args):
        self.rsrc = self.get_resource('BookTop1000Generator', create=True)
        #
        self.rsrc.xpath = None
        self.rsrc.seed = None
        # now read the objects which were already read in prev session
        for obj in self.objects_in():
            self.handle_entity(obj, False)

    def handle_entity(self, obj, do_compare):
        oid = obj.oid
        kindtags = obj.kindtags
        kind = kindtags['kind']
        if kind == "book_top1000_seed":
            fields = obj.json_data
            self.rsrc.seed = fields
        elif kind == "book_top1000_xpaths":
            fields = obj.json_data
            self.rsrc.xpath = fields
        else:
            raise Exception('Should not happen:-)')
        if (self.rsrc.seed is not None) and (self.rsrc.xpath is not None):
            json_data = {}
            json_data['url'] = self.rsrc.seed['url']
            json_data['xpath'] = self.rsrc.seed['xpath']
            json_data['crawl_descr'] = self.rsrc.xpath
            result = [ engine.LwObject(self.kindtags_default, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, self.rsrc.seed['url'], None, json_data, obj.sentence) ]
        else:
            result = []
        self.new_activation([obj], result, [self.rsrc, ], [self.rsrc, ])

    def handle_complex(self, obj):
        return self.handle_entity(obj, True)


def get_handler(context):
    return BookTop1000BookGenerator(context)
