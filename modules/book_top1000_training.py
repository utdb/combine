import logging
import argparse
import json
import engine
import modules.fetch
import sys
from pathlib import Path
from lxml import etree


class BookTop1000Training(engine.Activity):

    def handle_simple(self, obj):
        # activation.input(obj)
        # read the seeds from the file in the start seed file
        result = []
        pdb = []
        crawl_json = obj.json_data
        for book in crawl_json['examples']:
            print("Training on book: "+json.dumps(book, indent='   '))
            url = book.get('url')
            cur_pdb = {}
            for ex in crawl_json['tags']:
                cur_pdb[ex] = book.get(ex)
                cur_pdb[ex+"_xp"] = set()
            pdb.append(cur_pdb)
            page = modules.fetch.get_webpage(self.scheduler.db, url)
            tree = etree.fromstring(page, parser=etree.HTMLParser())
            #
            for el in tree.iter():
                if el.text is not None:
                    base = str(el.text)
                    for ex in crawl_json['tags']:
                        if base == book[ex]:
                            xp = el.getroottree().getpath(el)
                            cur_pdb[ex+'_xp'].add(xp)
                            print("xpath[\'"+ex+"\']#"+ str(el.text)+"(100%)#"+xp)
        crawl_json['xpath'] = { }
        print("\n")
        for ex in crawl_json['tags']:
            crawl_json['xpath'][ex] = sub_intersect(pdb, ex+'_xp')
            print("Traing xpath for \'"+ex+"\': "+str(crawl_json['xpath'][ex]))
        result = [ engine.LwObject(self.kindtags_default, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, url, None, crawl_json, obj.sentence) ]
        return result

def sub_intersect(l, subname):
    res = None
    for le in l:
        if res is None:
            res = le[subname]
        else:
            res = res.intersection(le[subname])
    return list(res)       

def get_handler(context):
    return BookTop1000Training(context)
