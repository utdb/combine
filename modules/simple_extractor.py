import logging
import argparse
import json
from collections import defaultdict
from util.flowcontrol import FlowControl
import engine
from lxml import etree


def attrify(item, fields):
    try:
        s = ''.join([(x.strip()+'|') for x in item.itertext() if len(x.strip())>0])
        # print("S="+s)
        l = [x for x in s.split('|') if len(x) > 0]
        fields[l[0]] = l[1]
    except:
        pass

def findSimple(tree, words, fields):
    # incomplete, should do lower case
    for keyword in words:
        for item in tree.xpath('//*[contains(text(), \''+keyword+'\')]'):
            attrify(item, fields)

def findSiblings(tree, sibling_words, fields):
    for kw in sibling_words:
        for item in tree.xpath('//*[contains(text(), \''+kw+'\')]'):
            ch = item.getparent().getparent().getchildren();
            if len(ch) > 1:
                newfields = {}
                for sib in ch:
                    for xx in sib.getchildren():
                        if len(sib.getchildren()) < 2:
                            attrify(sib, newfields)
                # all keywords must be in siblings tags
                if all(any(kw in tag for tag in newfields.keys()) for kw in sibling_words):
                    fields.update(newfields)

class SimpleExtractor(engine.Activity):

    def setup(self, args):
        parser = argparse.ArgumentParser()
        parser.add_argument('-1', '--simple', required=True)
        parser.add_argument('-n', '--sibling', required=True)
        args = vars(parser.parse_args(args))
        self.single_words = args['simple'].split('|')
        self.sibling_words = args['sibling'].split('|')
        self.flowcontrol = FlowControl('SIMPLE', self.sentence_id)

    def handle_simple(self, obj):
        if self.flowcontrol.is_active(obj):
            html_header = obj.json_data
            html_body = obj.str_data()
            extracted = {}
            tree = etree.fromstring(html_body, parser=etree.HTMLParser())
            findSimple(tree, self.single_words, extracted)
            findSiblings(tree, self.sibling_words, extracted)
            jd = obj.json_data
            jd['url'] = html_header.get("url")
            jd['extraction_id'] = self.sentence_id
            jd['extraction_module'] = 'simple'
            jd['extraction_kindtags'] = obj.kindtags
            jd['extraction_fields'] = extracted
            # print("BTSHOP_JSON_DATA:\n"+json.dumps(jd, indent='   '))
            return [engine.LwObject(self.kindtags_default, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, "", None, jd, obj.sentence), ]
        else:
            return []


def get_handler(context):
    return SimpleExtractor(context)
