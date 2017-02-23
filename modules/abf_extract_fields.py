import logging
import json
from collections import defaultdict
from util.flowcontrol import FlowControl
import engine
from pathlib import Path
from lxml import etree


def xpath_text(node, xpath, default=None):
    candidates = node.xpath(xpath)
    if candidates:
        return candidates[0].xpath('string()').strip()
    else:
        return default


def abf_extract_body_fields(html_body):
    tree = etree.fromstring(html_body, parser=etree.HTMLParser())

    title_guess = xpath_text(tree, '//h1')
    yield ('Product', title_guess)

    # description and top properties (Brand, Item Number, Categorie)
    # //dl[contains(@class, "productInformation")
    props = tree.xpath('//dl[contains(@class, "productInformation")]')[0]
    brand_guess = xpath_text(props[1], '.')
    item_guess = xpath_text(props[3], '.')
    category_guess = xpath_text(props[5], '.')
    yield ('Brand', brand_guess)
    yield ('Item Number', item_guess)
    yield ('Category', category_guess)

    # product properties (in tab down below)
    # //li/dl
    props = tree.xpath('//li/dl')
    for prop in props:
        field_guess = xpath_text(prop, 'dt')
        value_guess = xpath_text(prop, 'dd')
        yield (field_guess, value_guess)

#
#
#


class AbfExtractFields(engine.Activity):

    def setup(self, args):
        self.flowcontrol = FlowControl('ABF', self.sentence_id)

    def handle_simple(self, obj):
        if self.flowcontrol.is_active(obj):
            html_header = obj.json_data
            html_body = obj.str_data()
            extracted = {}
            try:
                for field in abf_extract_body_fields(html_body):
                    extracted[field[0]] = field[1]
            except:
                pass
            jd = obj.json_data
            jd['url'] = html_header.get("url")
            jd['extraction_id'] = self.sentence_id
            jd['extraction_module'] = 'abf'
            jd['extraction_kindtags'] = obj.kindtags
            jd['extraction_fields'] = extracted
            # print("ABF-EXTRACTING:\n"+json.dumps(jd, indent='   '))
            return [engine.LwObject(self.kindtags_default, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, "", None, jd, obj.sentence), ]
        else:
            return []


def get_handler(context):
    return AbfExtractFields(context)
