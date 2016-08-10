import logging
import json
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

    def handle(self, activation, obj):
        activation.input(obj)
        abf_storage = obj.text()
        html_header, html_body = abf_storage.split('\n--\n', 1)
        fields = []
        fields.extend(abf_extract_body_fields(html_body))
        header = json.loads(html_header)
        fields.extend([['url', header.get("url")]])
        json_entity_fields = json.dumps(fields, indent='   ')
        # print(json_entity_fields)
        activation.output(engine.LwObject("abf_entity_fields", [], "application/json", json_entity_fields, None))


def get_handler(context):
    return AbfExtractFields(context)
