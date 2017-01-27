import logging
import json
import re
import engine
from pathlib import Path
from lxml import etree


def xpath_text(node, xpath, default=None):
    candidates = node.xpath(xpath)
    if candidates:
        return candidates[0].xpath('string()').strip()
    else:
        return default


def btshop_extract_body_fields(html_body):
    tree = etree.fromstring(html_body, parser=etree.HTMLParser())
    for item in tree.xpath('//ul[@class="references"]/li'):
            field = item.text.strip()
            value = item.xpath('strong')[0].text.strip()

            if field.startswith('Artikelnummer'):
                    yield 'Product', value

                    brand = re.search('Artikelnummer(.*):', field)
                    if(brand):
                            yield 'Brand', brand.group(1).strip()

            if field.startswith('EAN'):
                    yield 'EAN', value

    descr_guess = xpath_text(tree, '//div[@class="part-left"]/div/p')
    if descr_guess:
            yield 'Description', descr_guess

    for feature in tree.xpath('//ul[@class="features"]/li/strong/..'):
            field = feature.xpath('strong')[0].text.strip()
            text = feature.xpath('string()')

            if field:
                    pos = text.find(field)
                    if pos != -1:
                            value = text[pos+len(field):].strip()
                            if value:
                                    yield field, value


class BtshopExtractFields(engine.Activity):

    def handle_simple(self, obj):
        html_header = obj.json_data
        html_body = obj.str_data()
        fields = {}
        for field in btshop_extract_body_fields(html_body):
            fields[field[0]] = field[1]
        fields['url'] = html_header.get("url")
        print("EXTRACTING:\n"+json.dumps(fields, indent='   '))
        return [engine.LwObject(self.kindtags_default, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, "", None, fields, obj.sentence), ]


def get_handler(context):
    return BtshopExtractFields(context)
