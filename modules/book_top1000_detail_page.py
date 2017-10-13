import sys
import logging
import argparse
import json
import engine
from pathlib import Path
from lxml import etree
import sys


class BookTop1000DetailPage(engine.Activity):

    def handle_simple(self, obj):
        result = []
        html_header = obj.json_data['crawl_descr']
        html_body = obj.str_data()
        print("/---------"+obj.json_data['url'])
        tree = etree.fromstring(html_body, parser=etree.HTMLParser())
        for tag in html_header['tags']:
            xp_list = html_header['xpath'][tag]
            for xp in xp_list: 
                for item in tree.xpath(xp):
                    print("Found: "+tag+":"+str(item.text))
        return []


def get_handler(context):
    return BookTop1000DetailPage(context)
