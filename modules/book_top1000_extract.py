import sys
import logging
import argparse
import json
import engine
from pathlib import Path
from lxml import etree
import sys


class BookTop1000Extract(engine.Activity):

    def handle_simple(self, obj):
        result = []
        html_header = obj.json_data
        html_body = obj.str_data()
        tree = etree.fromstring(html_body, parser=etree.HTMLParser())
        xp = html_header['xpath']
        # xp = '//table[@class=\'worksinseries\']/descendant::tr/descendant::a[1]/@href'
        for item in tree.xpath(xp):
            work_field = str(item)
            url = "http://www.librarything.com%s"%  (work_field);
            result.append(engine.LwObject(self.kindtags_default, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, url, None, obj.json_data, obj.sentence) )
            if len(result) > 5:
                break
        return result


def get_handler(context):
    return BookTop1000Extract(context)
