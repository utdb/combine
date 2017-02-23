import logging
import requests
import json
import engine


btshop_base = 'https://www.btshop.nl'

def fetch(url, parameters={}, method='get', headers=None):
        if method == 'get':
                r = requests.get(url, params=parameters, headers=headers)
        elif method == 'post':
                r = requests.post(url, data=parameters, headers=headers)

        r.raise_for_status()
        return r

def get_product_urls(query):
    r = fetch(btshop_base + '/nl/search/autocomplete/SearchBox', parameters={'term': query})
    j = json.loads(r.content.decode())

    yield from (btshop_base +  p['url'] for p in j['products'])

class BtshopGetDetailUrl(engine.Activity):

    def setup(self, args):
        # create a set of generated url's by the activity
        self.url_dict = {}
        # store all previously generated url's with his oid container
        for obj in self.objects_out():
            self.url_dict[obj.bytes_data] = [obj.oid]

    def handle_simple(self, obj):
        # activation.input(obj)
        result = []
        json_data = obj.json_data;
        json_data['detail_url_id'] = self.sentence_id
        rfc_item = json_data['seed'][2]
        # print('RFC_ITEM'+str(rfc_item))
        for detail_url in get_product_urls(rfc_item):
            # print("DETAIL_URL: "+detail_url)
            if detail_url not in self.url_dict:
                newobj = engine.LwObject(self.kindtags_default, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, detail_url, None, json_data, obj.sentence)
                self.url_dict[detail_url] = newobj.delayed_oid_container()
                result.append(newobj)
            else:
                # the activation shares its output with the first
                # print("DUPLICATE_URL: "+detail_url)
                delayed_oid = self.url_dict[detail_url][0]
                if delayed_oid > 0:
                    # otherwise the duplicate is duplicate within provenance
                    newobj = self.get_object(delayed_oid)
                    result.append(newobj)
            if False:
                break
        return result

def get_handler(context):
    return BtshopGetDetailUrl(context)
