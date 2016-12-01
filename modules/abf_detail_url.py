import logging
import requests
import json
import engine

base_url = 'http://webshop.abfbearings.com'


def abf_search_page(term, take=50, skip=0):
    request_payload = {
        'SearchModel': {
            'Include': term,
            'Paging': {
                'Skip': skip,
                'Take': take,
                'Sort': 'Collection',
                'SortDirection': 'ASC'
            }
        }
    }
    r = requests.post(base_url + '/catalog/Search', json=request_payload)
    r.raise_for_status()
    return r.json()


def abf_build_detail_url(product):
    url_key = product['ProductURLKey']
    title = product['TitleFriendly']
    for c in ",./%&*$#+":
        title = title.replace(c, '-')
    identifier = product['Id']
    return base_url + "/{}/{}/{}".format(url_key, title, identifier)


class AbfGetDetailUrl(engine.Activity):

    def setup(self, args):
        # create a set of generated url's by the activity
        self.url_dict = {}
        # store all previously generated url's with his oid container
        for obj in self.objects_out():
            self.url_dict[obj.bytes_data()] = [obj.oid()]

    def handle_simple(self, obj):
        # activation.input(obj)
        result = []
        rfc_fields = obj.json_data
        rfc_item = rfc_fields[2]
        query_page = abf_search_page(rfc_item)
        for p in query_page['Products']:
            bag = dict()
            for t in p:
                # Assuming only a single value per key:
                bag[t['Key']] = t['Value']
            detail_url = abf_build_detail_url(bag)
            # print("DETAIL_URL: "+detail_url)
            if detail_url not in self.url_dict:
                newobj = engine.LwObject(self.kindtags_default, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, detail_url, None, None, obj.sentence)
                self.url_dict[detail_url] = newobj.delayed_oid_container()
                result.append(newobj)
            else:
                # the activation shares its output with the first
                print("DUPLICATE_URL: "+detail_url)
                delayed_oid = self.url_dict[detail_url][0]
                if delayed_oid > 0:
                    # otherwise the duplicate is duplicate within provenance
                    newobj = self.get_object(delayed_oid)
                    result.append(newobj)
            # TODO: implement minimality testing through env
            if False:
                break
        return result


def get_handler(context):
    return AbfGetDetailUrl(context)
