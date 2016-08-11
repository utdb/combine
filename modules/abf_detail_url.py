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
        # store all previously generated url's with his avid
        for obj in self.objects_out():
            self.url_dict[obj.text()] = obj.avid()

    def handle(self, activation, obj):
        activation.input(obj)
        rfc_fields = json.loads(obj.text())
        rfc_item = rfc_fields[2]
        query_page = abf_search_page(rfc_item)
        for p in query_page['Products']:
            bag = dict()
            for t in p:
                # Assuming only a single value per key:
                bag[t['Key']] = t['Value']
            detail_url = abf_build_detail_url(bag)
            if detail_url not in self.url_dict:
                self.url_dict[obj.text()] = activation.avid()
                activation.output(engine.LwObject("abf_detail_url", [], "application/text", detail_url, None))
            else:
                # the activation shares its output with the first
                activation.share_activation(self.url_dict[obj.text()])
                print("DUPLICATE_URL: "+detail_url)
            # TODO: implement minimality testing through env
            # if True:
                # return


def get_handler(context):
    return AbfGetDetailUrl(context)
