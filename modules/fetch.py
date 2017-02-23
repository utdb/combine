import logging
import json
import requests
import engine
from engine import throttle


class Fetch(engine.Activity):

    def handle_simple(self, obj):
        detail_url = obj.str_data()
        detail_domain = detail_url.split("//")[-1].split("/")[0]
        # print("FETCH: "+detail_url)
        throttle.wait_for(detail_domain)
        result = requests.get(detail_url)
        result.raise_for_status()
        result.encoding = "utf-8"
        metadata = {
            "url": detail_url,
            "status": result.status_code,
            "headers": dict(result.headers)
            }
        # print(str(obj.json_data))
        for k, v in obj.json_data.items():
            metadata[k] = v # copy
        text = json.dumps(metadata, indent='   ') + '\n--\n' + result.text
        result = [engine.LwObject(self.kindtags_default, {'Content-Type': 'text/html', 'encoding': 'utf-8'}, text, None, metadata, obj.sentence), ]
        if True:
            file = open("./cache/fetch"+str(obj.oid), "w")
            file.write(text)
            file.close()
        return result


def get_handler(context):
    return Fetch(context)
