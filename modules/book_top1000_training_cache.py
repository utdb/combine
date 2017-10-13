import logging
import argparse
import json
import engine
import modules.fetch


class BookTop1000TrainingCache(engine.Activity):

    def handle_simple(self, obj):
        print("BookTop1000TrainingCache")
        for book in obj.json_data:
            url = book.get('url')
            page = modules.fetch.get_webpage(self.scheduler.db, url)
        return []


def get_handler(context):
    return BookTop1000TrainingCache(context)
