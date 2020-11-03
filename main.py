import argparse
import logging
import urllib

import requests

log = logging.getLogger(__name__)


class NYTimesSource(object):
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(self):
        pass

    def connect(self, inc_column=None, max_inc_value=None):
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass

    def read_ny_data(self, page_num):
        url_base = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
        params = {
            'q': self.args.query,
            'api-key': self.args.api_key,
            'page': page_num
        }
        url = '%s?%s' % (url_base, urllib.parse.urlencode(params))

        response = requests.get(url)
        return response.json()

    def _create_dotted_path(self, rootkey, data):
        def create_dotted_path_recur(rootkey, data):
            if type(data) is dict:
                for k, v in data.items():
                    create_dotted_path_recur('%s.%s' % (rootkey, k) if rootkey else k, data=v)
            else:
                result[rootkey] = data

        result = {}
        create_dotted_path_recur(rootkey, data)

        return result

    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        """
        for page_num in range(0, batch_size):
            json_data = self.read_ny_data(page_num)

            result = []
            for item in json_data.get('response', {}).get('docs', []):
                new_item = {}
                for k, v in item.items():
                    if k in self.getSchema():
                        new_item.update(self._create_dotted_path(k, v))

                result.append(new_item)
            yield result

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """

        schema = [
            'abstract',
            'web_url',
            'snippet',
            'headline',
            'pub_date',
            'document_type',
            'news_desk',
            'section_name',
            'subsection_name',
            'type_of_material',
            'word_count',
            'uri',
            '_id'
            # TODO: please, add other keys from NY api if you need https://developer.nytimes.com/docs/articlesearch-product/1/routes/articlesearch.json/get
        ]

        return schema


if __name__ == "__main__":
    config = {
        "api_key": "eQOmAZdwVqOhbeNI86oSRFsnWXcV5GNq",
        "query": "Silicon Valley",
    }
    source = NYTimesSource()

    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    source.args = argparse.Namespace(**config)

    for idx, batch in enumerate(source.getDataBatch(10)):
        print(f"{idx} Batch of {len(batch)} items")
        for item in batch:
            print(f"  - {item['_id']} - {item['headline.main']}")
