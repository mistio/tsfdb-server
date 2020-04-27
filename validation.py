import requests
import asyncio

mist_url = "http://192.168.1.8"
tsfdb_url = "http://tsfdb:8080"
token = "c6f02d5181a24c6384192e9edbcc8b7fd072f5215ebbf2ddeaafeb14d78c90e7"


class MistClient(object):
    def __init__(self, url, token):
        self.url = url
        self.token = token
        assert self.check_token()

    def _get_headers(self):
        return {'Authorization': self.token}

    def check_token(self):
        return requests.get(self.url + '/api/v1/clouds',
                            headers=self._get_headers()).ok

    def get_monitored_resources(self):
        monitoring = requests.get(self.url + '/api/v1/monitoring',
                                  headers=self._get_headers())
        if monitoring.ok:
            monitoring = monitoring.json()
            return monitoring.get("monitored_machines")
        return None


class TsfdbClient(object):
    def __init__(self, url):
        self.url = url

    def get_datapoints_from_resources(self, resources):
        print("Getting datapoints from %d resources" % len(resources))
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(self._get_datapoints(resources))
        if None in data:
            print("Failed to get datapoints from %d resources" %
                  len([True for d in data if not d]))
        data_dict = {}
        for d in data:
            data_dict.update(d)
        return data_dict

    async def _get_datapoints(self, resources):
        loop = asyncio.get_event_loop()
        data = [
            loop.run_in_executor(None, self.get_datapoints_from_resource, *
                                 (resource,))
            for resource in resources
        ]

        return await asyncio.gather(*data)

    def get_datapoints_from_resource(self, resource):
        metric = "system.load1"
        query = 'fetch("%s.%s", start="", stop="", step="")' % (
            resource, metric)
        data = requests.get(
            "%s/v1/datapoints?query=%s" % (self.url, query))
        if data.ok:
            data = data.json()
            return data["series"]
        return None


def main():
    mist = MistClient(mist_url, token)
    monitored_resources = list(mist.get_monitored_resources().keys())
    assert monitored_resources
    print("Number of monitored resources: %s" %
          len(monitored_resources))
    tsfdb = TsfdbClient(tsfdb_url)
    print(tsfdb.get_datapoints_from_resources(monitored_resources))


if __name__ == "__main__":
    main()
