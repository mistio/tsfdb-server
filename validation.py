import requests
import asyncio
from datetime import datetime

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


def calculate_time_intervals(datapoints):
    intervals = []
    prev_timestamp = 0
    for value, timestamp in datapoints:
        if prev_timestamp:
            intervals.append(timestamp - prev_timestamp)
        prev_timestamp = timestamp
    return intervals


def check_missing_datapoints(data):
    for resource, datapoints in data.items():
        if len(datapoints) == 0:
            print("No datapoints for resource with id: %s" % resource)
        elif len(datapoints) > 1:
            intervals = calculate_time_intervals(datapoints)
            if min(intervals) != max(intervals):
                print("Missing datapoints for resource with id: %s" % resource)


def check_inorder_datapoints(data):
    for resource, datapoints in data.items():
        if len(datapoints):
            if datapoints != sorted(datapoints, key=lambda x: x[1]):
                print("Datapoints are not in order for resource with id: %s"
                      % resource)


def check_late_datapoints(data, timestamp, max_acceptable_delay=30):
    max_delay = 0
    for resource, datapoints in data.items():
        if len(datapoints):
            max_delay = max(max_delay, timestamp -
                            datapoints[len(datapoints)-1][1])
            if (timestamp - datapoints[len(datapoints)-1][1]) \
                    > max_acceptable_delay:
                print("Over %ds delay in datapoints for resource with id: %d"
                      % max_acceptable_delay, resource)
    print("Max delay was %.3f s" % max_delay)


def main():
    mist = MistClient(mist_url, token)
    monitored_resources = list(mist.get_monitored_resources().keys())
    assert monitored_resources
    print("Number of monitored resources: %s" %
          len(monitored_resources))
    tsfdb = TsfdbClient(tsfdb_url)
    dt_before = datetime.now()
    data = tsfdb.get_datapoints_from_resources(monitored_resources)
    dt_after = datetime.now()
    print("Getting datapoints took: %d ms" %
          int((dt_after - dt_before).microseconds * 0.001))
    check_missing_datapoints(data)
    check_inorder_datapoints(data)
    check_late_datapoints(data, datetime.timestamp(dt_before))


if __name__ == "__main__":
    main()
