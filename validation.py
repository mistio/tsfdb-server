import requests
import asyncio
import sys
import getopt
from datetime import datetime

mist_url = "http://dogfood2-mist-api"
tsfdb_url = "http://localhost:8080"
token = "token"
default_resource_name = "tsfdb-stress-test"
org = ""

machine_data = {
    "cloud": "359f5325870f4fa4bee7e8de4ef3e4da",
    "name": "tsfdb-stress-test0",
    "image": "a9790c9a4d3f4e5ea17fa6415f394c87",
    "size": "82608761251b4164ac0dc985d45634df",
    "location": "8a993813f61b40809c0de304beddb55b",
    "key": "50bc1eefc74c4a929476b6d3225725bb",
    "monitoring": True
}


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

    def get_all_resources(self):
        resources = requests.get(self.url + '/api/v1/machines',
                                 headers=self._get_headers())
        if resources.ok:
            resources = resources.json()
            return resources
        return None

    def create_machine(self, data):
        print(requests.post(self.url + '/api/v1/clouds/' +
                            data["cloud"] + '/machines', json=data,
                            headers=self._get_headers()).text)

    def delete_machine(self, machine_id):
        print(requests.post(self.url + '/api/v1/machines/' +
                            machine_id, json={'action': 'destroy'},
                            headers=self._get_headers()).text)


class TsfdbClient(object):
    def __init__(self, url):
        self.url = url

    def get_datapoints_from_resources(self, resources, minutes=None):
        time_per_request = {}
        print("Getting datapoints from %d resources" % len(resources))
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(
            self._get_datapoints(resources, time_per_request, minutes))
        if None in data:
            print("Failed to get datapoints from %d resources" %
                  len([True for d in data if not d]))
        print("Slowest response took: %d ms" %
              int(min(time_per_request.values()).microseconds * 0.001))
        data_dict = {}
        for d in data:
            if d:
                data_dict.update(d)
        return data_dict

    async def _get_datapoints(self, resources, time_per_request, minutes=None):
        loop = asyncio.get_event_loop()
        data = [
            loop.run_in_executor(None, self.get_datapoints_from_resource, *
                                 (resource, time_per_request, minutes))
            for resource in resources
        ]

        return await asyncio.gather(*data)

    def get_datapoints_from_resource(self, resource, time_per_request,
                                     minutes=None):
        metric = "system.load1"
        if minutes:
            query = 'fetch("%s.%s", start="-%dm", stop="", step="")' % (
                resource, metric, minutes)
        else:
            query = 'fetch("%s.%s", start="", stop="", step="")' % (
                resource, metric)
        dt_before = datetime.now()
        data = None
        try:
            data = requests.get(
                "%s/v1/datapoints?query=%s" % (self.url, query),
                headers={'x-org-id': org})
        except requests.exceptions.ConnectionError:
            print(f"Could not fetch datapoints for resource: {resource}")
            return None
        dt_after = datetime.now()
        time_per_request[resource] = dt_after - dt_before
        if data.ok:
            data = data.json()
            return data.get("series")
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
    count = 0
    for resource, datapoints in data.items():
        if len(datapoints) == 0:
            print("No datapoints for resource with id: %s" % resource)
        elif len(datapoints) > 1:
            intervals = calculate_time_intervals(datapoints)
            if min(intervals) != max(intervals):
                print("Missing datapoints for resource with id: %s" % resource)
                count += 1
    print("Missing datapoints for %d resources" % count)


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
                print("Over %ds delay in datapoints for resource with id: %s"
                      % (max_acceptable_delay, resource))
    print("Max datapoints delay was %.3f s" % max_delay)


def get_resources_mist_ids(id_of_first_machine, num_of_machines, resources):
    resources_names = set(("%s%i" % (default_resource_name, i)) for i in range(
        id_of_first_machine, id_of_first_machine + num_of_machines))
    resources_mist_ids = []
    for resource in resources:
        if resource.get("name") in resources_names:
            resources_mist_ids.append(resource.get("id"))

    return resources_mist_ids


def delete_resources(id_of_first_machine, num_of_machines, mist):
    print("Deleting %d machines" % num_of_machines)
    resources = mist.get_all_resources()
    for resource_id in get_resources_mist_ids(id_of_first_machine,
                                              num_of_machines, resources):
        mist.delete_machine(resource_id)


def create_resources(id_of_first_machine, num_of_machines, mist):
    print("Creating %d machines" % num_of_machines)
    machine_data["name"] = "%s%d" % (
        default_resource_name, id_of_first_machine)
    machine_data["async"] = True
    machine_data["quantity"] = num_of_machines
    mist.create_machine(machine_data)


def check_resources(mist, tsfdb, minutes=None):
    print("Checking monitored machines")
    monitored_resources = list(mist.get_monitored_resources().keys())
    assert monitored_resources
    print("Number of monitored resources: %s" %
          len(monitored_resources))
    dt = datetime.now()
    data = tsfdb.get_datapoints_from_resources(monitored_resources, minutes)
    check_missing_datapoints(data)
    check_inorder_datapoints(data)
    if not minutes:
        check_late_datapoints(data, datetime.timestamp(dt))


def options():
    print('For monitoring stats:')
    print('\tvalidation.py -s [-r <range_in_minutes>]')
    print('For creating machines:')
    print('\tvalidation.py -m <num_of_machines> [-i <id_of_first_machine>]')
    print('For deleting machines:')
    print('\tvalidation.py -m <num_of_machines> [-i <id_of_first_machine>] -d')


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "m:i:dhsr:")
    except getopt.GetoptError:
        options()
        sys.exit(2)

    num_of_machines = 0
    id_of_first_machine = 0
    create = False
    delete = False
    stats = False
    minutes = None

    for opt, arg in opts:
        if opt == '-m':
            num_of_machines = int(arg)
            if not delete:
                create = True
        elif opt == "-i":
            id_of_first_machine = int(arg)
        elif opt == "-d":
            delete = True
            create = False
        elif opt == "-h":
            options()
            return
        elif opt == "-s":
            stats = True
        elif opt == "-r":
            minutes = int(arg)

    mist = MistClient(mist_url, token)
    tsfdb = TsfdbClient(tsfdb_url)

    if stats:
        check_resources(mist, tsfdb, minutes)
    elif delete:
        delete_resources(id_of_first_machine, num_of_machines, mist)
    elif create:
        create_resources(id_of_first_machine, num_of_machines, mist)
    else:
        options()


if __name__ == "__main__":
    main(sys.argv[1:])
