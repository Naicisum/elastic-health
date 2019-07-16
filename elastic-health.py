#!venv/bin/python
from collections import OrderedDict
import getopt
import json
import pandas as pd
import re
import ssl
import sys
import urllib
import urllib.request
import urllib.error


# Set Global Variables
debug = False
url_prefix = None
verbose = False


# Define Common Functions
def run_query(url):
    if debug:
        # Fmt: 1234567890
        print("Url      : " + url)
    try:
        result = json.load(urllib.request.urlopen(url))
    except urllib.error.URLError as err:
        if debug:
            debug_err(err)
        if isinstance(err.args[0], ssl.SSLError):
            print("SSL Error: {0}".format(err.args[0].args[1]))
        result = None
    return result


def fetch_nodes_list(server, port):
    nodes = list()
    url = url_prefix + server + ":" + port + '/_nodes?filter_path=**.node.name'
    if debug:
        # Fmt: 1234567890
        print("Server   : " + server)
        print("Port     : " + port)
    results = run_query(url)
    if debug:
        # Fmt: 1234567890
        print("Results  : {0}".format(type(results)))
    results = results['nodes']  # Drop down a level in the tree
    for key, value in results.items():
        single_list = (value['settings']['node']['name'], key)  # Create a list based on elastic node name then id
        nodes.append(single_list)
    return nodes


def fetch_nodes(node, server, port):
    data_set = OrderedDict()
    iterator = int(0)
    url = url_prefix + server + ":" + port + '/_nodes?human'
    domain_name = "." + server.split('.', 1)[1]
    results = run_query(url)
    results = results['nodes']
    if node is None:
        for server in results.items():
            data_set[iterator] = {'Cluster': server[1]['settings']['cluster']['name'],
                                  'Name': server[1]['name'].replace(domain_name, ''),
                                  'Address': server[1]['transport_address'],
                                  'Version': server[1]['version'],
                                  'Roles': server[1]['roles'],
                                  'Site': server[1]['attributes']['site'],
                                  'Host': server[1]['attributes']['host'].replace(domain_name, ''),
                                  'CPUs': server[1]['os']['allocated_processors'],
                                  'MemLocked': server[1]['process']['mlockall'],
                                  'Comp. OOPs': server[1]['jvm']['using_compressed_ordinary_object_pointers'],
                                  'JVM Name': server[1]['jvm']['vm_name'],
                                  'JVM Version': server[1]['jvm']['vm_version'],
                                  'Heap Min': server[1]['jvm']['mem']['heap_init'],
                                  'Heap Max': server[1]['jvm']['mem']['heap_max']}
            iterator += 1
    else:
        pattern = re.compile(node)
        for server in results.items():
            if pattern.search(server[1]['name']):
                data_set[iterator] = {'Cluster': server[1]['settings']['cluster']['name'],
                                      'Name': server[1]['name'].replace(domain_name, ''),
                                      'Address': server[1]['transport_address'],
                                      'Version': server[1]['version'],
                                      'Roles': server[1]['roles'],
                                      'Site': server[1]['attributes']['site'],
                                      'Host': server[1]['attributes']['host'].replace(domain_name, ''),
                                      'CPUs': server[1]['os']['allocated_processors'],
                                      'MemLocked': server[1]['process']['mlockall'],
                                      'Comp. OOPs': server[1]['jvm']['using_compressed_ordinary_object_pointers'],
                                      'JVM Name': server[1]['jvm']['vm_name'],
                                      'JVM Version': server[1]['jvm']['vm_version'],
                                      'Heap Min': server[1]['jvm']['mem']['heap_init'],
                                      'Heap Max': server[1]['jvm']['mem']['heap_max']}
                iterator += 1
    return data_set


def unique_dict_keys(data):
    data_set = list()
    for k, v in data.items():
        for sk, sv in v.items():
            if sk not in data_set:
                data_set.append(sk)
        break
    return data_set


def print_dict(data):
    if data.__len__() != 0:
        sort = unique_dict_keys(data)[1]
        data_frame = pd.DataFrame.from_dict(data, orient='index', columns=unique_dict_keys(data))
        if verbose:
            print(data_frame.sort_values(sort).to_string(index=False))
        else:
            print(data_frame[['Name', 'Address', 'Version', 'Roles', 'CPUs', 'MemLocked', 'Comp. OOPs', 'JVM Version',
                              'Heap Min', 'Heap Max']].sort_values(sort).to_string(index=False))
    else:
        print("No Data")


def debug_err(err):
    # Fmt: 1234567890
    print("Err      : {0}".format(err))
    print("Err Type : {0}".format(type(err)))
    print("Err Args : {0}".format(err.args))
    x = err.args
    print("X        : {0}".format(x))
    print("X Type   : {0}".format(type(x)))
    for y in x:
        # Fmt: 1234567890
        print("Y        : {0}".format(y))
        print("Y Type   : {0}".format(type(y)))
        print("Y Args   : {0}".format(y.args))
        z, zz = y.args
        print("Z        : {0}".format(z))
        print("Z Type   : {0}".format(type(z)))
        print("ZZ       : {0}".format(zz))
        print("ZZ Type  : {0}".format(type(zz)))


def usage(args, exit_code):
    print("Usage: " + args + " [options...] <server>")
    print("Options:")
    # Fmt:          1         2         3         4         5         6         7         8
    # Fmt: 12345678901234567890123456789012345678901234567890123456789012345678901234567890
    print(" -d, --debug         Enable debugging information")
    print(" -h, --help          This help text")
    print(" -k, --insecure      Allow insecure connections")
    print(" -n, --node          Specify node search pattern")
    print(" -p, --port          Set port for server")
    print(" -s, --server        Specify the server name or ip address")
    print(" -v, --verbose       Show additional columns")
    sys.exit(exit_code)


# Main Entry
def main(argv):
    global debug
    global url_prefix
    global verbose
    elastic_port = None
    elastic_server = None
    node = None
    opts = None
    args = None

    try:
        opts, args = getopt.getopt(argv, "dhkn:p:s:v", ["debug", "help", "insecure", "node=", "port=", "server=",
                                                        "verbose"])
    except getopt.GetoptError:
        usage(sys.argv[0], 2)

    for opt, arg in opts:
        if opt in ("-d", "--debug"):
            debug = True
        elif opt in ("-h", "--help"):
            usage(sys.argv[0], 0)
        elif opt in ("-k", "--insecure"):
            url_prefix = "http://"
        elif opt in ("-n", "--node"):
            node = arg
        elif opt in ("-p", "--port"):
            elastic_port = arg
        elif opt in ("-s", "--server"):
            elastic_server = arg
        elif opt in ("-v", "--verbose"):
            verbose = True

    if len(args) > 0:
        if elastic_server is None:
            elastic_server = args[0]

    # Set Defaults
    if elastic_server is None:
        usage(sys.argv[0], 2)
    if elastic_port is None:
        elastic_port = "9200"
    if url_prefix is None:
        url_prefix = "https://"

    # results = fetch_nodes_list(elastic_server, elastic_port)
    if node is None:
        results = fetch_nodes(None, elastic_server, elastic_port)
    else:
        results = fetch_nodes(node, elastic_server, elastic_port)

    print_dict(results)


if __name__ == '__main__':
    main(sys.argv[1:])
