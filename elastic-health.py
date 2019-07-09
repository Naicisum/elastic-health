#!venv/bin/python
import urllib
import urllib.request
import urllib.error
import json
import sys
import ssl
import getopt


# Set Global Variables
debug = False
url_prefix = None


# Define Common Functions
def run_query(url):
    if debug:
        # Fmt: 1234567890
        print("Url   : " + url)
    try:
        result = json.load(urllib.request.urlopen(url))
    except urllib.error.URLError as err:
        if debug:
            debug_err(err)
        if isinstance(err.args[0], ssl.SSLError):
            print("SSL Error: {0}".format(err.args[0].args[1]))
        result = None
    return result


def fetch_nodes(server, port):
    url = url_prefix + server + ":" + port + '/_nodes?filter_path=**.node.name'
    if debug:
        # Fmt: 1234567890
        print("Server: " + server)
        print("Port  : " + port)
    results = run_query(url)
    return results


def debug_err(err):
    print("Err     : {0}".format(err))
    print("Err Type: {0}".format(type(err)))
    print("Err Args: {0}".format(err.args))
    x = err.args
    print("X       : {0}".format(x))
    print("X Type  : {0}".format(type(x)))
    for y in x:
        print("Y       : {0}".format(y))
        print("Y Type  : {0}".format(type(y)))
        print("Y Args  : {0}".format(y.args))
        z, zz = y.args
        print("Z       : {0}".format(z))
        print("Z Type  : {0}".format(type(z)))
        print("ZZ      : {0}".format(zz))
        print("ZZ Type : {0}".format(type(zz)))


def usage(args, exit_code):
    print("Usage: " + args + "[options...] <server>")
    print("Options:")
    # Fmt:          1         2         3         4         5         6         7         8
    # Fmt: 12345678901234567890123456789012345678901234567890123456789012345678901234567890
    print(" -d, --debug         Enable debugging information")
    print(" -h, --help          This help text")
    print(" -k, --insecure      Allow insecure connections")
    print(" -p, --port          Set port for server")
    print(" -s, --server        Specify the server name or ip address")
    sys.exit(exit_code)


# Main Entry
def main(argv):
    global debug
    global url_prefix
    elastic_port = None
    elastic_server = None
    opts = None
    args = None

    try:
        opts, args = getopt.getopt(argv, "dhkp:s:", ["debug", "help", "insecure", "port=", "server="])
    except getopt.GetoptError:
        usage(sys.argv[0], 2)

    for opt, arg in opts:
        if opt in ("-d", "--debug"):
            debug = True
        elif opt in ("-h", "--help"):
            usage(sys.argv[0], 0)
        elif opt in ("-k", "--insecure"):
            url_prefix = "http://"
        elif opt in ("-p", "--port"):
            elastic_port = arg
        elif opt in ("-s", "--server"):
            elastic_server = arg

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

    results = fetch_nodes(elastic_server, elastic_port)
    print("Results:\n{0}".format(results))


if __name__ == '__main__':
    main(sys.argv[1:])
