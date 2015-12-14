from __future__ import print_function

import re
import binascii
import hashlib
import unicodedata
import json

from distutils.util import strtobool
from subprocess import Popen, PIPE


def print_boxed(string):
    splitted_string = string.split('\n')
    max_len = max(map(len, splitted_string))
    box_line = u"\u2500" * (max_len + 2)

    out = u"\u250c" + box_line + u"\u2510\n"
    out += '\n'.join([u"\u2502 {} \u2502".format(line.ljust(max_len)) for line in splitted_string])
    out += u"\n\u2514" + box_line + u"\u2518"
    print(out)


def yes_no_prompt(query, default=None):
    available_prompts = {None: " [y/n] ", 'y': " [Y/n] ", 'n': " [y/N] "}

    if default not in available_prompts:
        raise ValueError("Invalid default: '{}'".format(default))

    while True:
        try:
            answer = raw_input("{0}{1}".format(query, available_prompts[default]))
            return strtobool(answer)
        except ValueError:
            if answer == '' and default is not None:
                return strtobool(default)


def chunks(sequence, n):
    """ Yield successive n-sized chunks from sequence. """
    for i in range(0, len(sequence), n):
        yield sequence[i:i + n]


def generate_uid_from_string(value):
    """ Create unique identifier from a string. """
    return hashlib.sha256(value.encode()).hexdigest()


def slugify(value):
    """
    Converts to lowercase, removes non-word characters (alphanumerics and
    underscores) and converts spaces to underscores. Also strips leading and
    trailing whitespace.

    Reference
    ---------
    https://github.com/django/django/blob/1.7c3/django/utils/text.py#L436
    """
    try:
        value = unicode(value, "UTF-8")
    except NameError:
        pass  # In Python 3 all strings are already unicode.

    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    return str(re.sub('[-\s]+', '_', value))


def encode_escaped_characters(text, escaping_character="\\"):
    """ Escape the escaped character using its hex representation """
    def hexify(match):
        # Reference: http://stackoverflow.com/questions/18298251/python-hex-values-to-convert-to-a-string-integer
        return "\\x" + binascii.hexlify(match.group()[-1].encode()).decode()

    return re.sub(r"\\.", hexify, text)


def decode_escaped_characters(text):
    """ Convert hex representation to the character it represents """
    if len(text) == 0:
        return ''

    def unhexify(match):
        return binascii.unhexlify(match.group()[2:]).decode()

    return re.sub(r"\\x..", unhexify, text)


def save_dict_to_json_file(path, dictionary):
    with open(path, "w") as json_file:
        json_file.write(json.dumps(dictionary, indent=4, separators=(',', ': ')))


def load_dict_from_json_file(path):
    with open(path, "r") as json_file:
        return json.loads(json_file.read())


def detect_cluster():
    # Get server status
    try:
        output = Popen(["qstat", "-B"], stdout=PIPE).communicate()[0]
        if isinstance(output, bytes):
            output = output.decode("utf-8")
    except OSError:
        # If qstat is not available we assume that the cluster is unknown.
        return None
    # Get server name from status
    server_name = output.split('\n')[2].split(' ')[0]
    # Cleanup the name and return it
    cluster_name = None
    if server_name.split('.')[-1] == 'm':
        cluster_name = "mammouth"
    elif server_name.split('.')[-1] == 'guil':
        cluster_name = "guillimin"
    elif server_name.split('.')[-1] == 'helios':
        cluster_name = "helios"
    elif server_name.split('.')[-1] == 'hades':
        cluster_name = "hades"
    return cluster_name


def get_launcher(cluster_name):
    if cluster_name == "helios":
        return "msub"
    else:
        return "qsub"
