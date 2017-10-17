import re
import hashlib
import unicodedata
import json
import sys
import six
import functools

from distutils.util import strtobool
from subprocess import Popen, PIPE

HELIOS_ADVICE = ("On Helios, don't forget that the queue gpu_1, gpu_2, gpu_4"
                 " and gpu_8 give access to a specific amount of gpus."
                 "\nFor more advices, please refer to the official"
                 " documentation 'https://wiki.calculquebec.ca/w/Helios/en'")

MAMMOUTH_ADVICE = ("On Mammouth, please refer to the official documentation"
                   " for more information:"
                   " 'https://wiki.ccs.usherbrooke.ca/Accueil/en'")

HADES_ADVICE = ("On Hades, don't forget that the queue name '@hades' needs"
                " to be used.\nFor more advices, please refer to the"
                " official documentation: 'https://wiki.calculquebec.ca/w"
                "/Ex%C3%A9cuter_une_t%C3%A2che/en#tab=tab5'")

GUILLIMIN_ADVICE = ("On Guillimin, please refer to the official documentation"
                    " for more information: 'http://www.hpc.mcgill.ca/"
                    "index.php/starthere'")


def get_advice(cluster_name):

    if cluster_name == "helios":
        return HELIOS_ADVICE
    elif cluster_name == 'mammouth':
        return MAMMOUTH_ADVICE
    elif cluster_name == 'hades':
        return HADES_ADVICE
    elif cluster_name == "guillimin":
        return GUILLIMIN_ADVICE

    return ''


def jobname_generator(jobname, job_id):
    '''Crop the jobname to a maximum of 64 characters.
    Parameters
    ----------
    jobname : str
    Initial jobname.
    job_id: str
    ID of the job in the current batch.
    Returns
    -------
    str
    The cropped version of the string.  
    '''
    # 64 - 1 since the total length including -1 should be less than 64
    job_id = str(job_id)
    if len(jobname) + len(job_id) > 63:
        croped_string = '{}_{}'.format(jobname[:63 - len(job_id)], job_id)
    else:
        croped_string = '{}_{}'.format(jobname, job_id)
    return croped_string


def print_boxed(string):
    splitted_string = string.split('\n')
    max_len = max(map(len, splitted_string))
    box_line = u"\u2500" * (max_len + 2)

    out = u"\u250c" + box_line + u"\u2510\n"
    out += '\n'.join([u"\u2502 {} \u2502".format(line.ljust(max_len)) for line in splitted_string])
    out += u"\n\u2514" + box_line + u"\u2518"
    print out


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
    for i in xrange(0, len(sequence), n):
        yield sequence[i:i + n]


def generate_uid_from_string(value):
    """ Create unique identifier from a string. """
    return hashlib.sha256(value).hexdigest()


def slugify(value):
    """
    Converts to lowercase, removes non-word characters (alphanumerics and
    underscores) and converts spaces to underscores. Also strips leading and
    trailing whitespace.

    Reference
    ---------
    https://github.com/django/django/blob/1.7c3/django/utils/text.py#L436
    """
    value = unicodedata.normalize('NFKD', unicode(value, "UTF-8")).encode('ascii', 'ignore').decode('ascii')
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    return str(re.sub('[-\s]+', '_', value))


def encode_escaped_characters(text, escaping_character="\\"):
    """ Escape the escaped character using its hex representation """
    def hexify(match):
        return "\\x{0}".format(match.group()[-1].encode("hex"))

    return re.sub(r"\\.", hexify, text)


def decode_escaped_characters(text):
    """ Convert hex representation to the character it represents """
    if len(text) == 0:
        return ''

    def unhexify(match):
        return match.group()[2:].decode("hex")

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


def rethrow_exception(exception, new_message):

    def func_wraper(func):

        @functools.wraps(func)
        def test_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception as e:

                orig_exc_type, orig_exc_value, orig_exc_traceback = sys.exc_info()
                new_exc = Exception(new_message)
                new_exc.reraised = True
                new_exc.__cause__ = orig_exc_value

                new_traceback = orig_exc_traceback
                six.reraise(type(new_exc), new_exc, new_traceback)


        return test_func
    return func_wraper
