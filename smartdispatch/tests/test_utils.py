# -*- coding: utf-8 -*-
import unittest
try:
    from mock import patch
    import mock
except ImportError:
    from unittest.mock import patch
    import unittest.mock
from nose.tools import assert_equal, assert_true
from numpy.testing import assert_array_equal
import subprocess

from smartdispatch import utils

class PrintBoxedTests(unittest.TestCase):

    def setUp(self):
        self.empty = ''
        self.text = "This is weird test for a visual thing.\nWell maybe it's fine to test it's working."

    def test_print_boxed(self):
        utils.print_boxed(self.text)

    def test_print_boxed_empty(self):
        utils.print_boxed(self.empty)


def test_chunks():
    sequence = range(10)

    for n in range(1, 11):
        expected = []
        for start, end in zip(range(0, len(sequence), n), range(n, len(sequence) + n, n)):
            expected.append(sequence[start:end])

        assert_array_equal(list(utils.chunks(sequence, n)), expected, "n:{0}".format(n))


def test_generate_uid_from_string():
    assert_equal(utils.generate_uid_from_string("same text"), utils.generate_uid_from_string("same text"))
    assert_true(utils.generate_uid_from_string("same text") != utils.generate_uid_from_string("sametext"))

def test_jobname_generator():
    assert_equal(len(utils.jobname_generator("a-string-which-has-a-longer-length-nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn-than-64", 8798777)), 64)
    assert_equal(len(utils.jobname_generator("abcde",3)), 7)
    assert_equal(len(utils.jobname_generator("",21)), 3)

def test_slugify():
    testing_arguments = [("command", "command"),
                         ("/path/to/arg2/", "pathtoarg2"),
                         ("!\"/$%?&*()[]~{<>'.#|\\", ""),
                         ("éèàëöüùò±@£¢¤¬¦²³¼½¾", "eeaeouuo23141234"),  # ¼ => 1/4 => 14
                         ("arg with space", "arg_with_space")]

    for arg, expected in testing_arguments:
        assert_equal(utils.slugify(arg), expected)

command_output = """\
Server             Max   Tot   Que   Run   Hld   Wat   Trn   Ext   Com Status
----------------   ---   ---   ---   ---   ---   ---   ---   ---   --- ----------
gpu-srv1.{}      0  1674   524   121    47     0     0    22   960 Idle
"""

slurm_command = """\
   Cluster     ControlHost  ControlPort   RPC     Share GrpJobs       GrpTRES GrpSubmit MaxJobs       MaxTRES MaxSubmit     MaxWall                  QOS   Def QOS
---------- --------------- ------------ ----- --------- ------- ------------- --------- ------- ------------- --------- ----------- -------------------- ---------
      {}  132.204.24.224         6817  7680         1                                                                                           normal
"""


class ClusterIdentificationTest(unittest.TestCase):

    def test_detect_cluster(self):
        server_name = ["hades", "m", "guil", "helios", "hades"]
        clusters = ["hades", "mammouth", "guillimin", "helios"]

        for name, cluster in zip(server_name, clusters):
            with patch('smartdispatch.utils.Popen') as mock_communicate:
                mock_communicate.return_value.communicate.return_value = (command_output.format(name),)
                self.assertEquals(utils.detect_cluster(), cluster)

    # def test_detect_mila_cluster(self):
    #     with patch('smartdispatch.utils.Popen') as mock_communicate:
    #         mock_communicate.return_value.communicate.side_effect = OSError
    #         self.assertIsNone(utils.detect_cluster())

    def test_get_slurm_cluster_name(self):
        clusters = ["graham", "cedar", "mila"]

        for cluster in clusters:
            with patch('smartdispatch.utils.Popen') as mock_communicate:
                mock_communicate.return_value.communicate.return_value = (slurm_command.format(cluster),)
                self.assertEquals(utils.get_slurm_cluster_name(), cluster)
