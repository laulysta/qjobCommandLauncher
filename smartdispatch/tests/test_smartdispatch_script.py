import unittest
from smartdispatch import smartdispatch_script
import subprocess
from mock import patch
import tempfile as tmp
import shutil
import traceback

class TestSmartScript(unittest.TestCase):

    def setUp(self):
        self._base_dir = tmp.mkdtemp()
        smartdispatch_script.LOGS_FOLDERNAME = self._base_dir

    def tearDown(self):
        shutil.rmtree(self._base_dir)

    def test_gpu_check(self):

        argv = ['-x', '-g', '2', '-G', '1', '-C', '1', '-q', 'random', '-t', '00:00:10' ,'launch', 'echo', 'testing123']

        # Test if the check fail
        with self.assertRaises(SystemExit) as context:
            smartdispatch_script.main(argv=argv)

        self.assertTrue(context.exception.code, 2)

        # Test if the test pass
        argv[2] = '0'

        try:
            smartdispatch_script.main(argv=argv)
        except SystemExit as e:
            self.fail("The command failed the check, but it was supposed to pass.")


    def test_cpu_check(self):

        argv = ['-x', '-c', '2', '-C', '1', '-G', '1', '-t', '00:00:10', '-q', 'random', 'launch', 'echo', 'testing123']

        # Test if the check fail
        with self.assertRaises(SystemExit) as context:
            smartdispatch_script.main(argv=argv)

        self.assertTrue(context.exception.code, 2)

        # Test if the test pass
        argv[2] = '1'

        try:
            smartdispatch_script.main(argv=argv)
        except SystemExit as e:
            self.fail("The command failed the check, but it was supposed to pass.")



    @patch('subprocess.check_output')
    def test_launch_job_check(self, mock_check_output):

        mock_check_output.side_effect = subprocess.CalledProcessError(1, 1, "A wild error appeared!")
        argv = ['-t', '0:0:1', '-G', '1', '-C', '1', '-q', 'random', 'launch', 'echo', 'testing123']

        # Test if the test fail.
        try:
            with self.assertRaises(SystemExit) as context:
                smartdispatch_script.main(argv=argv)

                self.assertTrue(context.exception.code, 2)
        
        except subprocess.CalledProcessError:
            self.fail("smartdispatch_script.main() raised CalledProcessError unexpectedly:\n {}".format(traceback.format_exc()))

        # Test if the test pass (i.e the script run normaly)
        mock_check_output.side_effect = None
        mock_check_output.return_value = ""

        try:
            smartdispatch_script.main(argv=argv)
        except SystemExit as e:
            self.fail("The launcher had no problem, but the script failed nonetheless.")
