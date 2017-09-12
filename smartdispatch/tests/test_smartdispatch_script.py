import unittest
from smartdispatch import smartdispatch_script
import subprocess
from mock import patch
import tempfile as tmp
import shutil


class TestSmartScript(unittest.TestCase):

    def setUp(self):
        self._base_dir = tmp.mkdtemp()
        smartdispatch_script.LOGS_FOLDERNAME = self._base_dir

    def tearDown(self):
        shutil.rmtree(self._base_dir)

    def test_gpu_check(self):

        argv = ['-x', '-g', '2', '-G', '1', '-q', 'gpu_1', 'launch', 'echo', 'testing123']

        with self.assertRaises(SystemExit) as context:
            smartdispatch_script.main(argv=argv)

        self.assertTrue(context.exception.code, 2)

    def test_cpu_check(self):

        argv = ['-x', '-c', '2', '-C', '1', '-q', 'gpu_1', 'launch', 'echo', 'testing123']

        with self.assertRaises(SystemExit) as context:
            smartdispatch_script.main(argv=argv)

        self.assertTrue(context.exception.code, 2)

    @patch('subprocess.check_output')
    def test_launch_job_check(self, mock_check_output):

        mock_check_output.side_effect = subprocess.CalledProcessError(1, 1, "A wild error appeared!")
        argv = ['-q', 'gpu_1', 'launch', 'echo', 'testing123']

        try:
            with self.assertRaises(SystemExit) as context:
                smartdispatch_script.main(argv=argv)

                self.assertTrue(context.exception.code, 2)

        except subprocess.CalledProcessError:
            self.fail("smartdispatch_script.main() raised CalledProcessError unexpectedly!")
