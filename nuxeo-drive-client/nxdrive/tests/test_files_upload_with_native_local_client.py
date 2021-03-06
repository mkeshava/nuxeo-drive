'''
Created on Mar 22, 2016

@author: constantinm
'''

import os
import sys
import tempfile
import shutil
from functools import partial
from multiprocessing import Pool
from unittest import skipIf

from common_unit_test import UnitTestCase
from common_unit_test import log

if sys.platform == 'darwin':
    from local_client import MacFileManagerUtils

# TODO the TEST_* values depend on the local configuration for your test files
TEST_DIR = 'test_data'
# per SUPNXP-15494 this shall contain at least 100 files
TEST_ZIP = 'files.zip'
DEST_DIR = 'folder1'


# TODO Add support for Windows client here
if sys.platform == 'darwin':
    def copy(src, dst):
        MacFileManagerUtils.copy(src, dst)


def _get_copy_to(dst):
    return partial(copy, dst=dst)


def _get_copy_from(src):
    return partial(copy, src=src)


class TestUpload100PlusFiles(UnitTestCase):
    __name__ = 'TestUpload100PlusFiles'

    @classmethod
    def setUpClass(cls):
        '''
        cannot prepare the set of files with xattr here since it requires the infrastructure of
        engine, manager, clients, etc. to be setup
        '''
        root_path = cls._get_root_path()
        cls.tempdir = tempfile.mkdtemp(dir=root_path)

    def setUp(self):
        '''
        Unzip and sync the test files in order to create the file attribute (remote ref id),
        then save them in a temp location.
        Do this only once as it applies to all tests.
        :return: None
        '''
        super(TestUpload100PlusFiles, self).setUp()

        log.debug('*** enter TestUpload100PlusFiles.setUp()')
        log.debug('*** engine1 starting')
        self.engine_1.start()
        self.wait_sync()
        self.native_client = self.get_local_client(os.path.join(self.local_nxdrive_folder_1, u'My Docs'))

        if not os.path.exists(os.path.join(TestUpload100PlusFiles.tempdir, DEST_DIR)):
            # create the destination folder
            self.local_root_client_1.make_folder('/', DEST_DIR)

            # and copy data from root (<root>/test_data/files.zip) into the folder
            root_path = self._get_root_path()
            self._extract_test_files(os.path.join(root_path, TEST_ZIP), self.local_root_client_1._abspath('/' + DEST_DIR))

            self.engine_1.start()
            self.wait_sync()

            # move the synced folder into a temporary directory
            shutil.move(self.local_root_client_1._abspath(u'/' + DEST_DIR), TestUpload100PlusFiles.tempdir)

        log.debug('*** exit TestUpload100PlusFiles.setUp()')

    def tearDown(self):
        if getattr(self, 'native_client', None):
            del self.native_client
            self.native_client = None
        super(TestUpload100PlusFiles, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir)

    def test_sync_folder(self):
        self.native_client.make_folder(u'/', u'to_folder')

        # copy all files from tempdir to the new folder under My Docs
        self.native_client.copy(os.path.join(TestUpload100PlusFiles.tempdir, DEST_DIR),
                                self.native_client._abspath(u'/to_folder'))

        self.wait_sync()
        dest_remote_ref = self.local_root_client_1.get_remote_id(u'/to_folder/' + DEST_DIR)
        remote_children = self.remote_file_system_client_1.get_children_info(dest_remote_ref)
        local_children = os.listdir(self.local_root_client_1._abspath(u'/to_folder/' + DEST_DIR))
        self.assertEqual(len(remote_children), len(local_children),
                         'number of remote children (%d) does not match number of local children (%d)' %
                         (len(remote_children), len(local_children)))

    '''
    TODO Add support for Windows client here
    '''
    @skipIf(sys.platform != 'darwin', 'test is only for Mac')
    def test_sync_folder_multi(self):
        self.native_client.make_folder(u'/', u'to_folder2')
        # copy all files from tempdir to the new folder under My Docs
        copy_to = _get_copy_to(self.native_client._abspath(u'/to_folder2'))
        src_path = os.path.join(TestUpload100PlusFiles.tempdir, DEST_DIR)
        pool = Pool(5)
        pool.map(copy_to, [os.path.join(src_path, f) for f in os.listdir(src_path)])

        self.wait_sync()
        pool.close()
        pool.join()
        dest_remote_ref = self.local_root_client_1.get_remote_id(u'/to_folder2')
        remote_children = self.remote_file_system_client_1.get_children_info(dest_remote_ref)
        local_children = os.listdir(self.local_root_client_1._abspath(u'/to_folder2'))
        self.assertEqual(len(remote_children), len(local_children),
                         'number of remote children (%d) does not match number of local children (%d)' %
                         (len(remote_children), len(local_children)))

    @classmethod
    def _get_root_path(cls):
        '''
        TODO modify to match the location for your test archive
        :return: root path to the archive of test files
        '''
        import clouddesktop as cdd

        try:
            test_resources_path = os.path.join(
                os.path.join(
                        os.path.join(os.path.dirname(os.path.dirname(cdd.__file__)),
                                     os.pardir), TEST_DIR))
            return test_resources_path
        except Exception as e:
            log.error('path error: ', e)

    def _extract_test_files(self, zip_file, dest_dir):
        import zipfile

        with zipfile.ZipFile(zip_file) as zf:
            for member in zf.infolist():
                # Path traversal defense copied from
                # http://hg.python.org/cpython/file/tip/Lib/http/server.py#l789
                words = member.filename.split('/')
                path = dest_dir
                for word in words[:-1]:
                    drive, word = os.path.splitdrive(word)
                    head, word = os.path.split(word)
                    if word in (os.curdir, os.pardir, ''):
                        continue
                    path = os.path.join(path, word)
                zf.extract(member, path)
