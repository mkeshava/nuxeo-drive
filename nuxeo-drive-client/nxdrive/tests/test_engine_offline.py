'''
Created on 11-Jul-2016

@author: arameshkumar

Files/folders should not sync while engine is paused due to a network error. In this test case, mock is used to simulate a network error

'''

from common_unit_test import UnitTestCase
import nxdrive
from mock import patch
from urllib2 import URLError
from time import sleep
from nxdrive.client.base_automation_client import BaseAutomationClient

server_online = True
original_execute = BaseAutomationClient.execute
original_fetch_api = BaseAutomationClient.fetch_api


class EngineOffLineTestCase(UnitTestCase):

    '''
    1. fetch_api() and execute() methods in BaseAutomationClient are mocked to simulate network error
    2. Files/folders should not sync while engine is paused due to a network error
    3. Files/folders should not sync on manual pause and should sync on resume.
    '''

    def setUp(self):
        super(EngineOffLineTestCase, self).setUp()
        # Start engine and wait for Sync
        self.engine_1.start()
        self.wait_sync(wait_for_async=30)

    def tearDown(self):
        pass

    def mock_fetch_api(self):
        global server_online
        if not server_online:
            raise URLError("server is offline in mock_fetch_api:")
        return original_fetch_api(self)

    def mock_execute(self, *args, **kwargs):
        global server_online
        if not server_online and ('NuxeoDrive' in args[0] or 'Workspace' in args[0] or args[0].strip('/').endswith('/automation')):
            # block all Nuxeo Drive APIs and also UserWorkspace.Get API only when server offline
            raise URLError("server is offline in mock_execute:")
        return original_execute(self, *args, **kwargs)

    @patch.object(nxdrive.client.base_automation_client.BaseAutomationClient, 'fetch_api', mock_fetch_api)
    @patch.object(nxdrive.client.base_automation_client.BaseAutomationClient, 'execute', mock_execute)
    def test_with_mock_server_offline(self):

        '''
        1. Simulate Network Error
        2. Create a local folder inside user folder
        3. Create a file inside the folder
        4. Check the pair_state and error_count for the folder/files when engine goes offline
        5. Call original method to resume engine
        4. Check the pair_state when engine goes online
        '''

        global server_online
        server_online = False
        # Wait for GetChangeSummary to call check_offline method
        sleep(35)

        # Create a folder inside user folder and a file inside the folder
        self.local_client_1.make_folder('/', 'FolderA')
        self.local_client_1.make_file('/FolderA', 'TestFile.txt', content="test network failure")
        # Wait for events to be handled
        sleep(35)

        # Check pair_state and error_count for the folder/file because it should not try to sync when engine is offline
        test_folder = self.get_dao_state_from_engine_1('/FolderA')
        self.assertEqual(test_folder.pair_state, 'locally_created')
        self.assertEqual(test_folder.error_count, 0)
        test_file = self.get_dao_state_from_engine_1('/FolderA/TestFile.txt')
        self.assertEqual(test_file.pair_state, 'locally_created')
        self.assertEqual(test_file.error_count, 0)

        # Stop mocking and call original method
        global server_online
        server_online = True
        # Wait for queue_manager to process for the folder/file
        sleep(60)

        # pair_state should be 'synchronized' for the folder/file
        test_folder = self.get_dao_state_from_engine_1('/FolderA')
        self.assertEqual(test_folder.pair_state, 'synchronized')
        test_file = self.get_dao_state_from_engine_1('/FolderA/TestFile.txt')
        self.assertEqual(test_file.pair_state, 'synchronized')

    def test_with_manual_pause_resume(self):

        '''
        1. Pause the engine
        2. Create a local folder inside user folder
        3. Create a file inside the folder
        4. Check the pair_state and error_count for the folder/files when engine is paused
        5. Resume the engine
        4. Check the pair_state when engine is resumed
        '''

        # Suspend the engine
        self.engine_1.suspend()

        # Create a folder inside user folder and a file inside the folder
        self.local_client_1.make_folder('/', 'FolderB')
        self.local_client_1.make_file('/FolderB', 'TestFile1.txt', content="test manual pause and resume")
        # Wait for events to be handled
        sleep(35)

        # queue manager will not process for the folder/file when engine is paused and
        # database should not have entries for the folder/file
        test_folder = self.get_dao_state_from_engine_1('/FolderB')
        self.assertIsNone(test_folder)
        test_file = self.get_dao_state_from_engine_1('/FolderB/TestFile1.txt')
        self.assertIsNone(test_file)

        # Resume the engine
        self.engine_1.resume()
        # Wait for queue_manager to process the folder/file
        sleep(60)

        # pair_state should be 'synchronized' for the folder/file
        test_folder = self.get_dao_state_from_engine_1('/FolderB')
        self.assertEqual(test_folder.pair_state, 'synchronized')
        test_file = self.get_dao_state_from_engine_1('/FolderB/TestFile1.txt')
        self.assertEqual(test_file.pair_state, 'synchronized')
