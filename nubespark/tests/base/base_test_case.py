__author__ = 'topsykretts'

import unittest
from abc import ABCMeta, abstractclassmethod


class BaseTestCase(unittest.TestCase, metaclass=ABCMeta):
    """
    BaseTestCase for all the tests of this api
    Useful for initializing some test datasets
    """
    @abstractclassmethod
    def setUpClass(cls):
        """ The automatic setup method for subclasses """
        return

    @abstractclassmethod
    def tearDownClass(cls):
        """ The automatic tear down method for subclasses """
        return
