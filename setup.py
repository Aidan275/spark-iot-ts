__author__ = 'topsykretts'

from setuptools import setup, find_packages

setup(
   name='nubespark',
   version='1.0',
   description='A module for processing IOT timeseries data with spark',
   author='Shishir Adhikari',
   author_email='itsmeccr@gmail.com',
   packages=find_packages(),  #same as name
   # install_requires=['bar', 'greek'], #external packages as dependencies
)
