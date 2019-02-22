from setuptools import setup, find_packages

setup(name='dataflow-transactions-demo',
  version='0.1',
  description='Dependencies',

  install_requires=[
    'google-api-python-client',
    'urllib3',
  ],
  packages = find_packages()
 )
