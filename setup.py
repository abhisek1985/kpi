from setuptools import setup, find_packages
from codecs import open
from os import path

pwd = path.abspath(path.dirname(__file__))

with open(path.join(pwd, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='kpi',
    version='1.0.0',
    description='KaraKira Analytics API',
    long_description=long_description,
    author='Mayukh Sarkar',
    author_email='mayukh2012@hotmail.com',
    packages=find_packages(),
    entry_points={  # Optional
        'console_scripts': [
            'KPI=kpi.__main__:main',
        ],
    },
)
