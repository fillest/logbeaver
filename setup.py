import os
from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))

requirements = [
]

setup(
    name='logbeaver',
    version='0.1.17',
    classifiers=[
      "Programming Language :: Python",
      ],
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=requirements,
)
