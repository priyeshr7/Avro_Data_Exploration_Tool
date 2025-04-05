from setuptools import setup, find_packages

setup(
    name='avro_explorer',
    version='0.1',
    author='Priyesh',
    packages=find_packages(),
    install_requires=[
        'fastavro',
    ],
    python_requires='>=3.7',
)
