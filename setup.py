from setuptools import setup, find_packages

setup(
    name="avro_data_exploration_tool",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastavro>=1.4.0",
    ],
    description="A tool for exploring Avro files",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/priyeshr7/Avro_Data_Exploration_Tool",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.6",
)