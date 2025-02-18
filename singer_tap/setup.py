from setuptools import find_packages, setup  # type: ignore

setup(
    name="singer_tap",
    version="0.1.0",
    packages=find_packages(),
    package_data={
        "": ["py.typed"],
    },
    install_requires=[
        "singer-python==6.1.0",
        "jsonschema>=2.6.0,<3.0.0",
        "requests",
        "snowflake-connector-python",
        "pytz",
    ],
)
