from setuptools import setup


def readfile(filename):
    with open(filename, 'r+') as f:
        return f.read()


setup(
    name="mrpyconsole",
    version="2018.05.29",
    description="a simple console to run Mad Reduce (streaming) python programs on a distant Hadoop server",
    long_description=readfile('README.md'),
    author="David R. L. Zarebski",
    author_email="zarebskidavid@gmail.com",
    url="http://zarebski.io/",
    py_modules=['mrpyconsole'],
    license=readfile('LICENSE'),
    entry_points={
        'console_scripts': [
            'mrpyconsole = mrpyconsole:mrpyconsole'
        ]
    },
)
