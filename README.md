# test_de_python_v2

# setup

the project is not publicly available in pip repositories, you have to clone it and build your pip package :

    # clone the repository (no credential, read only)
    git clone git@github.com:laurentcarrie/test_de_python_v2.git
    cd test_de_python_v2

    # create a virtual env for python
    virtualenv .venv --python=3.8

    # activate it
    . .venv/bin/activate

    # the psycopg2 python package needs python-dev
    # we need it for the sql tests (second part)
    # if it is missing pip install psycopg2 will complain
    # sudo apt install python3-dev5

    # install requirements
    pip install -r requirements.txt

    # local install
    pip install -e .

    # run the tests
    pytest
