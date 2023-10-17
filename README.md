# RestOnDB-RESTQL-Python

This is a Python plug & play dynamic / automatic REST and GraphQL APIs on any relational database.
It uses the Flask Micro Web Framework with SQLAlchemy ORM for REST APIs and Graphene for GraphQL.
Check the postman collection available under /docs folder for API documentation.

Getting started
---------------

First you'll need to get the source of the project.

```bash
# Get the example project code
git clone <GIT URL>
cd db-service/
```

It is good idea (but not required) to create a virtual environment
for this project. We'll do this using
[virtualenv](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
to keep things simple,
but you may also find something like
[virtualenvwrapper](https://virtualenvwrapper.readthedocs.org/en/latest/)
to be useful:

```bash
# Create a virtualenv in which we can install the dependencies
virtualenv env
source env/bin/activate
```

Now we can install our dependencies:

```bash
pip install -r requirements.txt
```

Now the following command will setup the database, and start the server:

```bash
./app.py

```


Now head on over to
[http://127.0.0.1:5000/graphql](http://127.0.0.1:5000/graphql)
and run some queries!
