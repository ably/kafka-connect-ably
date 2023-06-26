# System / Load Test App

This is a simple Python app that makes it easy to simulate load
on the Ably Kafka connector using structured JSON records with
a registered schema.

The app will run on Python 3.10+.

## Install

To install the app, make use of [Pipenv](https://pipenv.pypa.io/en/latest/).
Check that your system is running Python 3.10+, or that you have a recent
Python distribution available with your preferred method for managing runtimes.
You can then pull in the dependencies to a new virtual environment with:

```
  $ python -m pipenv sync
```

Now just start a shell in your new virtual env and you should be ready to go:

```
  $ python -m pipenv shell
```

## Runmning the tests

The app has built-in command line help, so just run the following from your
virtual environment shell:

```
  $ python -m loadgen.main --help
```

It should print all the usage instructions. The main things you'll need to
provide are:

* An Ably client key that can be used to subscribe to the test channels
* Kafka bootstrap server addresses, used to post test records to Kafka
* Kafka topic name to post records to
* A schema registry URL, to register the JSON schema used in test records
* Parameters to control the rates and sizes of messages, numbers of Ably
  channels, etc.

  