# sqscli

## Commands

### qtocsv
Output a queue in a csv format

```bash
usage: sqscli qtocsv [options]
options:
  -h   Help
  -queue required   Queue name
```

Example: sqscli qtocsv -q #queue_name# > myfile.csv

### qtoq
Redrive a queue messages to another queue (from a DLQ to the main queue for instance)

```
usage: sqscli qtoq [options]
options:
  -queue1 required   Queue from
  -queue2 required   Queue to
```

Example: sqscli qtoq -q1 #dlq_name# -q2 #queue_name#

## Setup

```bash
# export environment variables
export $(cat ./env/sqscli.env | xargs)
```

## How to use this.
First you must have `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` exported inside your terminal view.

```bash
export AWS_ACCESS_KEY_ID=XXXYYYZZZ
export AWS_SECRET_ACCESS_KEY=ZZ/ABCDEFGH09876543KLMNN
sqscli qtocsv -q your-queue-name > your-file-name.csv
```
