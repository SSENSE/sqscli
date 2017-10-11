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
