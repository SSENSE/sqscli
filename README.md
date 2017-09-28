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

## Setup

```bash
# export environment variables
export $(cat ./env/sqscli.env | xargs)
```