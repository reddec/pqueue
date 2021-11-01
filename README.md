# Persistent queue

Persistent queue based on [bbolt DB](https://github.com/boltdb/bolt).

Supposed to be used as embeddable persistent queue to any Go application.

Features:

- messages are not limited by RAM size, stream-oriented data
- works good for small (automatic inlining) and for big streams
- simple, portable storage structure
- go-routine safe
- supports multiple-writers and multiple-readers
- supports ack/nack (commit with discard)

See [go-docs](https://pkg.go.dev/github.com/reddec/pqueue) for examples and details.

Requirements:
- go 1.17

## Motivation

I wanted to create an application for resource-constrained devices (ie: AWS Lightsail, Raspberry Pi Zero W, etc..)
which should store (always) and forward (eventually) information with very unreliable network
connection (ie: days without a link).
Information could be small (sensors) or huge (webhooks). Devices themselves may experience a power outage.

It means:
- stored information (number and individual records) may grow much above RAM
- stored information should not be marked as processed before explicit commit
- full-packed solutions like RabbitMQ/Kafka/etc can not be applied
- due to distributed nature of the system duplicates inevitable, however, should be a cheap way to deduplicate (ie: unique ID)