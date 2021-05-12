# sqlmq
A message queue based on `"database/sql".DB`.

[![Build Status](https://github.com/lovego/sqlmq/actions/workflows/go.yml/badge.svg)](https://github.com/lovego/sqlmq/actions/workflows/go.yml)
[![Coverage Status](https://coveralls.io/repos/github/lovego/sql-mq/badge.svg)](https://coveralls.io/github/lovego/sql-mq)
[![Go Report Card](https://goreportcard.com/badge/github.com/lovego/sqlmq)](https://goreportcard.com/report/github.com/lovego/sqlmq)
[![Documentation](https://pkg.go.dev/badge/github.com/lovego/sqlmq)](https://pkg.go.dev/github.com/lovego/sqlmq)

## Features
- 支持多个节点生产消息、消费消息，多个消费节点对所有消息进行负载均衡。
- 消费失败时支持重试（自定义重试等待时间）或放弃该消息。
- 保证同一个消息至少被消费一次，但不保证只被消费一次。
- 保证同一个消息的多次消费在时间上没有重叠。
- 消费每个消息时新建一个Goroutine，不会等待上一个消息消费完成，才进行下一个消息的消费。因此保证不了先生产先消费。

## Install
`$ go get github.com/lovego/sqlmq`


