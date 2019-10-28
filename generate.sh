#!/bin/sh


bundle exec grpc_tools_ruby_protoc --ruby_out=./lib/fluent/plugin --grpc_out=./lib/fluent/plugin -I ./proto_fluentd fluentd.proto



