#!/usr/bin/ruby

require "rubygems"
require "thrift_client"
$:.push('./target/gen-rb')
require "funny_name_generator"

client = ThriftClient.new(FunnyNameGenerator::Client, "localhost:9999", :transport_wrapper => nil)
p client.get_name()
