# This is a fake http/REST server.
#
#   ruby ./t/cluster_manager_mock.rb [optional_path_to_some_json_contents]
#
# For example...
#
#   ruby ./t/cluster_manager_mock.rb ./t/vbucket1.cfg
#
require 'rubygems'
require 'sinatra'

get '/pools/default/buckets/default' do
  some_json
end

get '/pools/default/bucketsStreaming/default' do
  some_json + "\n\n\n\n"
end

get '/pools/default/bucketsStreamingConfig/default' do
  some_json + "\n\n\n\n"
end

def some_json()
  if ARGV.length > 0
    return File.read(ARGV[0])
  end

  <<-eos
{
  "hashAlgorithm": "CRC",
  "numReplicas": 0,
  "serverList": ["localhost:11311"],
  "vBucketMap":
    [
      [0],
      [0],
      [0],
      [0]
    ]
}
  eos
end

