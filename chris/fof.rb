# friends of friends
# 
# queries all edge keys from Edges column family and generates list of edges and list of nodes
# iterates over nodes and collects friends from edge_keys
# iterates over all friends for each node and collects friends of the friends
# result is collected in a hash keyed by node_id

# nodes
# column_metadata =
# [
#   {column_name: name, validation_class: UTF8Type, index_type: KEYS},
#   {column_name: gender, validation_class: UTF8Type},
#   {column_name: occupation, validation_class: UTF8Type}
# ];

# edges
# column_metadata =
# [
#   {column_name: start, validation_class: UTF8Type, index_type: KEYS},
#   {column_name: end, validation_class: UTF8Type, index_type: KEYS},
#   {column_name: weight, validation_class: UTF8Type},
#   {column_name: relation, validation_class: UTF8Type}
# ];

require 'rubygems'
require 'cassandra'
require 'pp'
#require 'java'

# connect to server
client = Cassandra.new('CDM', '127.0.0.1:9160')

start_time = Time.now

# collect all edge keys
edge_keys = []
client.each(:Edges) { |key| edge_keys << key }
#puts "", "EDGES"
#pp edge_keys

# generate list of nodes based on edge_keys
node_ids = edge_keys.collect { |key| key.split('-') }.flatten.uniq
#puts "", "NODES"
#pp node_ids

# friends and friends of friends will be stored in hash, keyed by node id
fof = {}
# intialize with empty arrays for each node id
node_ids.each { |nid| fof[nid] = [] }

# get all friends for each node
node_ids.each do |nid|
  edge_keys.each do |ekey|
    estart, eend = ekey.split('-')
    fof[nid] << eend if estart == "#{nid}"
  end
end

fof.each_pair {|nid, friends| fof[nid] = friends.uniq}

#puts "", "friends have been computed!"
#pp fof

# fof hash now contains friend ids for each node
# a la {'1' => ['2', '4', '5'], '6' => []}
# iterate over those friend ids for each node and collect THEIR friends
threads = []
fof.each_pair do |nid, friends|
  friends.each do |fid|
    threads << Thread.new(fid) do |fid|
      edge_keys.each do |ekey|
        estart, eend = ekey.split('-')
        fof[nid] << eend if estart == "#{fid}"
      end
    end
  end
end

threads.each { |t|  t.join }

fof.each_pair {|nid, friends| fof[nid] = friends.uniq}
end_time = Time.now

#puts "", "friends of friends have been computed!"
pp fof

puts "runtime: #{end_time - start_time} seconds"