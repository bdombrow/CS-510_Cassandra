# transitive closure
# 

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

# connect to server
client = Cassandra.new('CDM', '127.0.0.1:9160')

start_time = Time.now

# collect all edge keys
edge_keys = []
client.each(:Edges) { |key| edge_keys << key }

# generate list of node ids based on edge_keys
node_ids   = edge_keys.collect { |key| key.split('-') }.flatten.uniq
node_count = node_ids.length

# Assume a function edgeCost(i,j) which returns the cost of the edge from i to j
# (infinity if there is none).
# Also assume that n is the number of vertices and edgeCost(i,i) = 0
path = Array.new(node_count + 1) { Array.new(node_count + 1).fill(1/0.0) }

# connected to self!
node_ids.each { |id| path[id.to_i][id.to_i] = 1 }

edge_keys.each do |ek|
  estart, eend = ek.split('-')
  row          = client.get(:Edges, ek)
  path[estart.to_i][eend.to_i] = 1
  path[eend.to_i][estart.to_i] = 1
end

# A 2-dimensional matrix. At each step in the algorithm, path[i][j] is the shortest path
# from i to j using intermediate vertices (1..k−1).  Each path[i][j] is initialized to
# edgeCost(i,j).
def floyd_warshall(node_ids, path)
  node_count = node_ids.length
  (0..node_count - 1).each do |k|
    (0..node_count - 1).each do |i|
      (0..node_count - 1).each do |j|
        # if path[i][j] or path[i][k] or path[k][j]
        path[i][j] = [ path[i][j], path[i][k] + path[k][j] ].min
      end
    end
  end
  path
end

puts "", "transitive closure"
result = floyd_warshall(node_ids, path)

# cleanup infinities - replace with 0's
result.map! { |row| row.each_with_index { |val, i| row[i] = 0 if row[i] == (1/0.0)} }

end_time = Time.now

pp result
puts "runtime: #{end_time - start_time} seconds"