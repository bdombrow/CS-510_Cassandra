# transitive closure
# 

# nodes
# column_metadata =
# [
#   {column_name: name, validation_class: UTF8Type, index_type: KEYS},
#   {column_name: gender, validation_class: UTF8Type},
#   {column_name: occupation, validation_class: UTF8Type},
#   {column_name: connections, validation_class: UTF8Type}
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
