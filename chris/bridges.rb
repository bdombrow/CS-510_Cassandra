# bridges


# generate adjacency matrix
graph = Array.new(node_count + 1) { Array.new(node_count + 1).fill 0 }

edge_keys.each do |ek|
  estart, eend = ek.split('-')
  row          = client.get(:Edges, ek)
  graph[estart.to_i][eend.to_i] = ek['weight'] || 1
end

index   = 0
stack   = []
bridges = []

def run(graph)
  bridges = []
  index = 0;
  stack =[]

  unless graph.nil?
    node_list = [] # get adjacent nodes
    unless node_list.nil?
      node_list.each do |node|
        tarjan(node, graph) if node.index == -1
      end
    end
  end
  bridges
end

def tarjan(node, adjacency_list)
  node.index = index;
  node.lowlink = index;
  index += 1
  stack.push(node)
  adjacency_list.each do |edge|
    n = edge['start']
    if(n.index == -1)
      tarjan(n, adjacency_list)
      node.lowlink = min(node.lowlink, n.lowlink)
    elsif stack.include? n
      node.lowlink = [node.lowlink, n.index].min
    end
  end
  
  if node.lowlink == node.index
    n = nil
    component = [] 
    while stack
      n = stack.pop
      component.push n
    end

    while n != node
      bridges.push component
    end
    bridges
  end
end

run graph