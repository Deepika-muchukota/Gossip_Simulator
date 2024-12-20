use "collections"
use "random"
use "time"
use "promises"

actor Main
  new create(env: Env) =>
    env.out.print("Starting Project 2")
    if env.args.size() != 4 then
      env.out.print("Usage: project2 numNodes topology algorithm")
      return
    end

    let num_nodes = try env.args(1)?.usize()? else 0 end
    let topology = try env.args(2)? else "" end
    let algorithm = try env.args(3)? else "" end

    let network = Network(env, num_nodes, topology, algorithm)
    network.start()
    network.wait_for_completion()

actor Network
  let env: Env
  let nodes: Array[Node]
  let topology: String
  let algorithm: String
  let num_nodes: USize
  var start_time: U64 = 0
  var converged_count: USize = 0
  let _complete: Promise[None] = Promise[None]
  var algorithm_complete: Bool = false

  new create(env': Env, num_nodes': USize, topology': String, algorithm': String) =>
    env = env'
    num_nodes = num_nodes'
    topology = topology'
    algorithm = algorithm'
    nodes = Array[Node](num_nodes)

    env.out.print("Creating network with " + num_nodes.string() + " nodes, topology: " + topology + ", algorithm: " + algorithm)

    for i in Range(0, num_nodes) do
      nodes.push(Node(i, this, env))
    end

    try
      match topology
      | "full" => create_full_network()?
      | "3D" => create_3d_grid()?
      | "line" => create_line()?
      | "imp3D" => create_imperfect_3d_grid()?
      else
        env.out.print("Invalid topology: " + topology)
      end
    else
      env.out.print("Error creating network topology")
    end

    env.out.print("Network created successfully")

  be start() =>
    env.out.print("Starting algorithm: " + algorithm)
    start_time = Time.millis()
    
    try
      match algorithm
      | "gossip" => start_gossip()?
      | "push-sum" => start_push_sum()?
      else
        env.out.print("Invalid algorithm: " + algorithm)
      end
    else
      env.out.print("Error starting algorithm")
    end

    Timeout(this, 120_000)

  be timeout() =>
    if not algorithm_complete then
      env.out.print("Timeout reached. Algorithm did not fully converge.")
      env.out.print(converged_count.string() + " out of " + num_nodes.string() + " nodes converged.")
      _complete(None)
    end

  be node_converged() =>
    if not algorithm_complete then
      converged_count = converged_count + 1
      env.out.print(converged_count.string() + " nodes have converged")
      if converged_count == num_nodes then
        algorithm_complete = true
        let end_time = Time.millis()
        env.out.print("All nodes have converged")
        env.out.print("Convergence time: " + (end_time - start_time).string() + " ms")
        _complete(None)
      end
    end

  be wait_for_completion() =>
    _complete.next[None](
      {(n: None) => None } iso,
      {() => None } iso
    )

  fun ref create_full_network() ? =>
    for i in Range(0, num_nodes) do
      for j in Range(0, num_nodes) do
        if i != j then
          nodes(i)?.add_neighbor(nodes(j)?)
        end 
      end 
    end

  fun ref create_3d_grid() ? =>
  let size = (num_nodes.f64().pow(1.0/3.0).ceil()).usize()
  for i in Range(0, num_nodes) do 
    let x = i % size 
    let y = (i / size) % size 
    let z = i / (size * size)

    if x > 0 then nodes(i)?.add_neighbor(nodes(i-1)?) end 
    if (x < (size - 1)) and ((i + 1) < num_nodes) then nodes(i)?.add_neighbor(nodes(i+1)?) end 
    if y > 0 then nodes(i)?.add_neighbor(nodes(i-size)?) end 
    if (y < (size - 1)) and ((i + size) < num_nodes) then nodes(i)?.add_neighbor(nodes(i+size)?) end 
    if z > 0 then nodes(i)?.add_neighbor(nodes(i-(size*size))?) end 
    if (z < (size - 1)) and ((i + (size*size)) < num_nodes) then nodes(i)?.add_neighbor(nodes(i+(size*size))?) end 
  end 

 fun ref create_line() ? => 
   for i in Range(0, num_nodes) do 
     if i > 0 then nodes(i)?.add_neighbor(nodes(i-1)?) end 
     if i < (num_nodes - 1) then nodes(i)?.add_neighbor(nodes(i+1)?) end 
   end 

 fun ref create_imperfect_3d_grid() ? => 
   create_3d_grid()? 
   let rand = Rand 
   for i in Range(0, num_nodes) do 
     var random_neighbor = rand.int(num_nodes.u64()).usize()
     while random_neighbor == i do 
       random_neighbor = rand.int(num_nodes.u64()).usize()
     end 
     nodes(i)?.add_neighbor(nodes(random_neighbor)?)
   end 

 fun ref start_gossip() ? => 
   let rand = Rand 
   let start_node = rand.int(num_nodes.u64()).usize()
   nodes(start_node)?.start_gossip()

 fun ref start_push_sum() ? =>
   let rand = Rand 
   let start_node = rand.int(num_nodes.u64()).usize()
   nodes(start_node)?.start_push_sum()

actor Node 
  let id: USize 
  let neighbors: Array[Node] 
  let network: Network 
  let env: Env 
  var rumor_count: USize 
  var converged: Bool 
  var has_notified_convergence: Bool 
  let rand: Rand 

  var s: F64 // For push-sum state.
  var w: F64 // For push-sum weight.
  var consecutive_rounds: USize // For convergence check.

  new create(id': USize, network': Network, env': Env) => 
    id = id' 
    network = network' 
    env = env' 
    neighbors = Array[Node] 
    rumor_count = 0 
    converged = false 
    has_notified_convergence = false 
    rand = Rand 
    s = id'.f64()
    w = 1.0
    consecutive_rounds = 0

  be add_neighbor(neighbor: Node) => neighbors.push(neighbor)

  be receive_rumor() =>  
    rumor_count = rumor_count + 1 
    if (not converged) and (rumor_count >= 10) then  
      converged = true  
      if not has_notified_convergence then  
        network.node_converged()
        has_notified_convergence = true  
      end  
    end  
    forward_rumor()

  fun ref forward_rumor() =>  
    if neighbors.size() > 0 then  
      try  
        let random_neighbor = neighbors(rand.int(neighbors.size().u64()).usize())?  
        random_neighbor.receive_rumor()
      else  
        None  
      end  
    end  

  be start_gossip() => receive_rumor()

  be start_push_sum() => send_push_sum()

  be receive_push_sum(s': F64, w': F64) =>
    let old_ratio = s / w
    s = s + s'
    w = w + w'
    let new_ratio = s / w
    if (old_ratio - new_ratio).abs() < 1e-10 then
      consecutive_rounds = consecutive_rounds + 1
      if (consecutive_rounds >= 3) and (not converged) then
        converged = true
        network.node_converged()
      end
    else
      consecutive_rounds = 0
    end
    send_push_sum()

  fun ref send_push_sum() =>
    s = s / 2.0
    w = w / 2.0
    if neighbors.size() > 0 then
      try  
        let random_neighbor = neighbors(rand.int(neighbors.size().u64()).usize())?
        random_neighbor.receive_push_sum(s, w)
      else
        None  
      end  
    end

actor Timeout  
  let _network: Network
  let _timeout: U64

  new create(network: Network, timeout: U64) =>
    _network = network
    _timeout = timeout
    _wait()

  be _wait() =>
    let start_time = Time.millis()
    while (Time.millis() - start_time) < _timeout do
      None
    end
    _network.timeout()
