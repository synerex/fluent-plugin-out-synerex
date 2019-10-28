
require 'grpc'
require 'synerex_services_pb'
require 'nodeapi_services_pb'
require 'optparse'
require 'logger'
require 'anyflake'
require 'google/protobuf/well_known_types'

include GRPC::Core::TimeConsts

# Globals Variables

$sxstub = nil
$nodeInfo = nil
$nodestub = nil
$threads = []
$updateCount = 0
$status = ""
$clientID = 0
$idgen = nil


module StdoutLogger
  def logger
    LOGGER
  end

  LOGGER = Logger.new(STDOUT)
end

GRPC.extend(StdoutLogger)

def do_notifyDemand(name,arg,msg)

  ts = Google::Protobuf::Timestamp.new
  p ts
  ts.from_time(Time.now)
  cdata = Api::Content.new(entity: msg)

  req = Api::Demand.new(id: generateIntID(),
                        sender_id: $clientID,
                        target_id: 0,
                        channel_type: 7,
                        demand_name: name,
                        ts: ts,
                        arg_json: arg,
                        mbus_id: 0,
                        cdata: cdata)
  
#  GRPC.logger.info("NotifyDemand Call #{req.inspect}")
  resp = $sxstub.notify_demand(req)
  GRPC.logger.info("NotifyDemand Response #{resp.inspect}")
  
end

def do_notifySupply(name,arg,msg)

  ts = Google::Protobuf::Timestamp.new

#  $log.puts("Do notify Supply:"+ts.to_s+":"+$clientID.to_s)

  ts.from_time(Time.now)
  cdata = Api::Content.new(entity: msg)

  req = Api::Supply.new(id: generateIntID(),
                        sender_id: $clientID,
                        target_id: 0,
                        channel_type: 7,   # for fluentd
                        supply_name: name,
                        ts: ts,
                        arg_json: arg,
                        mbus_id: 0,
                        cdata: cdata)
  
#  GRPC.logger.info("NotifySupply Call #{req.inspect}")
  resp = $sxstub.notify_supply(req)
  GRPC.logger.info("NotifySupply Response #{resp.inspect}")
  
end


def keepAlive()
  while $nodeInfo['secret'] != 0 do
    sleep( $nodeInfo['keepalive_duration'])
    $updateCount += 1
    req = Nodeapi::NodeUpdate.new(node_id: $nodeInfo['node_id'],
                                  secret: $nodeInfo['secret'],
                                  update_count: $updateCount,
                                  node_status: 0,
                                  node_arg: $status)
    resp = $nodestub.keep_alive(req)
    GRPC.logger.info("Response for #{$nodeInfo['node_id']}, #{resp.inspect}")
    
  end
end



def startKeepAlive()
  $threads << Thread.new { keepAlive() }
end



def do_registerNode(stub,name)
  GRPC.logger.info("Register Node")
  req = Nodeapi::NodeInfo.new(node_name: name,
                                node_type: Nodeapi::NodeType::PROVIDER,
                                server_info: "",
                                node_pbase_version: "0.1.2",
                                with_node_id: -1,
                                cluster_id: 0,
                                area_id: "Default",
                                channelTypes: [7]) #fluentd
  resp = stub.register_node(req)
  GRPC.logger.info("RN:Answer: #{resp.inspect}")

  return resp
end

def generateIntID()
  return $idgen.next_id
end

def registerServ(nodesv,name)
  # connect to nodeserv
  $nodestub = Nodeapi::Node::Stub.new(nodesv, :this_channel_is_insecure, timeout: INFINITE_FUTURE)
  GRPC.logger.info(".. connecting insecurely on nodeserv #{nodesv}")
  $nodeInfo =  do_registerNode($nodestub,name)  # got server info
  
  service_epoch = Time.new(2010, 11, 4, 1, 42, 54).strftime('%s%L').to_i
  # may Twitter epoch of snowflake,
  # 1288834974657
  
  $idgen = AnyFlake.new(service_epoch, $nodeInfo.node_id)
  $clientID = generateIntID()
  
  GRPC.logger.info(".. connecting insecurely on synerex server to #{$nodeInfo['server_info']}")
  $sxstub = Api::Synerex::Stub.new($nodeInfo.server_info, :this_channel_is_insecure, timeout: INFINITE_FUTURE)

  startKeepAlive
end


  

