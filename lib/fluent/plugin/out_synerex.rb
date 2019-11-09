#
# Copyright 2019- Synerex Project (Nobuo Kawaguchi)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'grpc'
require 'fluent/plugin/output'
require_relative 'provider-util'
require_relative 'fluentd_pb'
require 'google/protobuf/well_known_types'


module Fluent
  module Plugin
    class SynerexOutput < Fluent::Plugin::Output
      Fluent::Plugin.register_output("synerex", self)

      desc "Synerex NodeServer address"
      config_param :server, :string, default: "127.0.0.1"
      desc "Synerex NodeServer port"
      config_param :port, :string, default: "9990"

      config_section :buffer do
        config_set_default :chunk_keys, ['tag']
        config_set_default :flush_at_shutdown, true
        config_set_default :chunk_limit_size, 10*1024
      end

#      def prefer_buffered_processing
#        false
#      end

      def multi_workers_ready?
        true
      end

      def configure(conf)
        GRPC.logger.info("Configure start ")
        super

#        @lfile = File.open('/tmp/synerex_out.txt',"w+")

      end


      # called befor start
      def start
        super
        nodeserv = @server+":"+@port
#        @lfile.puts("NodeServer:"+nodeserv)        
        registerServ(nodeserv,"Fluentd-Provider")
      end

      # called befor stop
      def shutdown
        super
      end

      attr_accessor :formatter

      def process(tag, es)
#        @lfile.puts ( "from process:"+tag.class.to_s+":"+tag.to_s)
        es.each do |time, record|
          process_record(tag, time, record)
        end
      end

      def write(chunk)
#        @lfile.puts("from chunk["+chunk.metadata.class.to_s+"]"+chunk.metadata.to_s)
        tag = chunk.metadata.tag
        chunk.each do |time, record|
          process_record(tag, time, record)
        end
      end

      def process_record(tag, time, record)
        response = send_message(tag, time, record)
      end

      def send_message(tag, time, record)
#        @lfile.puts ( "tag   :"+tag.class.to_s+":"+tag.to_s)
#        @lfile.puts ("time  :"+time.class.to_s+":"+time.to_s)
#        @lfile.puts ("record:"+record.class.to_s+":"+record.to_s)

        ts = Google::Protobuf::Timestamp.new(seconds: time)
        
        rec = Proto::Fluentd::FluentdRecord.new(tag: tag.to_s, time:ts, record: record.to_s)
        code = Proto::Fluentd::FluentdRecord.encode(rec)
        
        do_notifySupply("Fluentd","tag",code)
        return nil
      end
    end

  end
end
