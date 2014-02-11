# encoding: utf-8
require "logstash/codecs/base"
require "json"

# This codec will take JSON returned by collectd (e.g. sent via the write amqp
# plugin) and create a logstash event from it

class LogStash::Codecs::CollectdJSON < LogStash::Codecs::Base
  config_name "collectdjson"
  milestone 1

  # Should we remove the 'interval' field?
  config :prune_intervals, :validate => :boolean, :default => true

  public
  def decode(line)
    Thread.new do
      # The collectd JSON is 'wrapped' inside an array
      events = JSON.parse(line[1..-2])
      counter = 0
      events['dstypes'].each do
        to_yield = {}
        events.each do |key,value|
          case key
          when 'type'
            to_yield['collectd_type'] = value
          when 'values'
            to_yield['value'] = value[counter]
          when 'time'
            to_yield['time'] = (value * 1000).to_i
          when 'dstypes', 'dsnames'
            # Nothing, don't add these to the event
          else
            if key != 'interval' || (key == 'interval' && !@prune_intervals)
              if value.is_a?(Array)
                to_yield[key] = value[counter] if !value[counter].empty?
              else
                to_yield[key] = value if !value.empty?
              end
            end
          end
        end
        counter += 1
        yield(LogStash::Event.new(to_yield))
      end
    end
  end
end
