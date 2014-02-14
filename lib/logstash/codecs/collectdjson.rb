# encoding: utf-8
require "logstash/codecs/base"
require "json"
require "time"

# This codec will take JSON returned by collectd (e.g. sent via the write amqp
# plugin) and create a logstash event from it

class LogStash::Codecs::CollectdJSON < LogStash::Codecs::Base
  config_name "collectdjson"
  milestone 1

  # Should we remove the 'interval' field?
  config :prune_intervals, :validate => :boolean, :default => true

  # How many worker threads should we use?
  config :workers, :validate => :number, :default => 5

  # How much backlog (in items) do we allow before blocking?
  config :queue_size, :validate => :number, :default => 2000

  public
  def register
    @to_delete = %w(time type interval dsnames values dstypes)
    @work = SizedQueue.new(@queue_size)

    @workers.times do
      Thread.new { codecworker }
    end
  end

  private
  def codecworker
    LogStash::Util::set_thread_name("|collectdjson")
    begin
      while true
        line = @work.pop
        # The collectd JSON is 'wrapped' inside an array
        collectd = JSON.parse(line[1..-2])
        ls_event = {}

        # Parse some special field in the data
        ls_event['@timestamp'] = Time.at(collectd['time'])
        ls_event['collectd_type'] = collectd['type']
        ls_event['interval'] = collectd['interval'] if !@prune_intervals
        collectd['dsnames'].each_with_index do |name,i|
          ls_event[name] = collectd['values'][i]
        end

        # delete all fields from collectd already added to the event and
        # clean up the collectd data
        collectd.each do |key,value|
          collectd.delete(key) if @to_delete.include?(key) or value.empty?
        end

        # We merge ls_event with collectd because collectd might contain extra
        # data we care about
        yield(LogStash::Event.new(ls_event.merge(collectd)))
      end
    rescue => e
      @logger.error("Exception in codec worker", "exception" => e, "backtrace" => e.backtrace)
    end
  end

  public
  def decode(line)
    @work.push(line)
  end
end
