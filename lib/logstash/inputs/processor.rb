require "logstash/util/loggable"
module LogStash
  module Inputs
    module Azure
      class Processor
        include LogStash::Util::Loggable
        include com.microsoft.azure.eventprocessorhost.IEventProcessor

        def initialize(queue, codec, checkpoint_interval, decorator, meta_data)
          @queue = queue
          @codec = codec
          @checkpoint_interval = checkpoint_interval
          @last_checkpoint = Time.now.to_i
          @decorator = decorator
          @meta_data = meta_data
          @batch_counter = 0
          @logger = self.logger

        end

        def onOpen(context)
          @logger.info("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s} is opening.")
        end

        def onClose(context, reason)
          @logger.info("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s} is closing. (reason=#{reason.to_s})")
        end

        def onEvents(context, batch)
          @logger.debug("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s} is processing a batch of size #{batch.size}.") if @logger.debug?
          last_payload = nil
          batch_size = 0
          @batch_counter += 1
          batch.each do |payload|
            bytes = payload.getBytes
            batch_size += bytes.size
            @logger.trace("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s}, Offset: #{payload.getSystemProperties.getOffset.to_s},"+
                              " Sequence: #{payload.getSystemProperties.getSequenceNumber.to_s}, Size: #{bytes.size}") if @logger.trace?

            @codec.decode(bytes.to_a.pack('C*')) do |event|

              @decorator.call(event)
              if @meta_data
                event.set("[@metadata][medfar_logs][name]", context.getEventHubPath)
                event.set("[@metadata][medfar_logs][consumer_group]", context.getConsumerGroupName)
                event.set("[@metadata][medfar_logs][processor_host]", context.getOwner)
                event.set("[@metadata][medfar_logs][partition]", context.getPartitionId)
                event.set("[@metadata][medfar_logs][offset]", payload.getSystemProperties.getOffset)
                event.set("[@metadata][medfar_logs][sequence]", payload.getSystemProperties.getSequenceNumber)
                event.set("[@metadata][medfar_logs][timestamp]",payload.getSystemProperties.getEnqueuedTime.getEpochSecond)
                event.set("[@metadata][medfar_logs][event_size]", bytes.size)
              end
              @queue << event
              if @checkpoint_interval > 0
                now = Time.now.to_i
                since_last_check_point = now - @last_checkpoint
                if since_last_check_point >= 300
                  context.checkpoint(payload).get
                  @logger.debug("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s} finished checkpointing.") if @logger.debug?
                  @last_checkpoint = now
                end
              end
            end
            last_payload = payload
          end

          @codec.flush
          #always create checkpoint at end of onEvents in case of sparse events
          #if ((@batch_counter % 5) == 0)
          #  context.checkpoint(last_payload).get if last_payload
          #end
          @logger.debug("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s} finished processing a batch of #{batch_size} bytes.") if @logger.debug?
        end

        def onError(context, error)
          @logger.error("Event Hub: #{context.getEventHubPath.to_s}, Partition: #{context.getPartitionId.to_s} experienced an error #{error.to_s})")
        end
      end
    end
  end
end