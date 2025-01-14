# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/azure_event_hubs"

java_import com.microsoft.azure.eventprocessorhost.EventProcessorHost
java_import com.microsoft.azure.eventprocessorhost.InMemoryCheckpointManager
java_import com.microsoft.azure.eventprocessorhost.InMemoryLeaseManager

describe LogStash::Inputs::AzureEventHubs do


  subject(:input) {LogStash::Plugin.lookup("input", "azure_event_hubs").new(config)}

  describe "Event Hubs Configuration -> " do
    shared_examples "an exploded Event Hub config" do |x|
      it "it explodes #{x} event hub(s) correctly" do
        exploded_config = input.event_hubs_exploded
        x.times do |i|
          expect(exploded_config[i]['event_hubs'].size).to be == 1 #always 1 in the exploded form
          expect(exploded_config[i]['event_hubs'][0]).to eql('event_hub_name' + i.to_s)
          expect(exploded_config[i]['event_hub_connections'][0].value).to start_with('Endpoint=sb://...')
          expect(exploded_config[i]['storage_connection'].value).to eql('DefaultEndpointsProtocol=https;AccountName=...')
          expect(exploded_config[i]['threads']).to be == 9
          expect(exploded_config[i]['codec'].class.to_s).to eq("LogStash::Codecs::Plain")
          expect(exploded_config[i]['consumer_group']).to eql('cg')
          expect(exploded_config[i]['max_batch_size']).to be == 20
          expect(exploded_config[i]['prefetch_count']).to be == 30
          expect(exploded_config[i]['receive_timeout']).to be == 40
          expect(exploded_config[i]['initial_position']).to eql('look_back')
          expect(exploded_config[i]['initial_position_look_back']).to be == 50
          expect(exploded_config[i]['checkpoint_interval']).to be == 60
          expect(exploded_config[i]['decorate_events']).to be_truthy
        end
      end
    end

    describe "Basic Config" do
      before do
        input.register
      end
      let(:config) do
        {
            'event_hub_connections' => ['Endpoint=sb://...;EntityPath=event_hub_name0', 'Endpoint=sb://...;EntityPath=event_hub_name1'],
            'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...',
            'threads' => 9,
            'codec' => 'plain',
            'consumer_group' => 'cg',
            'max_batch_size' => 20,
            'prefetch_count' => 30,
            'receive_timeout' => 40,
            'initial_position' => 'look_back',
            'initial_position_look_back' => 50,
            'checkpoint_interval' => 60,
            'decorate_events' => true
        }
      end
      it_behaves_like "an exploded Event Hub config", 2

      it "it runs the Event Processor Host" do
        mock_queue = double("queue")
        mock_host = double("event_processor_host")
        mock_host_context = double("host_context")
        completable_future = java.util.concurrent.CompletableFuture.new
        #simulate work being done before completing the future
        Thread.new do
          sleep 2
          completable_future.complete("")
        end

        # rspec has issues with counters and concurrent code, so use threadsafe counters instead
        host_counter = java.util.concurrent.atomic.AtomicInteger.new
        register_counter = java.util.concurrent.atomic.AtomicInteger.new
        unregister_counter = java.util.concurrent.atomic.AtomicInteger.new
        assertion_count = java.util.concurrent.atomic.AtomicInteger.new

        allow(mock_host).to receive(:getHostContext) {mock_host_context}
        allow(mock_host_context).to receive(:getEventHubPath) {"foo"}

        expect(mock_host).to receive(:registerEventProcessorFactory).at_most(2).times {
          register_counter.incrementAndGet
          completable_future
        }
        expect(mock_host).to receive(:unregisterEventProcessor).at_most(2).times {
          unregister_counter.incrementAndGet
          completable_future
        }

        build_step_mock = double("final build step")
        expect(build_step_mock).to receive(:build).at_most(2).times.and_return(mock_host)

        executor_step = double("executor step")
        expect(executor_step).to receive(:setExecutor).at_most(2).times.and_return(build_step_mock)

        mock_connection_string = double("connection string")
        expect(mock_connection_string).to receive(:useEventHubConnectionString).at_most(2).times {|event_hub_connection|
          case event_hub_connection
          when /.*event_hub_name0$/
            expect(event_hub_connection).to eql(config['event_hub_connections'][0])
          when /.*event_hub_name1$/
            expect(event_hub_connection).to eql(config['event_hub_connections'][1])
          end
          executor_step
        }

        mock_builder = double("storage and lease managers")
        expect(mock_builder).to receive(:useAzureStorageCheckpointLeaseManager).at_most(2).times {|storage_connection_str, storage_container, storage_blob_prefix|
          case storage_container
          when 'event_hub_name0'
            assertion_count.incrementAndGet
          when 'event_hub_name1'
            assertion_count.incrementAndGet
          end
          mock_connection_string
        }

        expect(EventProcessorHost::EventProcessorHostBuilder).to receive(:newBuilder).at_most(2).times {|host_name, consumer_group|
          expect(host_name).to start_with('logstash')
          host_counter.incrementAndGet
          mock_builder
        }
        # signal the stop first since the run method blocks until stop is called.
        input.do_stop
        input.run(mock_queue)
        expect(host_counter.get).to be == 2
        expect(register_counter.get).to be == 2
        expect(unregister_counter.get).to be == 2
        expect(assertion_count.get).to be == 2
      end

      describe "single connection, no array syntax" do
        let(:config) do
          {
              'event_hub_connections' => 'Endpoint=sb://logstash/;SharedAccessKeyName=activity-log-readonly;SharedAccessKey=something;EntityPath=event_hub1'
          }
        end
        it "it can handle a single connection without the array notation" do
          expect {input}.to_not raise_error
          exploded_config = input.event_hubs_exploded
          expect(exploded_config.size).to be == 1
          expect(exploded_config[0]['event_hub_connections'][0].value).to eql('Endpoint=sb://logstash/;SharedAccessKeyName=activity-log-readonly;SharedAccessKey=something;EntityPath=event_hub1')
        end
      end
    end

    describe "Advanced Config" do
      before do
        input.register
      end
      let(:config) do
        {
            'config_mode' => 'advanced',
            'event_hubs' => [
                {'event_hub_name0' => {
                    'event_hub_connection' => 'Endpoint=sb://...',
                    'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...',
                    'codec' => 'plain',
                    'consumer_group' => 'cg',
                    'max_batch_size' => 20,
                    'prefetch_count' => 30,
                    'receive_timeout' => 40,
                    'initial_position' => 'look_back',
                    'initial_position_look_back' => 50,
                    'checkpoint_interval' => 60,
                    'decorate_events' => true}},
                {'event_hub_name1' => {
                    'event_hub_connection' => '1Endpoint=sb://...',
                    'storage_connection' => '1DefaultEndpointsProtocol=https;AccountName=...',
                    'codec' => 'json',
                    'consumer_group' => 'cg1',
                    'receive_timeout' => 41,
                    'initial_position' => 'end',
                    'checkpoint_interval' => 61,
                    'decorate_events' => false,
                    'storage_container' => 'alt_container'}},
                # same named event hub with different configuration is allowed
                {'event_hub_name0' => {
                    'event_hub_connection' => 'Endpoint=sb://...',
                    'consumer_group' => 'ls'}}
            ],
            'codec' => 'plain',
            'consumer_group' => 'default_consumer_group',
            'max_batch_size' => 21,
            'threads' => 9
        }
      end
      it_behaves_like "an exploded Event Hub config", 1
      it "it explodes the 2cnd advanced config event hub correctly" do
        exploded_config = input.event_hubs_exploded
        expect(exploded_config[1]['event_hubs'].size).to be == 1 #always 1 in the exploded form
        expect(exploded_config[1]['event_hubs'][0]).to eql('event_hub_name1')
        expect(exploded_config[1]['event_hub_connections'][0].value).to eql('1Endpoint=sb://...')
        expect(exploded_config[1]['storage_connection'].value).to eql('1DefaultEndpointsProtocol=https;AccountName=...')
        expect(exploded_config[1]['threads']).to be == 9
        expect(exploded_config[1]['codec'].class.to_s).to eq("LogStash::Codecs::JSON") # different between configs
        expect(exploded_config[1]['consumer_group']).to eql('cg1') # override global
        expect(exploded_config[1]['max_batch_size']).to be == 21 # filled from global
        expect(exploded_config[1]['prefetch_count']).to be == 300 # default
        expect(exploded_config[1]['receive_timeout']).to be == 41
        expect(exploded_config[1]['initial_position']).to eql('end')
        expect(exploded_config[1]['initial_position_look_back']).to be == 86400 # default
        expect(exploded_config[1]['checkpoint_interval']).to be == 61
        expect(exploded_config[1]['decorate_events']).to be_falsy
        expect(exploded_config[1]['storage_container']).to eq('alt_container')
      end

      it "it runs the Event Processor Host" do
        mock_queue = double("queue")
        mock_host = double("event_processor_host")
        mock_host_context = double("host_context")
        completable_future = java.util.concurrent.CompletableFuture.new
        #simulate work being done before completing the future
        Thread.new do
          sleep 2
          completable_future.complete("")
        end

        # rspec has issues with counters and concurrent code, so use threadsafe counters instead
        host_counter = java.util.concurrent.atomic.AtomicInteger.new
        register_counter = java.util.concurrent.atomic.AtomicInteger.new
        unregister_counter = java.util.concurrent.atomic.AtomicInteger.new
        assertion_count = java.util.concurrent.atomic.AtomicInteger.new
        allow(input).to receive(:get_host_context) {mock_host_context}
        allow_any_instance_of(InMemoryLeaseManager).to receive(:java_send)
        allow_any_instance_of(InMemoryCheckpointManager).to receive(:java_send)

        allow(mock_host_context).to receive(:getEventHubPath) {"foo"}

        expect(mock_host).to receive(:registerEventProcessorFactory).at_most(3).times {
          register_counter.incrementAndGet
          completable_future
        }
        expect(mock_host).to receive(:unregisterEventProcessor).at_most(3).times {
          unregister_counter.incrementAndGet
          completable_future
        }

        build_step_mock = double("final build step")
        expect(build_step_mock).to receive(:build).at_most(3).times.and_return(mock_host)

        executor_step = double("executor step")
        expect(executor_step).to receive(:setExecutor).at_most(3).times.and_return(build_step_mock)

        mock_connection_string = double("connection string")
        expect(mock_connection_string).to receive(:useEventHubConnectionString).at_most(3).times {|event_hub_connection|
          case event_hub_connection
          when /.*event_hub_name0$/
            expect(event_hub_connection).to eql(config['event_hubs'][0]['event_hub_name0']['event_hub_connections'][0].value)
          when /.*event_hub_name1$/
            expect(event_hub_connection).to eql(config['event_hubs'][1]['event_hub_name1']['event_hub_connections'][0].value)
          end
          executor_step
        }

        managers_mock = double("checkpoint and lease managers")
        expect(managers_mock).to receive(:useUserCheckpointAndLeaseManagers).at_most(3).times {|checkpoint_mngr, lease_mngr|
          assertion_count.incrementAndGet
          mock_connection_string
        }
        expect(managers_mock).to receive(:useAzureStorageCheckpointLeaseManager).at_most(3).times {|storage_connection_str, storage_container, storage_blob_prefix|
          case storage_container
          when 'event_hub_name0'
            assertion_count.incrementAndGet
          when 'alt_container'
            assertion_count.incrementAndGet
          end
          mock_connection_string
        }

        expect(EventProcessorHost::EventProcessorHostBuilder).to receive(:newBuilder).at_most(3).times {|host_name, consumer_group|
          expect(host_name).to start_with('logstash')
          host_counter.incrementAndGet
          managers_mock
        }

        # signal the stop first since the run method blocks until stop is called.
        input.do_stop
        input.run(mock_queue)
        expect(host_counter.get).to be == 3
        expect(register_counter.get).to be == 3
        expect(unregister_counter.get).to be == 3
        expect(assertion_count.get).to be == 3
      end

      it "can create an in memory EPH" do
        #event_hub, event_hub_name, scheduled_executor_service
        exploded_config = input.event_hubs_exploded
        # During build step Azure libraries does a syntax check of EventHub connection string, so needs to be a pseudo real
        exploded_config[0]['event_hub_connections'] = [::LogStash::Util::Password.new("Endpoint=sb://logstash.windows.net/;SharedAccessKeyName=activity-log-read-only;SharedAccessKey=blabla;EntityPath=ops-logs")]
        input.create_in_memory_event_processor_host(exploded_config[0], exploded_config[0]['event_hubs'].first, nil)
      end
    end

    describe "Bad Basic Config" do
      describe "Offset overwriting" do
        let(:config) do
          {
              'event_hub_connections' => ['Endpoint=sb://...;EntityPath=event_hub_name0', 'Endpoint=sb://...;EntityPath=event_hub_name0'],
              'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...'
          }
        end
        it "it errors when using same consumer group and storage container" do
          expect {input}.to raise_error(/overwriting offsets/)
        end
      end

      describe "Invalid Event Hub name" do
        let(:config) do
          {
              'event_hub_connections' => ['Endpoint=sb://logstash/;SharedAccessKeyName=activity-log-readonly;SharedAccessKey=thisshouldnotbepartofthelogmessage'],
              'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...'
          }
        end
        it "it errors when using same consumer group and storage container" do
          expect {input}.to raise_error(/that the connection string contains the EntityPath/)
          expect {input}.to raise_error(/redacted/)
          expect {input}.to raise_error(/^((?!thisshouldnotbepartofthelogmessage).)*$/)
        end
      end
    end

    describe "Bad Advanced Config" do
      describe "Offset overwritting" do
        let(:config) do
          {
              'config_mode' => 'advanced',
              'event_hubs' => [
                  {'event_hub_name0' => {
                      'event_hub_connection' => 'Endpoint=sb://...',
                  }},
                  {'event_hub_name1' => {
                      'event_hub_connection' => '1Endpoint=sb://...',
                  }}
              ],

              'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...',
              'consumer_group' => 'default_consumer_group',
              'storage_container' => 'logstash'
          }
        end
        it "it errors when using same consumer group and storage container" do
          expect {input}.to raise_error(/overwriting offsets/)
        end
      end
    end
  end
end
