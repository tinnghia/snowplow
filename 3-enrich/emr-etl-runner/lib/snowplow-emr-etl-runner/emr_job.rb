# Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Alex Dean (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'set'
require 'elasticity'
require 'awrence'
require 'json'
require 'base64'
require 'contracts'

# Global variable used to decide whether to patch Elasticity's AwsRequestV4 payload with Configurations
# This is only necessary if we are loading Thrift with AMI >= 4.0.0
$patch_thrift_configuration = false

# Monkey patched to support Configurations
module Elasticity
  class AwsRequestV4
    def payload
      if $patch_thrift_configuration
        @ruby_service_hash["Configurations"] = [{
          "Classification" => "core-site",
          "Properties" => {
            "io.file.buffer.size" => "65536"
          }
        },
        {
          "Classification" => "mapred-site",
          "Properties" => {
            "mapreduce.user.classpath.first" => "true"
          }
        }]
      end
      AwsUtils.convert_ruby_to_aws_v4(@ruby_service_hash).to_json
    end
  end
end

# Ruby class to execute Snowplow's Hive jobs against Amazon EMR
# using Elasticity (https://github.com/rslifka/elasticity).
module Snowplow
  module EmrEtlRunner
    class EmrJob

      include Contracts

      # Constants
      JAVA_PACKAGE = "com.snowplowanalytics.snowplow"
      PARTFILE_REGEXP = ".*part-.*"
      BOOTSTRAP_FAILURE_INDICATOR = /BOOTSTRAP_FAILURE|bootstrap action|Master instance startup failed/
      NO_DATA_FAILURE_INDICATOR = /check-data-to-process/
      DIR_NOT_EMPTY_FAILURE_INDICATOR = /check-dir-empty/
      STANDARD_HOSTED_ASSETS = "s3://snowplow-hosted-assets"
      ENRICH_STEP_INPUT = 'hdfs:///local/snowplow/raw-events/'
      ENRICH_STEP_OUTPUT = 'hdfs:///local/snowplow/enriched-events/'
      SHRED_STEP_OUTPUT = 'hdfs:///local/snowplow/shredded-events/'

      # Need to understand the status of all our jobflow steps
      @@running_states = Set.new(%w(WAITING RUNNING PENDING SHUTTING_DOWN))
      @@failed_states  = Set.new(%w(FAILED CANCELLED))

      include Monitoring::Logging
      include Snowplow::EmrEtlRunner::Utils

      # Initializes our wrapper for the Amazon EMR client.
      Contract Bool, Bool, Bool, Bool, Bool, ConfigHash, ArrayOf[String], String => EmrJob
      def initialize(debug, staging, enrich, shred, es, config, enrichments_array, resolver)

        logger.debug "Initializing EMR jobflow"

        # Configuration
        custom_assets_bucket =
          get_hosted_assets_bucket(STANDARD_HOSTED_ASSETS, config[:aws][:s3][:buckets][:assets], config[:aws][:emr][:region])
        standard_assets_bucket =
          get_hosted_assets_bucket(STANDARD_HOSTED_ASSETS, STANDARD_HOSTED_ASSETS, config[:aws][:emr][:region])
        assets = get_assets(
          custom_assets_bucket,
          config[:enrich][:versions][:hadoop_enrich],
          config[:enrich][:versions][:hadoop_shred],
          config[:enrich][:versions][:hadoop_elasticsearch])

        collector_format = config[:collectors][:format]
        run_tstamp = Time.new
        run_id = run_tstamp.strftime("%Y-%m-%d-%H-%M-%S")
        @run_id = run_id
        etl_tstamp = (run_tstamp.to_f * 1000).to_i.to_s
        output_codec = output_codec_from_compression_format(config[:enrich][:output_compression])

        # Configure Elasticity with your AWS credentials
        Elasticity.configure do |c|
          c.access_key = config[:aws][:access_key_id]
          c.secret_key = config[:aws][:secret_access_key]
        end

        # Create a job flow
        @jobflow = Elasticity::JobFlow.new

        # Configure
        @jobflow.name                 = config[:enrich][:job_name]

        if config[:aws][:emr][:ami_version] =~ /^[1-3].*/
          @legacy = true
          @jobflow.ami_version = config[:aws][:emr][:ami_version]
        else
          @legacy = false
          @jobflow.release_label = "emr-#{config[:aws][:emr][:ami_version]}"
        end

        @jobflow.tags                 = config[:monitoring][:tags]
        @jobflow.ec2_key_name         = config[:aws][:emr][:ec2_key_name]

        @jobflow.region               = config[:aws][:emr][:region]
        @jobflow.job_flow_role        = config[:aws][:emr][:jobflow_role] # Note job_flow vs jobflow
        @jobflow.service_role         = config[:aws][:emr][:service_role]
        @jobflow.placement            = config[:aws][:emr][:placement]
        @jobflow.additional_info      = config[:aws][:emr][:additional_info]
        unless config[:aws][:emr][:ec2_subnet_id].nil? # Nils placement so do last and conditionally
          @jobflow.ec2_subnet_id      = config[:aws][:emr][:ec2_subnet_id]
        end

        @jobflow.log_uri              = config[:aws][:s3][:buckets][:log]
        @jobflow.enable_debugging     = debug
        @jobflow.visible_to_all_users = true

        @jobflow.instance_count       = config[:aws][:emr][:jobflow][:core_instance_count] + 1 # +1 for the master instance
        @jobflow.master_instance_type = config[:aws][:emr][:jobflow][:master_instance_type]
        @jobflow.slave_instance_type  = config[:aws][:emr][:jobflow][:core_instance_type]

        s3_endpoint = get_s3_endpoint(config[:aws][:s3][:region])
        csbr = config[:aws][:s3][:buckets][:raw]
        csbe = config[:aws][:s3][:buckets][:enriched]
        csbs = config[:aws][:s3][:buckets][:shredded]

        # EBS
        unless config[:aws][:emr][:jobflow][:core_instance_ebs].nil?
          ebs_bdc = Elasticity::EbsBlockDeviceConfig.new

          ebs_bdc.volume_type          = config[:aws][:emr][:jobflow][:core_instance_ebs][:volume_type]
          ebs_bdc.size_in_gb           = config[:aws][:emr][:jobflow][:core_instance_ebs][:volume_size]
          ebs_bdc.volumes_per_instance = 1
          if config[:aws][:emr][:jobflow][:core_instance_ebs][:volume_type] == "io1"
            ebs_bdc.iops = config[:aws][:emr][:jobflow][:core_instance_ebs][:volume_iops]
          end

          ebs_c = Elasticity::EbsConfiguration.new
          ebs_c.add_ebs_block_device_config(ebs_bdc)
          ebs_c.ebs_optimized = true

          unless config[:aws][:emr][:jobflow][:core_instance_ebs][:ebs_optimized].nil?
            ebs_c.ebs_optimized = config[:aws][:emr][:jobflow][:core_instance_ebs][:ebs_optimized]
          end

          @jobflow.set_core_ebs_configuration(ebs_c)
        end

        # Patching of the hadoop config files for thrift
        if collector_format == 'thrift'
          if @legacy
            [
              Elasticity::HadoopBootstrapAction.new('-c', 'io.file.buffer.size=65536'),
              Elasticity::HadoopBootstrapAction.new('-m', 'mapreduce.user.classpath.first=true')
            ].each do |action|
              @jobflow.add_bootstrap_action(action)
            end
          else
            $patch_thrift_configuration = true
          end
        end

        # Add custom bootstrap actions
        bootstrap_actions = config[:aws][:emr][:bootstrap]
        unless bootstrap_actions.nil?
          bootstrap_actions.each do |bootstrap_action|
            @jobflow.add_bootstrap_action(Elasticity::BootstrapAction.new(bootstrap_action))
          end
        end

        # Prepare a bootstrap action based on the AMI version
        bootstrap_script_location = if @legacy
          "#{standard_assets_bucket}common/emr/snowplow-ami3-bootstrap-0.1.0.sh"
        else
          "#{standard_assets_bucket}common/emr/snowplow-ami4-bootstrap-0.2.0.sh"
        end
        cc_version = get_cc_version(config[:enrich][:versions][:hadoop_enrich])
        @jobflow.add_bootstrap_action(Elasticity::BootstrapAction.new(bootstrap_script_location, cc_version))

        # Install and launch HBase
        hbase = config[:aws][:emr][:software][:hbase]
        unless not hbase
          install_hbase_action = Elasticity::BootstrapAction.new("s3://#{config[:aws][:emr][:region]}.elasticmapreduce/bootstrap-actions/setup-hbase")
          @jobflow.add_bootstrap_action(install_hbase_action)

          start_hbase_step = Elasticity::CustomJarStep.new("/home/hadoop/lib/hbase-#{hbase}.jar")
          start_hbase_step.name = "Start HBase #{hbase}"
          start_hbase_step.arguments = [ 'emr.hbase.backup.Main', '--start-master' ]
          @jobflow.add_step(start_hbase_step)
        end

        # Install Lingual
        lingual = config[:aws][:emr][:software][:lingual]
        unless not lingual
          install_lingual_action = Elasticity::BootstrapAction.new("s3://files.concurrentinc.com/lingual/#{lingual}/lingual-client/install-lingual-client.sh")
          @jobflow.add_bootstrap_action(install_lingual_action)
        end

        # Now let's add our task group if required
        tic = config[:aws][:emr][:jobflow][:task_instance_count]
        if tic > 0
          instance_group = Elasticity::InstanceGroup.new.tap { |ig|
            ig.count = tic
            ig.type  = config[:aws][:emr][:jobflow][:task_instance_type]

            tib = config[:aws][:emr][:jobflow][:task_instance_bid]
            if tib.nil?
              ig.set_on_demand_instances
            else
              ig.set_spot_instances(tib)
            end
          }

          @jobflow.set_task_instance_group(instance_group)
        end

        # staging
        if staging
          # Sanity check before staging: processing should be empty
          @jobflow.add_step(get_check_dir_empty_step(csbr[:processing], standard_assets_bucket))

          src_pattern = collector_format == 'clj-tomcat' ? '.*localhost\_access\_log.*\.txt.*' : '.+'
          csbr[:in].map { |l|
            staging_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
            staging_step.arguments = [
              "--src", l,
              "--dest", csbr[:processing],
              "--s3Endpoint", s3_endpoint,
              "--srcPattern", src_pattern,
              "--deleteOnSuccess"
            ]
            staging_step.name << ": Staging of #{l}"
            @jobflow.add_step(staging_step)
          }

          # Sanity check steps after staging:
          # - processing shoulnd't be empty
          # - enriched and shredded should be empty
          @jobflow.add_step(get_check_data_to_process_step(csbr[:processing], standard_assets_bucket))
          [ enrich ? csbe[:good] : '', shred ? csbs[:good] : '' ].reject { |s| s == '' }.each do |l|
            @jobflow.add_step(get_check_dir_empty_step(l, standard_assets_bucket))
          end
        end

        enrich_final_output = if enrich
          partition_by_run(csbe[:good], run_id)
        else
          csbe[:good] # Doesn't make sense to partition if enrich has already been done
        end

        if enrich

          raw_input = csbr[:processing]

          # for ndjson/urbanairship we can group by everything, just aim for the target size
          group_by = is_ua_ndjson(collector_format) ? ".*\/(\w+)\/.*" : ".*([0-9]+-[0-9]+-[0-9]+)-[0-9]+.*"

          # Create the Hadoop MR step for the file crushing
          compact_to_hdfs_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          compact_to_hdfs_step.arguments = [
            "--src"         , raw_input,
            "--dest"        , ENRICH_STEP_INPUT,
            "--s3Endpoint"  , s3_endpoint
          ] + [
            "--groupBy"     , group_by,
            "--targetSize"  , "128",
            "--outputCodec" , "lzo"
          ].select { |el|
            is_cloudfront_log(collector_format) || is_ua_ndjson(collector_format)
          }
          compact_to_hdfs_step.name << ": Raw S3 -> HDFS"

          # Add to our jobflow
          @jobflow.add_step(compact_to_hdfs_step)

          # 2. Enrichment
          enrich_step = build_scalding_step(
            "Enrich Raw Events",
            assets[:enrich],
            "enrich.hadoop.EtlJob",
            { :in     => glob_path(ENRICH_STEP_INPUT),
              :good   => ENRICH_STEP_OUTPUT,
              :bad    => partition_by_run(csbe[:bad],    run_id),
              :errors => partition_by_run(csbe[:errors], run_id, config[:enrich][:continue_on_unexpected_error])
            },
            { :input_format     => collector_format,
              :etl_tstamp       => etl_tstamp,
              :iglu_config      => Base64.strict_encode64(resolver),
              :enrichments      => build_enrichments_json(enrichments_array)
            }
          )

          # Late check whether our enrichment directory is empty. We do an early check too
          @jobflow.add_step(get_check_dir_empty_step(csbe[:good], standard_assets_bucket))
          @jobflow.add_step(enrich_step)

          # We need to copy our enriched events from HDFS back to S3
          copy_to_s3_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          copy_to_s3_step.arguments = [
            "--src"        , ENRICH_STEP_OUTPUT,
            "--dest"       , enrich_final_output,
            "--srcPattern" , PARTFILE_REGEXP,
            "--s3Endpoint" , s3_endpoint
          ] + output_codec
          copy_to_s3_step.name << ": Enriched HDFS -> S3"
          @jobflow.add_step(copy_to_s3_step)

          copy_success_file_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          copy_success_file_step.arguments = [
            "--src"        , ENRICH_STEP_OUTPUT,
            "--dest"       , enrich_final_output,
            "--srcPattern" , ".*_SUCCESS",
            "--s3Endpoint" , s3_endpoint
          ]
          copy_success_file_step.name << ": Enriched HDFS _SUCCESS -> S3"
          @jobflow.add_step(copy_success_file_step)
        end

        if shred

          # 3. Shredding
          shred_final_output = partition_by_run(csbs[:good], run_id)

          # If we enriched, we free some space on HDFS by deleting the raw events
          # otherwise we need to copy the enriched events back to HDFS
          if enrich
            @jobflow.add_step(get_rmr_step(ENRICH_STEP_INPUT, standard_assets_bucket))
          else
            copy_to_hdfs_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
            copy_to_hdfs_step.arguments = [
              "--src"        , enrich_final_output, # Opposite way round to normal
              "--dest"       , ENRICH_STEP_OUTPUT,
              "--srcPattern" , PARTFILE_REGEXP,
              "--s3Endpoint" , s3_endpoint
            ] # Either user doesn't want compression, or files are already compressed
            copy_to_hdfs_step.name << ": Enriched S3 -> HDFS"
            @jobflow.add_step(copy_to_hdfs_step)
          end

          shred_step = build_scalding_step(
            "Shred Enriched Events",
            assets[:shred],
            "enrich.hadoop.ShredJob",
            { :in          => glob_path(ENRICH_STEP_OUTPUT),
              :good        => SHRED_STEP_OUTPUT,
              :bad         => partition_by_run(csbs[:bad], run_id),
              :errors      => partition_by_run(csbs[:errors], run_id, config[:enrich][:continue_on_unexpected_error])
            },
            {
              :iglu_config => Base64.strict_encode64(resolver)
            }
          )

          # Late check whether our target directory is empty
          @jobflow.add_step(get_check_dir_empty_step(csbs[:good], standard_assets_bucket))
          @jobflow.add_step(shred_step)

          # We need to copy our shredded types from HDFS back to S3
          copy_to_s3_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          copy_to_s3_step.arguments = [
            "--src"        , SHRED_STEP_OUTPUT,
            "--dest"       , shred_final_output,
            "--srcPattern" , PARTFILE_REGEXP,
            "--s3Endpoint" , s3_endpoint
          ] + output_codec
          copy_to_s3_step.name << ": Shredded HDFS -> S3"
          @jobflow.add_step(copy_to_s3_step)
        end

        if es
          get_elasticsearch_steps(config, assets, enrich, shred).each do |step|
            @jobflow.add_step(step)
          end
        end

        # We need to copy our enriched events from HDFS back to S3
        archive_raw_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
        archive_raw_step.arguments = [
          "--src"        , csbr[:processing],
          "--dest"       , partition_by_run(csbr[:archive], run_id),
          "--s3Endpoint" , s3_endpoint,
          "--deleteOnSuccess"
        ]
        archive_raw_step.name << ": Raw S3 Staging -> S3 Archive"
        @jobflow.add_step(archive_raw_step)

        self
      end

      # Create one step for each Elasticsearch target for each source for that target
      Contract ConfigHash, Hash, Bool, Bool => ArrayOf[ScaldingStep]
      def get_elasticsearch_steps(config, assets, enrich, shred)
        elasticsearch_targets = config[:storage][:targets].select {|t| t[:type] == 'elasticsearch'}

        # The default sources are the enriched and shredded errors generated for this run
        default_sources = []
        default_sources << partition_by_run(config[:aws][:s3][:buckets][:enriched][:bad], @run_id) if enrich
        default_sources << partition_by_run(config[:aws][:s3][:buckets][:shredded][:bad], @run_id) if shred

        steps = elasticsearch_targets.flat_map { |target|

          sources = target[:sources] || default_sources

          sources.map { |source|
            step = ScaldingStep.new(
              assets[:elasticsearch],
              "com.snowplowanalytics.snowplow.storage.hadoop.ElasticsearchJob",
              ({
                :input => source,
                :host => target[:host],
                :port => target[:port].to_s,
                :index => target[:database],
                :type => target[:table],
                :es_nodes_wan_only => target[:es_nodes_wan_only] ? "true" : "false"
              }).reject { |k, v| v.nil? }
            )
            step_name = "Errors in #{source} -> Elasticsearch: #{target[:name]}"
            step.name << ": #{step_name}"
            step
          }
        }

        # Wait 60 seconds before starting the first step so S3 can become consistent
        if (enrich || shred) && steps.any?
          steps[0].arguments << '--delay' << '60'
        end
        steps
      end

      # Run (and wait for) the daily ETL job.
      #
      # Throws a BootstrapFailureError if the job fails due to a bootstrap failure.
      # Throws an EmrExecutionError if the jobflow fails for any other reason.
      Contract ConfigHash => nil
      def run(config)

        snowplow_tracking_enabled = ! config[:monitoring][:snowplow].nil?

        jobflow_id = @jobflow.run
        logger.debug "EMR jobflow #{jobflow_id} started, waiting for jobflow to complete..."

        if snowplow_tracking_enabled
          Monitoring::Snowplow.parameterize(config)
          Monitoring::Snowplow.instance.track_job_started(@jobflow)
        end

        status = wait_for()

        if status.successful
          logger.debug "EMR jobflow #{jobflow_id} completed successfully."
          if snowplow_tracking_enabled
            Monitoring::Snowplow.instance.track_job_succeeded(@jobflow)
          end

        elsif status.bootstrap_failure
          if snowplow_tracking_enabled
            Monitoring::Snowplow.instance.track_job_failed(@jobflow)
          end
          raise BootstrapFailureError, get_failure_details(jobflow_id)

        elsif status.no_data_failure
          raise NoDataToProcessError, "No Snowplow logs to process since last run"

        elsif not status.dir_not_empty_failures.empty?
          raise DirectoryNotEmptyError,
            "The following directories should have been empty:\n#{status.dir_not_empty_failures.join("\n")}"

        else
          if snowplow_tracking_enabled
            Monitoring::Snowplow.instance.track_job_failed(@jobflow)
          end
          raise EmrExecutionError, get_failure_details(jobflow_id)
        end

        nil
      end

    private

      # Defines a Scalding data processing job as an Elasticity
      # jobflow step.
      #
      # Parameters:
      # +step_name+:: name of step
      # +main_class+:: Java main class to run
      # +folders+:: hash of in, good, bad, errors S3/HDFS folders
      # +extra_step_args+:: additional arguments to pass to the step
      #
      # Returns a step ready for adding to the jobflow.
      Contract String, String, String, Hash, Hash => ScaldingStep
      def build_scalding_step(step_name, jar, main_class, folders, extra_step_args={})

        # Build our argument hash
        arguments = extra_step_args
          .merge({
            :input_folder      => folders[:in],
            :output_folder     => folders[:good],
            :bad_rows_folder   => folders[:bad],
            :exceptions_folder => folders[:errors]
          })
          .reject { |k, v| v.nil? } # Because folders[:errors] may be empty

        # Now create the Hadoop MR step for the jobflow
        scalding_step = ScaldingStep.new(jar, "#{JAVA_PACKAGE}.#{main_class}", arguments)
        scalding_step.name << ": #{step_name}"

        scalding_step
      end

      # Wait for a jobflow.
      # Check its status every 5 minutes till it completes.
      #
      # Returns true if the jobflow completed without error,
      # false otherwise.
      Contract None => JobResult
      def wait_for()

        success = false
        bootstrap_failure = false
        no_data_failure = false
        dir_not_empty_failures = false

        # Loop until we can quit...
        while true do
          begin
            # Count up running tasks and failures
            statuses = @jobflow.cluster_step_status.map(&:state).inject([0, 0]) do |sum, state|
              [ sum[0] + (@@running_states.include?(state) ? 1 : 0), sum[1] + (@@failed_states.include?(state) ? 1 : 0) ]
            end

            # If no step is still running, then quit
            if statuses[0] == 0
              success = statuses[1] == 0 # True if no failures
              bootstrap_failure = EmrJob.bootstrap_failure?(@jobflow)
              no_data_failure = EmrJob.no_data_failure?(@jobflow)
              dir_not_empty_failures = EmrJob.dir_not_empty_failures(@jobflow)
              break
            else
              # Sleep a while before we check again
              sleep(120)
            end

          rescue SocketError => se
            logger.warn "Got socket error #{se}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue Errno::ECONNREFUSED => ref
            logger.warn "Got connection refused #{ref}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue Errno::ECONNRESET => res
            logger.warn "Got connection reset #{res}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue Errno::ETIMEDOUT => to
            logger.warn "Got connection timeout #{to}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue RestClient::InternalServerError => ise
            logger.warn "Got internal server error #{ise}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue Elasticity::ThrottlingException => te
            logger.warn "Got Elasticity throttling exception #{te}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue ArgumentError => ae
            logger.warn "Got Elasticity argument error #{ae}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue IOError => ioe
            logger.warn "Got IOError #{ioe}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          end
        end

        JobResult.new(success, bootstrap_failure, no_data_failure, dir_not_empty_failures)
      end

      # Prettified string containing failure details
      # for this job flow.
      Contract String => String
      def get_failure_details(jobflow_id)

        cluster_step_status = @jobflow.cluster_step_status
        cluster_status = @jobflow.cluster_status

        [
          "EMR jobflow #{jobflow_id} failed, check Amazon EMR console and Hadoop logs for details (help: https://github.com/snowplow/snowplow/wiki/Troubleshooting-jobs-on-Elastic-MapReduce). Data files not archived.",
          "#{@jobflow.name}: #{cluster_status.state} [#{cluster_status.last_state_change_reason}] ~ #{self.class.get_elapsed_time(cluster_status.ready_at, cluster_status.ended_at)} #{self.class.get_timespan(cluster_status.ready_at, cluster_status.ended_at)}"
        ].concat(cluster_step_status
            .sort { |a,b|
              self.class.nilable_spaceship(a.started_at, b.started_at)
            }
            .each_with_index
            .map { |s,i|
              " - #{i + 1}. #{s.name}: #{s.state} ~ #{self.class.get_elapsed_time(s.started_at, s.ended_at)} #{self.class.get_timespan(s.started_at, s.ended_at)}"
            })
          .join("\n")
      end

      # Gets the time span.
      #
      # Parameters:
      # +start+:: start time
      # +_end+:: end time
      Contract Maybe[Time], Maybe[Time] => String
      def self.get_timespan(start, _end)
        "[#{start} - #{_end}]"
      end

      # Spaceship operator supporting nils
      #
      # Parameters:
      # +a+:: First argument
      # +b+:: Second argument
      Contract Maybe[Time], Maybe[Time] => Num
      def self.nilable_spaceship(a, b)
        case
        when (a.nil? and b.nil?)
          0
        when a.nil?
          1
        when b.nil?
          -1
        else
          a <=> b
        end
      end

      # Gets the elapsed time in a
      # human-readable format.
      #
      # Parameters:
      # +start+:: start time
      # +_end+:: end time
      Contract Maybe[Time], Maybe[Time] => String
      def self.get_elapsed_time(start, _end)
        if start.nil? or _end.nil?
          "elapsed time n/a"
        else
          # Adapted from http://stackoverflow.com/a/19596579/255627
          seconds_diff = (start - _end).to_i.abs

          hours = seconds_diff / 3600
          seconds_diff -= hours * 3600

          minutes = seconds_diff / 60
          seconds_diff -= minutes * 60

          seconds = seconds_diff

          "#{hours.to_s.rjust(2, '0')}:#{minutes.to_s.rjust(2, '0')}:#{seconds.to_s.rjust(2, '0')}"
        end
      end

      # Returns true if the jobflow seems to have failed due to a bootstrap failure
      Contract Elasticity::JobFlow => Bool
      def self.bootstrap_failure?(jobflow)
        jobflow.cluster_step_status.all? {|s| s.state == 'CANCELLED'} &&
        (! (jobflow.cluster_status.last_state_change_reason =~ BOOTSTRAP_FAILURE_INDICATOR).nil?)
      end

      # Returns true if the jobflow failed due to the fact that there were no data to process
      Contract Elasticity::JobFlow => Bool
      def self.no_data_failure?(jobflow)
        jobflow.cluster_step_status.any? { |s|
          s.state == 'FAILED' && s.args.any? { |a| a =~ NO_DATA_FAILURE_INDICATOR }
        }
      end

      # Returns the directories that should have been empty
      Contract Elasticity::JobFlow => ArrayOf[String]
      def self.dir_not_empty_failures(jobflow)
        jobflow.cluster_step_status.select { |s|
          s.state == 'FAILED' && s.args.any? { |a| a =~ DIR_NOT_EMPTY_FAILURE_INDICATOR }
        }.map { |s| s.args[1] }
      end

      Contract String, String => Elasticity::ScriptStep
      def get_rmr_step(location, bucket)
        script = "#{bucket}common/emr/snowplow-hadoop-fs-rmr.sh"
        step = Elasticity::ScriptStep.new(@jobflow.region, script, location)
        step.name << ": Recursively removing content from #{location}"
        step
      end

      # Builds a script step checking that there is data in the processing bucket
      Contract String, String => Elasticity::ScriptStep
      def get_check_data_to_process_step(location, bucket)
        get_check_step("Checking that there is data to process in #{location}", location,
          "#{bucket}common/emr/snowplow-check-data-to-process.sh")
      end

      # Builds a script step checking that the specified location is empty
      Contract String, String => Elasticity::ScriptStep
      def get_check_dir_empty_step(location, bucket)
        get_check_step("Checking that #{location} is empty", location,
          "#{bucket}common/emr/snowplow-check-dir-empty.sh")
      end

      Contract String, String, String => Elasticity::ScriptStep
      def get_check_step(name, location, script)
        step = Elasticity::ScriptStep.new(@jobflow.region, script, location)
        step.name << ": #{name}"
        step
      end

    end
  end
end
