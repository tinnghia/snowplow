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

require 'contracts'

module Snowplow
  module EmrEtlRunner
    class Runner

      include Contracts

      include Monitoring::Logging

      # Initialize the class.
      Contract ArgsHash, ConfigHash, ArrayOf[String], String => Runner
      def initialize(args, config, enrichments_array, resolver)

        # Let's set our logging level immediately
        Monitoring::Logging::set_level config[:monitoring][:logging][:level]

        @args = args
        @config = config
        @enrichments_array = enrichments_array
        @resolver = resolver

        self
      end

      # Our core flow
      Contract None => nil
      def run

        resume = @args[:resume_from]
        steps = {
          :staging => resume.nil?,
          :enrich => (resume.nil? or resume == 'enrich'),
          :shred => (resume.nil? or [ 'enrich', 'shred' ].include?(resume)),
          :es => (resume.nil? or [ 'enrich', 'shred', 'elasticsearch' ].include?(resume))
        }

        # Keep relaunching the job until it succeeds or fails for a reason other than a bootstrap failure
        tries_left = @config[:aws][:emr][:bootstrap_failure_tries]
        while true
          begin
            tries_left -= 1
            job = EmrJob.new(@args[:debug], steps[:staging], steps[:enrich], steps[:shred], steps[:es],
              @config, @enrichments_array, @resolver)
            job.run(@config)
            break
          rescue BootstrapFailureError => bfe
            logger.warn "Job failed. #{tries_left} tries left..."
            if tries_left > 0
              # Random timeout between 0 and 10 minutes
              bootstrap_timeout = rand(1..600)
              logger.warn("Bootstrap failure detected, retrying in #{bootstrap_timeout} seconds...")
              sleep(bootstrap_timeout)
            else
              raise
            end
          end
        end

        logger.info "Completed successfully"
        nil
      end

    end
  end
end
