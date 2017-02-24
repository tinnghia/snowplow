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

# Author::    Ben Fradet (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'spec_helper'
require 'elasticity'

EmrJob = Snowplow::EmrEtlRunner::EmrJob

describe EmrJob do
  describe '.get_check_step' do
    it 'should build a script step' do
      expect(EmrJob.send(:get_check_step, 'l', 'script').arguments).to eq([ 'script', 'l' ])
    end
  end

  describe '.get_check_dir_empty_step' do
    it 'should build a check-dir-empty script step' do
      expect(EmrJob.send(:get_check_dir_empty_step, 'l', 'b/').arguments).to eq(
        [ 'b/common/emr/snowplow-check-dir-empty.sh', 'l' ])
    end
  end
end
