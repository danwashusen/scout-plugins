require 'spec_helper'

describe CassandraPlugin do
  subject { CassandraPlugin.new(Time.now, {}, {cassandra_dir: '/usr/cassandra' }) }

  describe "build_report" do
    context "when cassandra nodetool returns successful result" do
      it "should take cassandra 'nodetool status' output and convert it to metrics" do
        output = File.read('./spec/support/outputs/successful_output.txt')
        expected_report = {
          :total_datacenters=>2,
          :total_nodes=>6,
          :avg_node_load=>26954498921.813335,
          :up_nodes=>5,
          :down_nodes=>1
        }
        subject.should_receive(:'`').with("/usr/cassandra/bin/nodetool status").and_return output
        subject.should_receive(:report).with expected_report
        subject.build_report
      end
    end

    context "when cassandra nodetool returns failing result" do
      it "should call alert" do
        output = File.read('./spec/support/outputs/failing_output.txt')
        subject.should_receive(:'`').with("/usr/cassandra/bin/nodetool status").and_return output
        subject.should_not_receive(:report)
        subject.should_receive(:alert).with "Plugin was unable to connect to C* cluster"
        subject.build_report
      end
    end
  end

  describe "load_to_number" do
    subject { CassandraPlugin.new(Time.now, {}, {}) }

    [
     ["346.98 KB", 355307.52],
     ["346.98 MB", 355307.52 * 1024],
     ["346.98 GB", 355307.52 * 1024 * 1024],
     ["346.98 TB", 355307.52 * 1024 * 1024 * 1024],
    ].each do |string_value, number|
      context "when #{string_value} provided" do
        it "should return #{number}" do
          subject.send(:load_to_number, string_value).should == number
        end
      end
    end
  end

end
