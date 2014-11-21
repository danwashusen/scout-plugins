class CassandraPlugin < Scout::Plugin

  class UnableToConnect < RuntimeError; end
  class InvalidNodetool < RuntimeError; end

  DATA_SUFFIXES = ['KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']

  OPTIONS = <<-EOS
    nodetool_path:
      default: /usr/bin/nodetool
      name: Cassandra nodetool path
      notes: Path to the Cassandra nodetool executable.
    cassandra_host:
      default: 127.0.0.1
      name: Cassandra host
      notes: Cassnadra node hostname or ip address.
  EOS

  needs 'csv'

  def build_report
    data_centers = post_process_data_centers collect_data_centers_info
    data = {}
    data[:total_datacenters] = data_centers.size
    data[:total_nodes] = data_centers.collect{|dc| dc[:nodes].size }.reduce :+
    data[:avg_node_load] = avg_node_load data_centers
    data[:up_nodes] = nodes_by_status data_centers, 'UN'
    data[:down_nodes] = nodes_by_status data_centers, 'DN'
    report data
  rescue UnableToConnect
    alert "Plugin was unable to connect to C* cluster"
  rescue InvalidNodetool
    error "Plugin was unable to exec nodetool_path '#{option(:nodetool_path)}'"
  end

  protected
  def nodes_by_status(data_centers, status)
    data_centers.collect{|dc| dc[:nodes].select{|n| n[:status] == status}.size }.reduce :+
  end

  def avg_node_load(data_centers)
    loads = data_centers.collect{|dc| dc[:nodes].collect{|n| n[:load] } }.flatten
    loads.collect{|l| load_to_number(l) }.reduce(:+) / loads.size.to_f
  end

  def load_to_number(load)
    value, suffix = load.match(/(\d+\.\d+) (['A-Z']{2})$/)[1..2]
    value = value.to_f
    multiply_times = DATA_SUFFIXES.index(suffix) + 1
    multiply_times.times{ value = value * 1024}
    value
  end

  def collect_data_centers_info
    data_centers = []
    nodetool_path = option(:nodetool_path)
    cassandra_host = option(:cassandra_host)
    data = `#{nodetool_path} --host #{cassandra_host} status 2>&1`
    raise InvalidNodetool if not $?.success?
    raise UnableToConnect if data =~ /Connection refused/
    raise UnableToConnect if data =~ /Cannot resolve/
    current_data_center = nil
    data.each_line do |line|
      next if line.strip.empty?
      case line
      when /Datacenter\:/
        current_data_center = {}
        current_data_center[:name] = line.match(/\: (.+)/)[1]
        data_centers << current_data_center
      when /\-\-/
        current_data_center[:csv] = line
      end
      next unless current_data_center
      next unless current_data_center[:csv]
      current_data_center[:csv] << line
    end
    data_centers
  end

  def post_process_data_centers(data_centers)
    data_centers.each do |data_center|
      data_center[:nodes] = []
      csv_string = data_center.delete(:csv)
      next unless csv_string
      keys = []
      csv_string.lines.each do |line|
        case line
        when /^--/
          keys = line.split(/\s{2}+/).collect do |key|
            case key
            when /--/
              :status
            else
              key.strip.gsub(' ', '_').downcase.to_sym
            end
          end
        else
          values = line.split(/\s{2}+/)
          node_hash = {}
          keys.each_with_index do |key, i|
            node_hash[key] = values[i].strip
          end
          data_center[:nodes] << node_hash
        end
      end
    end
    data_centers
  end
end
