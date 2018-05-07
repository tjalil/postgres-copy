require 'csv'

module PostgresCopy
  module ActsAsCopyTarget
    extend ActiveSupport::Concern

    included do
    end

    module CopyMethods

      # remove duplication in options_string assignment by breaking it into separate method 
      # applicable to both `copy_to` + `copy_from`
      def options_format(type, options)
        if options[:format] == :binary
          "BINARY"
        else
          if type == 'copy_to'
            "DELIMITER '#{options[:delimiter]}' CSV #{options[:header] ? 'HEADER' : ''}"
          else
            quote = options[:quote] == "'" ? "''" : options[:quote]
            null = options.key?(:null) ? "NULL '#{options[:null]}'" : ''
            "DELIMITER '#{options[:delimiter]}' QUOTE '#{quote}' #{null} CSV"
          end
        end
      end

      ##### COPY_TO START #####

      # extract data copy block from copy_to
      def execute_copy_to(path = nil, options_string, options_query)
        if path
          raise "You have to choose between exporting to a file or receiving the lines inside a block" if block_given?
          connection.execute("COPY (#{options_query}) TO '#{sanitize_sql(path)}' WITH #{options_string}")
        else
          connection.raw_connection.copy_data("COPY (#{options_query}) TO STDOUT WITH #{options_string}") do
            while line = connection.raw_connection.get_copy_data do
              yield(line) if block_given?
            end
          end
        end
      end

      # Copy data to a file passed as a string (the file path) or to lines that are passed to a block
      def copy_to(path = nil, options = {})
        options = {:delimiter => ",", :format => :csv, :header => true}.merge(options)
        options_string = options_format('copy_to', options)
        options_query = options.delete(:query) || self.all.to_sql

        execute_copy_to(path, options_string, options_query)
        return self
      end

      # chunk copied lines together 
      def chunk_lines(buffer_lines, result)
        Enumerator.new do |y|
          result.each_slice(buffer_lines.to_i) do |slice|
            y << slice.join
          end
        end
      end

      # Create an enumerator with each line from the CSV.
      # Note that using this directly in a controller response
      # will perform very poorly as each line will get put
      # into its own chunk. Joining every (eg) 100 rows together
      # is much, much faster.
      def copy_to_enumerator(options={})
        buffer_lines = options.delete(:buffer_lines)

        # Somehow, self loses its scope once inside the Enumerator
        scope = self.current_scope || self

        result = Enumerator.new do |y|
          scope.copy_to(nil, options) do |line|
            y << line
          end
        end
        
        if buffer_lines.to_i > 0
          chunk_lines(buffer_lines, result)
        else
          result
        end
      end

      # Copy all data to a single string
      def copy_to_string(options = {})
        data = ''
        self.copy_to(nil, options) {|l| data << l }

        if options[:format] == :binary
          data.force_encoding("ASCII-8BIT")
        end

        data
      end

      ##### COPY_TO END #####
      
      ##### COPY_FROM START #####

      def define_columns_list(options, io)
        if options[:format] == :binary
          options[:columns] || []
        elsif options[:header]
          line = io.gets
          options[:columns] || line.strip.split(options[:delimiter])
        else
          options[:columns]
        end
      end

      def define_table(options)
        if options[:table]
          connection.quote_table_name(options[:table])
        else
          quoted_table_name
        end
      end

      def execute_copy_from_csv_binary(options, io)
        bytes = 0

        begin
          while line = io.readpartial(10240)
            connection.raw_connection.put_copy_data(line)
            bytes += line.bytesize
          end
        
          rescue EOFError
        end
      end

      def execute_copy_from_csv_non_binary(options, io)
        while line = io.gets do
          next if line.strip.size == 0
          
          if block_given?
            row = CSV.parse_line(line.strip, {:col_sep => options[:delimiter]})
            yield(row)

            next if row.all? {|f| f.nil? }
            line = CSV.generate_line(row, {:col_sep => options[:delimiter]})
          end

          connection.raw_connection.put_copy_data(line)
        end
      end

      # Copy data from a CSV that can be passed as a string (the file path) or as an IO object.
      # * You can change the default delimiter, passing delimiter: '' in the options hash
      # * You can map fields from the file to different fields in the table using a map in the options hash
      # * For further details on usage take a look at the README.md
      def copy_from(path_or_io, options = {})
        options = {:delimiter => ",", :format => :csv, :header => true, :quote => '"'}.merge(options)
        options_string = options_format('copy_from', options)
        io = path_or_io.instance_of?(String) ? File.open(path_or_io, 'r') : path_or_io

        columns_list = define_columns_list(options, io)
        table = define_table(options)

        columns_list = columns_list.map {|c| options[:map][c.to_s] } if options[:map]
        columns_string = columns_list.size > 0 ? "(\"#{columns_list.join('","')}\")" : ""

        connection.raw_connection.copy_data(%{COPY #{table} #{columns_string} FROM STDIN #{options_string}}) do
          if options[:format] == :binary
            execute_copy_from_csv_binary(options, io)
          else
            execute_copy_from_csv_non_binary(options, io)
          end
        end
      end
    end

    ##### COPY_FROM END #####

    module ClassMethods
      def acts_as_copy_target
        extend PostgresCopy::ActsAsCopyTarget::CopyMethods
      end
    end
    
  end
end

ActiveRecord::Base.send :include, PostgresCopy::ActsAsCopyTarget
