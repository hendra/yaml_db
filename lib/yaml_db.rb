require 'rubygems'
require 'yaml'
require 'active_record'
require 'rails/railtie'
require 'yaml_db/rake_tasks'
require 'yaml_db/version'
require 'yaml_db/serialization_helper'

module YamlDb
  module Helper
    def self.loader
      Load
    end

    def self.dumper
      Dump
    end

    def self.extension
      "yml"
    end
  end


  module Utils
    def self.chunk_records(records)
      yaml = [ records ].to_yaml
      yaml.sub!(/---\s\n|---\n/, '')
      yaml.sub!('- - -', '  - -')
      yaml
    end

  end

  class Dump < SerializationHelper::Dump

    def self.dump_table_columns(io, table)
      io.write("\n")
      io.write({ table => { 'columns' => table_column_names(table) } }.to_yaml)
    end

    def self.dump_table_records(io, table)
      table_record_header(io)

      column_names = table_column_names(table)

      each_table_page(table) do |records|
        rows = SerializationHelper::Utils.unhash_records(records.to_a, column_names)
        io.write(Utils.chunk_records(rows))
      end
    end

    def self.table_record_header(io)
      io.write("  records: \n")
    end

  end

  class Load < SerializationHelper::Load
    # Monkey path to reorder truncatation and table loads to respect foreign key dependencies
    def self.load_documents(io, truncate = true)
      yall = {}
      YAML.load_stream(io) do |ydoc|
        yall.merge!(ydoc)
      end

      unordered_tables = yall.keys.reject { |table| ['ar_internal_metadata', 'schema_info', 'schema_migrations'].include?(table) }.sort
      tables = []
      while unordered_tables.any?
        loadable_tables = unordered_tables.find_all do |table|
          foreign_keys = ActiveRecord::Base.connection.foreign_keys(table)
          foreign_keys.reject { |foreign_key| tables.include?(foreign_key.to_table) }.empty?
        end

        if loadable_tables.empty?
          abort("Unable to sequence the following tables for loading: " + unordered_tables.join(', '))
        end

        tables += loadable_tables
        unordered_tables -= loadable_tables
      end

      if truncate == true
        tables.reverse.each do |table|
          truncate_table(table)
        end
      end

      tables.each do |table|
        next if yall[table].nil?
        load_table(table, yall[table], truncate)
      end
    end
  end

  class Railtie < Rails::Railtie
    rake_tasks do
      load File.expand_path('../tasks/yaml_db_tasks.rake',
__FILE__)
    end
  end

end
