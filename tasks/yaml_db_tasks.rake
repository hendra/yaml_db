namespace :db do
	desc "Dump schema and data to db/schema.rb and db/data.yml"
	task(:dump => [ "db:schema:dump", "db:data:dump" ])

	desc "Load schema and data from db/schema.rb and db/data.yml"
	task(:load => [ "db:schema:load", "db:data:load" ])

	namespace :data do
		def db_dump_data_file (extension = "yml")
			"#{dump_dir}/data.#{extension}"
        end
            
        def dump_dir(dir = "")
          "#{RAILS_ROOT}/db#{dir}"
        end

		desc "Dump contents of database to db/data.extension (defaults to yaml)"
		task :dump, :format_class, :needs => :environment do |t,args|
            args.with_defaults(:format_class => "YamlDb::Helper")
            helper = args.format_class.constantize
			SerializationHelper.new(helper).dump db_dump_data_file helper.extension
		end

		desc "Dump contents of database to curr_dir_name/tablename.extension (defaults to yaml)"
		task :dump_dir, :format_class, :needs => :environment do |t,args|
            args.with_defaults(:format_class => "YamlDb::Helper")
            time_dir = dump_dir "/#{Time.now.to_s.gsub(/ /, '_')}"
            SerializationHelper.new(args.format_class.constantize).dump_to_dir time_dir
		end

		desc "Load contents of db/data.yml into database"
		task(:load => :environment) do
			SerializationHelper.new(YamlDb::Helper).load db_dump_data_file
		end
	end
end
