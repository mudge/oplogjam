require 'sequel'
require 'oplogjam/mapping'
require 'oplogjam/progress'

module Oplogjam
  class Configuration
    attr_reader :db, :namespaces, :progress_options

    def initialize(db)
      @db = db
    end

    def load(config)
      @namespaces = config.fetch('namespaces'.freeze, {})
      @progress_options = config.fetch('progress'.freeze, 'table'.freeze => 'oplogjam'.freeze)

      self
    end

    def mapping
      Mapping.new(db).load(namespaces)
    end

    def progress
      progress_table = progress_options.fetch('table'.freeze)
      progress_schema = progress_options.fetch('schema'.freeze, 'public'.freeze)

      @progress = Progress.new(db.from(Sequel.qualify(progress_schema, progress_table)))
    end
  end
end
