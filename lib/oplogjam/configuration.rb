require 'oplogjam/mapping'
require 'oplogjam/progress'
require 'oplogjam/table'

module Oplogjam
  class Configuration
    attr_reader :db, :namespaces, :progress_options

    def initialize(db)
      @db = db
    end

    def load(config)
      @namespaces = config.fetch('namespaces', {})
      @progress_options = config.fetch('progress', TABLE => 'oplogjam')

      self
    end

    def mapping
      Mapping.new(db).load(namespaces)
    end

    def progress
      progress_table = progress_options.fetch(TABLE)
      progress_schema = progress_options.fetch(SCHEMA, PUBLIC)

      @progress = Progress.new(db.from(Sequel.qualify(progress_schema, progress_table)))
    end
  end
end
