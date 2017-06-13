require 'sequel'

module Oplogjam
  class Mapping
    attr_reader :db, :mapping

    def initialize(db)
      @db = db
      @mapping = Hash.new(UnmappedNamespace.new)
    end

    def load(mappings)
      mappings.each do |namespace, table_options|
        table = table_options.fetch('table'.freeze)
        schema = table_options.fetch('schema'.freeze, 'public'.freeze)

        mapping[namespace] = db.from(Sequel.qualify(schema, table))
      end

      self
    end

    def [](namespace)
      mapping[namespace]
    end
  end

  class UnmappedNamespace
    def where(_)
      self
    end

    def update(_); end
    def insert(_); end
  end
end
