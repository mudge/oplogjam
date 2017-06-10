module Oplogjam
  InvalidUpdate = Class.new(ArgumentError)

  class Update
    attr_reader :h, :ts, :ns, :o2, :o

    def self.from(bson)
      h = bson.fetch('h')
      ts = bson.fetch('ts')
      ns = bson.fetch('ns')
      o2 = bson.fetch('o2')
      o = bson.fetch('o')

      new(h, ts, ns, o2, o)
    rescue KeyError => e
      fail InvalidUpdate, "missing field: #{e}"
    end

    def initialize(h, ts, ns, o2, o)
      @h = Integer(h)
      @ts = Oplogjam::Timestamp(ts)
      @ns = String(ns)
      @o2 = Oplogjam::Document(o2)
      @o = Oplogjam::Document(o)
    end

    alias_method :id, :h
    alias_method :namespace, :ns
    alias_method :query, :o2
    alias_method :update, :o

    def timestamp
      Time.at(ts.seconds, ts.increment)
    end

    def ==(other)
      return false unless other.is_a?(Update)

      id == other.id
    end

    def apply(connection)
      connection[to_sql].update
    end

    def to_sql
      table_name = namespace.split('.', 2).join('_')
      row_id = String(query.fetch('_id'))
      attributes = {
        :document => unsets_to_jsonb(sets_to_jsonb),
        :updated_at => Time.now.utc
      }

      DB.from(table_name).where(:id => row_id).update_sql(attributes)
    end

    private

    def sets_to_jsonb(column = :document)
      update.fetch('$set', {}).inject(column) do |target, (field, value)|
        path = field.split('.')

        Sequel.pg_jsonb(target).set(path, value.to_json)
      end
    end

    def unsets_to_jsonb(column = :document)
      update.fetch('$unset', {}).inject(column) do |target, (field, _)|
        path = field.split('.')

        Sequel.pg_jsonb(target).delete_path(path)
      end
    end
  end
end
