require 'oplogjam/set'
require 'oplogjam/unset'

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
      raise InvalidUpdate, "missing field: #{e}"
    end

    def initialize(h, ts, ns, o2, o)
      @h = Integer(h)
      @ts = Oplogjam::Timestamp(ts)
      @ns = String(ns)
      @o2 = Oplogjam::Document(o2)
      @o = Oplogjam::Document(o)
    end

    alias id h
    alias namespace ns
    alias query o2
    alias update o

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
      row_id = query.fetch('_id').to_json

      DB
        .from(table_name)
        .where(id: row_id, deleted_at: nil)
        .update_sql(document: jsonb_update, updated_at: Time.now.utc)
    end

    private

    def jsonb_update
      return Sequel.pg_jsonb(update) if replacement?

      unsets_to_jsonb(sets_to_jsonb(Sequel.pg_jsonb(:document)))
    end

    def sets_to_jsonb(column)
      return column unless update.key?('$set')

      Set.from(update.fetch('$set')).update(column)
    end

    def unsets_to_jsonb(column)
      return column unless update.key?('$unset')

      Unset.from(update.fetch('$unset')).delete(column)
    end

    def replacement?
      !update.key?('$set') && !update.key?('$unset')
    end
  end
end
