require 'oplogjam/jsonb'
require 'oplogjam/set'
require 'oplogjam/types'
require 'oplogjam/unset'

module Oplogjam
  InvalidUpdate = Class.new(ArgumentError)

  class Update
    attr_reader :h, :ts, :ns, :o2, :o

    def self.from(bson)
      h = bson.fetch('h'.freeze)
      ts = bson.fetch('ts'.freeze)
      ns = bson.fetch('ns'.freeze)
      o2 = bson.fetch('o2'.freeze)
      o = bson.fetch('o'.freeze)

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

    def apply(mapping)
      table = mapping.get(namespace)
      row_id = query.fetch('_id'.freeze).to_json

      table
        .where(id: row_id, deleted_at: nil)
        .update(document: jsonb_update, updated_at: Time.now.utc)
    end

    private

    def jsonb_update
      return Sequel.pg_jsonb(update) if replacement?

      unsets_to_jsonb(sets_to_jsonb(Sequel.pg_jsonb(:document)))
    end

    def sets_to_jsonb(column)
      return column unless update.key?('$set'.freeze)

      Set.from(update.fetch('$set'.freeze)).update(column)
    end

    def unsets_to_jsonb(column)
      return column unless update.key?('$unset'.freeze)

      Unset.from(update.fetch('$unset'.freeze)).delete(column)
    end

    def replacement?
      !update.key?('$set'.freeze) && !update.key?('$unset'.freeze)
    end
  end
end
