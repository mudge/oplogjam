require 'oplogjam/types'

module Oplogjam
  InvalidDelete = Class.new(ArgumentError)

  class Delete
    attr_reader :h, :ts, :ns, :o

    def self.from(bson)
      h = bson.fetch('h'.freeze)
      ts = bson.fetch('ts'.freeze)
      ns = bson.fetch('ns'.freeze)
      o = bson.fetch('o'.freeze)

      new(h, ts, ns, o)
    rescue KeyError => e
      raise InvalidDelete, "missing field: #{e}"
    end

    def initialize(h, ts, ns, o)
      @h = Integer(h)
      @ts = Oplogjam::Timestamp(ts)
      @ns = String(ns)
      @o = Oplogjam::Document(o)
    end

    alias id h
    alias namespace ns
    alias query o

    def timestamp
      Time.at(ts.seconds, ts.increment)
    end

    def ==(other)
      return false unless other.is_a?(Delete)

      id == other.id
    end

    def apply(mapping)
      table = mapping.get(namespace)
      row_id = query.fetch('_id'.freeze).to_json

      table
        .where(id: row_id, deleted_at: nil)
        .update(deleted_at: Time.now.utc)
    end
  end
end
