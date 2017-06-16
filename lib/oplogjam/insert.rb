require 'oplogjam/constants'
require 'oplogjam/jsonb'
require 'oplogjam/types'

module Oplogjam
  InvalidInsert = Class.new(ArgumentError)

  class Insert
    attr_reader :h, :ts, :ns, :o

    def self.from(bson)
      h = bson.fetch(H)
      ts = bson.fetch(TS)
      ns = bson.fetch(NS)
      o = bson.fetch(O)

      new(h, ts, ns, o)
    rescue KeyError => e
      raise InvalidInsert, "missing field: #{e}"
    end

    def initialize(h, ts, ns, o)
      @h = Integer(h)
      @ts = Oplogjam::Timestamp(ts)
      @ns = String(ns)
      @o = Oplogjam::Document(o)
    end

    alias namespace ns
    alias id h
    alias document o

    def timestamp
      Time.at(ts.seconds, ts.increment)
    end

    def ==(other)
      return false unless other.is_a?(Insert)

      id == other.id
    end

    def apply(mapping)
      table = mapping[namespace]
      row_id = document.fetch(ID).to_json

      table.insert(id: row_id,
                   document: Sequel.pg_jsonb(document),
                   created_at: Time.now.utc,
                   updated_at: Time.now.utc)
    end
  end
end
