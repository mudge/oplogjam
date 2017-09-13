module Oplogjam
  InvalidDelete = Class.new(ArgumentError)

  class Delete
    attr_reader :h, :ts, :ns, :o

    def self.from(bson)
      h = bson.fetch(H)
      ts = bson.fetch(TS)
      ns = bson.fetch(NS)
      o = bson.fetch(O)

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
      table = mapping[namespace]
      return unless table

      row_id = query.fetch(ID).to_json

      table
        .where(id: row_id, deleted_at: nil)
        .update(updated_at: Time.now.utc, deleted_at: Time.now.utc)
    end
  end
end
