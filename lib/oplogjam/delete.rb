module Oplogjam
  InvalidDelete = Class.new(ArgumentError)

  class Delete
    attr_reader :h, :ts, :ns, :o

    def self.from(bson)
      h = bson.fetch('h')
      ts = bson.fetch('ts')
      ns = bson.fetch('ns')
      o = bson.fetch('o')

      new(h, ts, ns, o)
    rescue KeyError => e
      fail InvalidDelete, "missing field: #{e}"
    end

    def initialize(h, ts, ns, o)
      @h = Integer(h)
      @ts = Oplogjam::Timestamp(ts)
      @ns = String(ns)
      @o = Oplogjam::Document(o)
    end

    alias_method :id, :h
    alias_method :namespace, :ns
    alias_method :query, :o

    def timestamp
      Time.at(ts.seconds, ts.increment)
    end

    def ==(other)
      return false unless other.is_a?(Delete)

      id == other.id
    end
  end
end
