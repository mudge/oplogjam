module Oplogjam
  InvalidCommand = Class.new(ArgumentError)

  class Command
    attr_reader :h, :ts, :ns, :o

    def self.from(bson)
      h = bson.fetch(H)
      ts = bson.fetch(TS)
      ns = bson.fetch(NS)
      o = bson.fetch(O)

      new(h, ts, ns, o)
    rescue KeyError => e
      raise InvalidCommand, "missing field: #{e}"
    end

    def initialize(h, ts, ns, o)
      @h = Integer(h)
      @ts = Oplogjam::Timestamp(ts)
      @ns = String(ns)
      @o = Oplogjam::Document(o)
    end

    alias id h
    alias command o
    alias namespace ns

    def timestamp
      Time.at(ts.seconds, ts.increment)
    end

    def ==(other)
      return false unless other.is_a?(Command)

      id == other.id
    end

    def apply(_mapping); end
  end
end
