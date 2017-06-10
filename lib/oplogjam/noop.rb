module Oplogjam
  InvalidNoop = Class.new(ArgumentError)

  class Noop
    attr_reader :h, :ts, :msg

    def self.from(bson)
      h = bson.fetch('h')
      ts = bson.fetch('ts')
      o = bson.fetch('o')
      msg = o.fetch('msg')

      new(h, ts, msg)
    rescue KeyError => e
      fail InvalidNoop, "missing field: #{e}"
    end

    def initialize(h, ts, msg)
      @h = Integer(h)
      @ts = Oplogjam::Timestamp(ts)
      @msg = String(msg)
    end

    alias_method :message, :msg
    alias_method :id, :h

    def timestamp
      Time.at(ts.seconds, ts.increment)
    end

    def ==(other)
      return false unless other.is_a?(Noop)

      id == other.id
    end

    def apply(_connection)
    end

    def to_sql
    end
  end
end
