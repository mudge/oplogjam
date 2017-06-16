require 'oplogjam/constants'
require 'oplogjam/types'

module Oplogjam
  InvalidNoop = Class.new(ArgumentError)

  class Noop
    attr_reader :h, :ts, :msg

    def self.from(bson)
      h = bson.fetch(H)
      ts = bson.fetch(TS)
      o = bson.fetch(O)
      msg = o.fetch(MSG)

      new(h, ts, msg)
    rescue KeyError => e
      raise InvalidNoop, "missing field: #{e}"
    end

    def initialize(h, ts, msg)
      @h = Integer(h)
      @ts = Oplogjam::Timestamp(ts)
      @msg = String(msg)
    end

    alias message msg
    alias id h

    def timestamp
      Time.at(ts.seconds, ts.increment)
    end

    def ==(other)
      return false unless other.is_a?(Noop)

      id == other.id
    end

    def apply(_mapping); end
  end
end
