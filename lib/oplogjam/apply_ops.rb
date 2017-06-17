module Oplogjam
  InvalidApplyOps = Class.new(ArgumentError)

  class ApplyOps
    attr_reader :h, :ts, :ns, :apply_ops

    def self.from(bson)
      h = bson.fetch(H)
      ts = bson.fetch(TS)
      ns = bson.fetch(NS)
      o = bson.fetch(O)
      apply_ops = o.fetch(APPLY_OPS)

      new(h, ts, ns, apply_ops)
    rescue KeyError => e
      raise InvalidApplyOps, "missing field: #{e}"
    end

    def initialize(h, ts, ns, apply_ops)
      @h = Integer(h)
      @ts = Oplogjam::Timestamp(ts)
      @ns = String(ns)
      @apply_ops = Array(apply_ops)
    end

    alias id h
    alias namespace ns

    def timestamp
      Time.at(ts.seconds, ts.increment)
    end

    def apply(mapping)
      operations.each do |operation|
        operation.apply(mapping)
      end
    end

    def operations
      apply_ops.map { |bson| Operation.from(bson) }
    end

    def ==(other)
      return unless other.is_a?(ApplyOps)

      id == other.id
    end
  end
end
