module Oplogjam
  InvalidApplyOps = Class.new(ArgumentError)

  class ApplyOps
    attr_reader :h, :ts, :ns, :apply_ops

    def self.from(bson)
      h = bson.fetch('h')
      ts = bson.fetch('ts')
      ns = bson.fetch('ns')
      o = bson.fetch('o')
      apply_ops = o.fetch('applyOps')

      new(h, ts, ns, apply_ops)
    rescue KeyError => e
      fail InvalidApplyOps, "missing field: #{e}"
    end

    def initialize(h, ts, ns, apply_ops)
      @h = Integer(h)
      @ts = Oplogjam::Timestamp(ts)
      @ns = String(ns)
      @apply_ops = Array(apply_ops)
    end

    alias_method :id, :h
    alias_method :namespace, :ns

    def timestamp
      Time.at(ts.seconds, ts.increment)
    end

    def operations
      apply_ops.map { |bson| Operation.from(bson) }
    end

    def apply(connection)
      operations.each do |operation|
        operation.apply(connection)
      end
    end

    def to_sql
      operations.map { |operation| operation.to_sql }.join(';')
    end
  end
end
