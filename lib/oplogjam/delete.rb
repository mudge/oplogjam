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

    def apply(connection)
      connection[to_sql].delete
    end

    def to_sql
      table_name = namespace.split('.', 2).join('_')
      row_id = String(query.fetch('_id'))

      DB.from(table_name).where(:id => row_id).delete_sql
    end
  end
end
