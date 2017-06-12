module Oplogjam
  InvalidInsert = Class.new(ArgumentError)

  class Insert
    attr_reader :h, :ts, :ns, :o

    def self.from(bson)
      h = bson.fetch('h')
      ts = bson.fetch('ts')
      ns = bson.fetch('ns')
      o = bson.fetch('o')

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

    def apply(connection)
      connection[to_sql].insert
    end

    def to_sql
      table_name = namespace.split('.', 2).join('_')
      row_id = document.fetch('_id').to_json

      DB
        .from(table_name)
        .insert_sql(id: row_id,
                    document: Sequel.pg_jsonb(document),
                    created_at: Time.now.utc,
                    updated_at: Time.now.utc)
    end
  end
end
