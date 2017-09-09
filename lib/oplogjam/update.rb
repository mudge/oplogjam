require 'oplogjam/operators'

module Oplogjam
  InvalidUpdate = Class.new(ArgumentError)

  class Update
    attr_reader :h, :ts, :ns, :o2, :o

    def self.from(bson)
      h = bson.fetch(H)
      ts = bson.fetch(TS)
      ns = bson.fetch(NS)
      o2 = bson.fetch(O2)
      o = bson.fetch(O)

      new(h, ts, ns, o2, o)
    rescue KeyError => e
      raise InvalidUpdate, "missing field: #{e}"
    end

    def initialize(h, ts, ns, o2, o)
      @h = Integer(h)
      @ts = Oplogjam::Timestamp(ts)
      @ns = String(ns)
      @o2 = Oplogjam::Document(o2)
      @o = Oplogjam::Document(o)
    end

    alias id h
    alias namespace ns
    alias query o2
    alias update o

    def timestamp
      Time.at(ts.seconds, ts.increment)
    end

    def ==(other)
      return false unless other.is_a?(Update)

      id == other.id
    end

    def apply(mapping)
      table = mapping[namespace]
      return unless table

      row_id = query.fetch(ID).to_json

      table
        .where(id: row_id, deleted_at: nil)
        .update(document: jsonb_update, updated_at: Time.now.utc)
    end

    private

    def jsonb_update
      return Sequel.pg_jsonb(query.merge(update)) if replacement?

      unsets_to_jsonb(sets_to_jsonb(Sequel.pg_jsonb_op(:document)))
    end

    def sets_to_jsonb(column)
      return column unless update.key?(SET)

      Operators::Set.from(update.fetch(SET)).update(column)
    end

    def unsets_to_jsonb(column)
      return column unless update.key?(UNSET)

      Operators::Unset.from(update.fetch(UNSET)).delete(column)
    end

    def replacement?
      !update.key?(SET) && !update.key?(UNSET)
    end
  end
end
