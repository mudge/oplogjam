module Oplogjam
  InvalidUpdate = Class.new(ArgumentError)

  class Update
    attr_reader :h, :ts, :ns, :o2, :o

    def self.from(bson)
      h = bson.fetch('h')
      ts = bson.fetch('ts')
      ns = bson.fetch('ns')
      o2 = bson.fetch('o2')
      o = bson.fetch('o')

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

    def apply(connection)
      connection[to_sql].update
    end

    def to_sql
      table_name = namespace.split('.', 2).join('_')
      row_id = String(query.fetch('_id'))

      DB
        .from(table_name)
        .where(id: row_id)
        .update_sql(document: jsonb_update, updated_at: Time.now.utc)
    end

    private

    def jsonb_update
      return Sequel.pg_jsonb(update) unless update.key?('$set') || update.key?('$unset')

      unsets_to_jsonb(sets_to_jsonb)
    end

    def sets_to_jsonb(column = Sequel.pg_jsonb(:document))
      update.fetch('$set', {}).inject(column) do |target, (field, value)|
        path = field.split('.')
        next target.set(path, value.to_json) if path.size == 1

        partial_path = []

        path
          .inject(target) { |expr, segment|
            partial_path += [segment]
            next expr if path == partial_path

            # Set any intermediate keys to empty hashes if they don't already exist.
            #
            # e.g.
            #
            #     jsonb_set("document", ARRAY['a'], coalesce(("document" #> ARRAY['a']), '{}'::jsonb), true)
            expr.set(
              partial_path,
              Sequel.function(:coalesce, expr[partial_path], Sequel.pg_jsonb({}))
            )
          }
          .set(partial_path, value.to_json)
      end
    end

    def unsets_to_jsonb(column = Sequel.pg_jsonb(:document))
      update.fetch('$unset', {}).inject(column) do |target, (field, _)|
        path = field.split('.')

        Sequel.pg_jsonb(target).delete_path(path)
      end
    end
  end
end