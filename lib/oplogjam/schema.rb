module Oplogjam
  class Schema
    COLUMNS = %i[id document created_at updated_at].freeze

    attr_reader :db

    def initialize(db)
      @db = db
    end

    def import(collection, name, batch_size = 100)
      collection.find.snapshot(true).each_slice(batch_size) do |documents|
        values = documents.map { |document|
          [
            Sequel.object_to_json(document.fetch(ID)),
            Sequel.pg_jsonb(document),
            Time.now.utc,
            Time.now.utc
          ]
        }

        db[name].import(COLUMNS, values)
      end
    end

    def create_table(name)
      db.create_table?(name) do
        uuid :uuid, default: Sequel.function(:uuid_generate_v1), primary_key: true
        jsonb :id, null: false
        jsonb :document, null: false
        timestamp :created_at, null: false
        timestamp :updated_at, null: false
        timestamp :deleted_at
      end
    end

    def add_indexes(name)
      db.alter_table(name) do
        add_index %i[id deleted_at], unique: true, if_not_exists: true
        add_index :id, unique: true, where: { deleted_at: nil }, if_not_exists: true
      end
    end
  end
end
