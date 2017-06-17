module Oplogjam
  class Table
    attr_reader :db

    def initialize(db)
      @db = db
    end

    def create(name)
      db.create_table?(name) do
        uuid :uuid, default: Sequel.function(:uuid_generate_v4), primary_key: true
        jsonb :id, null: false
        jsonb :document, null: false
        timestamp :created_at
        timestamp :updated_at
        timestamp :deleted_at
        unique %i[id deleted_at]
      end
    end
  end
end
