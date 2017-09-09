module Oplogjam
  class Table
    attr_reader :db

    def initialize(db)
      @db = db
    end

    def create(name)
      db.create_table?(name) do
        uuid :uuid, default: Sequel.function(:uuid_generate_v1), primary_key: true
        jsonb :id, null: false
        jsonb :document, null: false
        timestamp :created_at, null: false
        timestamp :updated_at
        timestamp :deleted_at
        unique %i[id deleted_at]
        index :id, unique: true, where: { deleted_at: nil }
      end
    end
  end
end
