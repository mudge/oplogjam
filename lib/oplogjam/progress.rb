require 'bson'

module Oplogjam
  class Progress
    attr_reader :db

    def initialize(db)
      @db = db
    end

    def latest
      populate
      row = db.from(:oplogjam).first

      BSON::Timestamp.new(*row.values_at(:seconds, :increment))
    end

    def populate
      return unless db.from(:oplogjam).empty?

      db.from(:oplogjam).insert(seconds: 0, increment: 0)
    end

    def record(operation)
      ts = operation.ts

      db.from(:oplogjam).update(seconds: ts.seconds, increment: ts.increment)
    end
  end
end
