require 'bson'

module Oplogjam
  class Progress
    attr_reader :table

    def initialize(table)
      @table = table
    end

    def latest
      populate
      row = table.first

      BSON::Timestamp.new(*row.values_at(:seconds, :increment))
    end

    def populate
      return unless table.empty?

      table.insert(seconds: 0, increment: 0)
    end

    def record(operation)
      ts = operation.ts

      table.update(seconds: ts.seconds, increment: ts.increment)
    end
  end
end
