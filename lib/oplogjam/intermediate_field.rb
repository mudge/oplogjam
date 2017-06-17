require 'oplogjam/intermediate'

module Oplogjam
  class IntermediateField < Intermediate
    def update(column)
      populated_column = column.set(path, Sequel.function(:coalesce, column[path], EMPTY_OBJECT))

      nodes.inject(populated_column, &UPDATE_COLUMN)
    end
  end
end
