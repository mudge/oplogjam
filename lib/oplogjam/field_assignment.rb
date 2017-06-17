require 'oplogjam/assignment'

module Oplogjam
  class FieldAssignment < Assignment
    def update(column)
      column.set(path, value.to_json)
    end
  end
end
