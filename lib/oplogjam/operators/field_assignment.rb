require 'oplogjam/operators/assignment'

module Oplogjam
  module Operators
    class FieldAssignment < Assignment
      def update(column)
        column.set(path, Sequel.object_to_json(value))
      end
    end
  end
end
