require 'oplogjam/operators/assignment'

module Oplogjam
  module Operators
    class FieldAssignment < Assignment
      def update(column)
        column.set(path, value.to_json)
      end
    end
  end
end
