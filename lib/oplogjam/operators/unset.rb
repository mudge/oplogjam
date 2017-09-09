require 'oplogjam/operators/unset_field'
require 'oplogjam/operators/unset_index'

module Oplogjam
  module Operators
    class Unset
      def self.from(operation)
        operation.each_with_object(new) do |(dotted_path, _), unset|
          path = dotted_path.split(FIELD_SEPARATOR)

          if path.last =~ NUMERIC_INDEX
            unset.unset_index(path)
          else
            unset.unset_field(path)
          end
        end
      end

      attr_reader :unsets

      def initialize(unsets = [])
        @unsets = unsets
      end

      def unset_field(path)
        unsets << UnsetField.new(path)
      end

      def unset_index(path)
        unsets << UnsetIndex.new(path)
      end

      def delete(column)
        unsets.inject(column) do |subject, unset|
          unset.delete(subject)
        end
      end
    end
  end
end
