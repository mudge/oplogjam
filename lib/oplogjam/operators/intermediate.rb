module Oplogjam
  module Operators
    class Intermediate
      attr_reader :path, :tree

      def initialize(path, tree = {})
        @path = path
        @tree = tree
      end

      def populate(path)
        if path.last =~ NUMERIC_INDEX
          populate_index(path)
        else
          populate_field(path)
        end
      end

      def set(path, value)
        if path.last =~ NUMERIC_INDEX
          set_index(path, value)
        else
          set_field(path, value)
        end
      end

      def populate_field(path)
        tree[path] ||= IntermediateField.new(path)
      end

      def populate_index(path)
        tree[path] ||= IntermediateIndex.new(path)
      end
      def set_field(path, value)
        tree[path] = FieldAssignment.new(path, value)
      end

      def set_index(path, value)
        tree[path] = IndexAssignment.new(path, value)
      end

      def nodes
        tree.values
      end
    end
  end
end
