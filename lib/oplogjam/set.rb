require 'oplogjam/field_assignment'
require 'oplogjam/index_assignment'
require 'oplogjam/intermediate_field'
require 'oplogjam/intermediate_index'

module Oplogjam
  class Set

    # Transform a MongoDB $set operation (e.g. $set: { 'a.1.b.0': 'foo' }) into a tree of nodes more amenable to
    # transforming into SQL.
    #
    # Specifically, parse nested field and index assignments into specific node types FieldAssignment, IndexAssignment
    # (for setting the final value) and IntermediateField, IntermediateIndex (for any intermediate fields and indexes).
    #
    # e.g.
    #
    #   $set: { a: 1 } will become Set(['a'] => FieldAssignment(['a'], 1))
    #   $set: { a: 1, b: 2 } will become Set(['a'] => FieldAssignment(['a'], 1), ['b'] => FieldAssignment(['b'], 2))
    #   $set: { 'a.b': 1 } will become Set(['a'] => IntermediateField(['a'], FieldAssignment(['a', 'b'], 1)))
    def self.from(operation)

      # Start with an empty Set and iterate over every key in the $set operation, mutating the Set as we go
      operation.each_with_object(new) do |(dotted_path, value), set|

        # Split the dotted path `a.b.c` into an array `['a', 'b', 'c']`
        path = dotted_path.split(FIELD_SEPARATOR)

        # Start with an empty path which will incrementally populate
        current_path = []

        # Starting with the set, go through the successive path segments, building up intermediate paths on the current
        # node, e.g. 'a.b' will iterate over ['a'], ['a', 'b']
        #
        # Note that we exclude the final path segment as that will be used below in a separate set phase
        populated_node = path[0...-1].inject(set) { |current_node, segment|

          # Extend the current path with the current segment appended.
          current_path << segment

          # Populate an empty intermediate with a copy of the current path.
          #
          # Note that this could either be a numeric index (which might be indexing into an array) or an object field
          # name.
          current_node.populate(current_path.dup)
        }

        # Set the final value on the full path
        populated_node.set(path, value)
      end
    end

    attr_reader :tree

    def initialize(tree = {})
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

    def update(column)
      nodes.inject(column, &UPDATE_COLUMN)
    end

    def nodes
      tree.values
    end
  end
end
