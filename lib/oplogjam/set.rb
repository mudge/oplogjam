require 'oplogjam/jsonb'

module Oplogjam
  class Set
    def self.from(operation)
      operation.each_with_object(new) do |(dotted_path, value), set|

        # Split the dotted path `a.b.c` into an array `['a', 'b', 'c']`
        path = dotted_path.split('.'.freeze)
        current_path = []

        # Start by populating the top-level set
        current_node = set

        # Go through the successive path segments, building up intermediate paths
        # ['a'], ['a', 'b']
        #
        # Note that we exclude the final path segment as that will be used below in a separate set phase and that we
        # must not mutate current_path in place (e.g. by using <<) as references to it will live on in node definitions.
        path[0...-1].each_with_index do |segment, index|
          next_segment = path[index + 1]
          current_path += [segment]

          # Populate an empty intermediate if need be, updating the current node so further traversal uses that as a
          # base. As an intermediate can be either an object or an array, check whether the next segment is a numeric
          # index or not.
          if next_segment =~ /\A\d+\z/
            current_node = current_node.populate_array(current_path)
          else
            current_node = current_node.populate_object(current_path)
          end
        end

        # If the last segment is numeric then this is setting an index and we must ensure all previous indexes also
        # exist.
        if path.last =~ /\A\d+\z/
          index = Integer(path.last, 10)

          # Go through each previous index, setting it to null if need be
          (0...index).each do |i|
            current_node = current_node.set_null(current_path + [i.to_s])
          end
        end

        # Set the final value on the full path
        current_node.set(path, value)
      end
    end

    attr_reader :tree

    def initialize(tree = {})
      @tree = tree
    end

    def populate_object(path)
      tree[path] ||= IntermediateObject.new(path)
    end

    def populate_array(path)
      tree[path] ||= IntermediateArray.new(path)
    end

    def set(path, value)
      tree[path] = Assignment.new(path, value)
    end

    def update(column)
      nodes.inject(column) do |subject, node|
        node.update(subject)
      end
    end

    def nodes
      tree.values
    end
  end

  class IntermediateObject
    attr_reader :path, :tree

    def initialize(path, tree = {})
      @path = path
      @tree = tree
    end

    def populate_object(path)
      tree[path] ||= IntermediateObject.new(path)
    end

    def populate_array(path)
      tree[path] ||= IntermediateArray.new(path)
    end

    def set_null(path)
      tree[path] = Nullify.new(path)
    end

    def set(path, value)
      tree[path] = Assignment.new(path, value)
    end

    def update(column)
      populated_column = column.set(path, Sequel.function(:coalesce, column[path], Sequel.pg_jsonb({})))

      nodes.inject(populated_column) do |subject, node|
        node.update(subject)
      end
    end

    def nodes
      tree.values
    end
  end

  class IntermediateArray
    attr_reader :path, :tree

    def initialize(path, tree = {})
      @path = path
      @tree = tree
    end

    def populate_object(path)
      tree[path] ||= IntermediateObject.new(path)
    end

    def populate_array(path)
      tree[path] ||= IntermediateArray.new(path)
    end

    def set_null(path)
      tree[path] = Nullify.new(path)
    end

    def set(path, value)
      tree[path] = Assignment.new(path, value)
    end

    def update(column)
      populated_column = column.set(path, Sequel.function(:coalesce, column[path], Sequel.pg_jsonb([])))

      nodes.inject(populated_column) do |subject, node|
        node.update(subject)
      end
    end

    def nodes
      tree.values
    end
  end

  class Assignment
    attr_reader :path, :value

    def initialize(path, value)
      @path = path
      @value = value
    end

    def update(column)
      column.set(path, value.to_json)
    end
  end

  class Nullify
    attr_reader :path, :child

    def initialize(path)
      @path = path
    end

    def set_null(path)
      @child = Nullify.new(path)
    end

    def set(path, value)
      @child = Assignment.new(path, value)
    end

    def update(column)
      child.update(column.set(path, Sequel.function(:coalesce, column[path], 'null')))
    end
  end
end
