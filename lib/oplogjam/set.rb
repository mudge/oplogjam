module Oplogjam
  class Set
    attr_reader :tree

    def initialize(tree = {})
      @tree = tree
    end

    def populate(path)
      tree[path] ||= Intermediate.new(path)
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

    def self.from(operation)
      set = new
      operation.each do |dotted_path, value|

        # Split the dotted path `a.b.c` into an array `['a', 'b', 'c']`
        path = dotted_path.split('.')
        current_path = []

        # Start by populating the top-level set
        current_node = set

        # Go through the successive path segments, building up intermediate paths
        # ['a'], ['a', 'b']
        #
        # Note that we exclude the final path segment as that will be used below in a separate set phase and that we
        # must not mutate current_path in place (e.g. by using <<) as references to it will live on in node definitions.
        path[0...-1].each do |segment|
          current_path += [segment]

          # Populate an empty intermediate if need be, updating the current node so further traversal uses that as a
          # base
          current_node = current_node.populate(current_path)
        end

        # Set the final value on the full path
        current_node.set(path, value)
      end

      # Return the finally populated set
      set
    end
  end

  class Intermediate
    attr_reader :path, :tree

    def initialize(path, tree = {})
      @path = path
      @tree = tree
    end

    def populate(path)
      tree[path] ||= Intermediate.new(path)
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
end
