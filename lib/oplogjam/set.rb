require 'oplogjam/constants'
require 'oplogjam/jsonb'

module Oplogjam
  class Set
    def self.from(operation)
      operation.each_with_object(new) do |(dotted_path, value), set|
        # Split the dotted path `a.b.c` into an array `['a', 'b', 'c']`
        path = dotted_path.split(FIELD_SEPARATOR)
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
          # base.
          #
          # Note that this could either be a numeric index (which might be indexing into an array) or an object field
          # name.
          if segment =~ NUMERIC_INDEX
            current_node = current_node.populate_index(current_path)
          else
            current_node = current_node.populate_field(current_path)
          end
        end

        # Set the final value on the full path
        if path.last =~ NUMERIC_INDEX
          current_node.set_index(path, value)
        else
          current_node.set_field(path, value)
        end
      end
    end

    attr_reader :tree

    def initialize(tree = {})
      @tree = tree
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
      nodes.inject(column) do |subject, node|
        node.update(subject)
      end
    end

    def nodes
      tree.values
    end
  end

  class Intermediate
    attr_reader :path, :tree

    def initialize(path, tree = {})
      @path = path
      @tree = tree
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

  class IntermediateField < Intermediate
    def update(column)
      populated_column = column.set(path, Sequel.function(:coalesce, column[path], EMPTY_OBJECT))

      nodes.inject(populated_column) do |subject, node|
        node.update(subject)
      end
    end
  end

  class IntermediateIndex < Intermediate
    def update(column)
      # Now for a not-so-fun bit!
      #
      # As this is a numeric index, it might either be an index into an existing array or a numeric field name on an
      # object.
      #
      # If it is an index into an array then we need to ensure that all prior indexes down to 0 are either set or null.
      # If it is anything else, it should be an empty object. In order to figure that out, we need to look at the parent
      # path and switch based on its type.
      filled_array_column = (0...index).inject(column) do |subject, i|
        prior_path = parent_path + [i.to_s]

        subject.set(prior_path, Sequel.function(:coalesce, column[prior_path], NULL))
      end

      populated_column = Sequel.pg_jsonb_op(
        Sequel.case(
          {
            ARRAY_TYPE => filled_array_column.set(path, Sequel.function(:coalesce, filled_array_column[path], EMPTY_OBJECT))
          },
          column.set(path, Sequel.function(:coalesce, column[path], EMPTY_OBJECT)),
          Sequel.function(:jsonb_typeof, column[parent_path])
        )
      )

      nodes.inject(populated_column) do |subject, node|
        node.update(subject)
      end
    end

    def parent_path
      path[0...-1]
    end

    def index
      Integer(path.last, 10)
    end
  end

  class Assignment
    attr_reader :path, :value

    def initialize(path, value)
      @path = path
      @value = value
    end
  end

  class FieldAssignment < Assignment
    def update(column)
      column.set(path, value.to_json)
    end
  end

  class IndexAssignment < Assignment
    def update(column)
      # Now for a not-so-fun bit!
      #
      # As this is a numeric index, it might either be an index into an existing array or a numeric field name on an
      # object.
      #
      # If it is an index into an array then we need to ensure that all prior indexes down to 0 are either set or null.
      filled_array_column = (0...index).inject(column) do |subject, i|
        prior_path = parent_path + [i.to_s]

        subject.set(prior_path, Sequel.function(:coalesce, column[prior_path], NULL))
      end

      populated_column = Sequel.pg_jsonb_op(
        Sequel.case(
          { ARRAY_TYPE => filled_array_column },
          column,
          Sequel.function(:jsonb_typeof, column[parent_path])
        )
      )

      populated_column.set(path, value.to_json)
    end

    def index
      Integer(path.last, 10)
    end

    def parent_path
      path[0...-1]
    end
  end
end
