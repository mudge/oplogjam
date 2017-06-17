require 'oplogjam/intermediate'

module Oplogjam
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
      filled_array_column = (0...index).inject(column) { |subject, i|
        prior_path = parent_path + [i.to_s]

        subject.set(prior_path, Sequel.function(:coalesce, column[prior_path], NULL))
      }

      populated_column = Sequel.pg_jsonb_op(
        Sequel.case(
          {
            ARRAY_TYPE => filled_array_column.set(path, Sequel.function(:coalesce, filled_array_column[path], EMPTY_OBJECT))
          },
          column.set(path, Sequel.function(:coalesce, column[path], EMPTY_OBJECT)),
          column[parent_path].typeof
        )
      )

      nodes.inject(populated_column, &UPDATE_COLUMN)
    end

    def parent_path
      path[0...-1]
    end

    def index
      Integer(path.last, 10)
    end
  end
end
