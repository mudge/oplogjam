require 'oplogjam/operators/assignment'

module Oplogjam
  module Operators
    class IndexAssignment < Assignment
      def update(column)
        # Now for a not-so-fun bit!
        #
        # As this is a numeric index, it might either be an index into an existing array or a numeric field name on an
        # object.
        #
        # If it is an index into an array then we need to ensure that all prior indexes down to 0 are either set or null.
        filled_array_column = (0...index).inject(column) { |subject, i|
          prior_path = parent_path + [i.to_s]

          subject.set(prior_path, Sequel.function(:coalesce, column[prior_path], NULL))
        }

        populated_column = Sequel.pg_jsonb_op(
          Sequel.case(
            { ARRAY_TYPE => filled_array_column },
            column,
            column[parent_path].typeof
          )
        )

        populated_column.set(path, Sequel.object_to_json(value))
      end

      def index
        Integer(path.last, 10)
      end

      def parent_path
        path[0...-1]
      end
    end
  end
end
