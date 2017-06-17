module Oplogjam
  class UnsetIndex
    attr_reader :path

    def initialize(path)
      @path = path
    end

    def delete(column)
      nullify_or_unset = Sequel.case(
        [
          [
            column[parent_path].array_length > index,
            column.set(path, NULL)
          ]
        ],
        column.delete_path(path)
      )

      Sequel.pg_jsonb_op(
        Sequel.case(
          { ARRAY_TYPE => nullify_or_unset },
          column.delete_path(path),
          column[parent_path].typeof
        )
      )
    end

    def parent_path
      path[0...-1]
    end

    def index
      Integer(path.last, 10)
    end
  end
end
