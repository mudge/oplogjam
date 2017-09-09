module Oplogjam
  module Operators
    class UnsetField
      attr_reader :path

      def initialize(path)
        @path = path
      end

      def delete(column)
        column.delete_path(path)
      end
    end
  end
end
