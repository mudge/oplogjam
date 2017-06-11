module Oplogjam
  class Set
    attr_reader :root

    def self.from(set)
      root = set.each_with_object({}) do |(dotted_path, value), tree|
        path_segments = dotted_path.split('.')
        current_path = []

        path_segments.inject(tree) do |acc, path_segment|
          current_path += [path_segment]

          if path_segment == path_segments.last
            acc[current_path] = value
          else
            acc[current_path] ||= {}
          end

          acc[current_path]
        end
      end

      new(root)
    end

    def initialize(root)
      @root = root
    end

    def to_sql(column)
      root.inject(column) do |subject, (field, value)|
        next subject.set(field, value.to_json) unless value.is_a?(Hash)

        self
          .class
          .new(value)
          .to_sql(subject.set(field, Sequel.function(:coalesce, subject[field], Sequel.pg_jsonb({}))))
      end
    end

    def ==(other)
      return false unless other.is_a?(Set)

      root == other.root
    end
  end
end
