module Oplogjam
  class Assignment
    attr_reader :path, :value

    def initialize(path, value)
      @path = path
      @value = value
    end
  end
end
