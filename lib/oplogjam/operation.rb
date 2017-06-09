module Oplogjam
  class Operation
    attr_reader :document

    def initialize(document)
      @document = document
    end

    def timestamp
      Time.at(ts.seconds, ts.increment)
    end

    def id
      document[:h]
    end

    def o
      document[:o]
    end

    def ts
      document[:ts]
    end

    def ==(other)
      other.is_a?(Operation) && id == other.id
    end
  end
end
