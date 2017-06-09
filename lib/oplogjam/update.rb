require 'oplogjam/operation'

module Oplogjam
  class Update < Operation
    def namespace
      document[:ns]
    end

    def query
      document[:o2]
    end

    def update
      o
    end
  end
end
