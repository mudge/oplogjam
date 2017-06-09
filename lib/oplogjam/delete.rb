require 'oplogjam/operation'

module Oplogjam
  class Delete < Operation
    def namespace
      document[:ns]
    end

    def query
      o
    end
  end
end
