require 'oplogjam/operation'

module Oplogjam
  class Insert < Operation
    def namespace
      document[:ns]
    end
  end
end
