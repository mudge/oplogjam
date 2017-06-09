require 'oplogjam/operation'

module Oplogjam
  class Noop < Operation
    def message
      o[:msg]
    end
  end
end
