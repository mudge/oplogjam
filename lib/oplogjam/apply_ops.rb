require 'oplogjam/operation'

module Oplogjam
  class ApplyOps < Operation
    def operations
      o[:applyOps].map { |bson| Oplogjam.from(bson) }
    end
  end
end
