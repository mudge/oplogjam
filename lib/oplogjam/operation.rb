require 'oplogjam/apply_ops'
require 'oplogjam/command'
require 'oplogjam/constants'
require 'oplogjam/delete'
require 'oplogjam/insert'
require 'oplogjam/noop'
require 'oplogjam/update'

module Oplogjam
  InvalidOperation = Class.new(ArgumentError)

  class Operation
    def self.from(bson)
      op = bson.fetch('op'.freeze, 'unknown'.freeze)

      case op
      when N then Noop.from(bson)
      when I then Insert.from(bson)
      when U then Update.from(bson)
      when D then Delete.from(bson)
      when C
        if bson.fetch(O, {}).key?(APPLY_OPS)
          ApplyOps.from(bson)
        else
          Command.from(bson)
        end
      else
        raise InvalidOperation, "invalid operation: #{bson}"
      end
    end
  end
end
