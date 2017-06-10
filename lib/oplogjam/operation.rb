require 'oplogjam/apply_ops'
require 'oplogjam/command'
require 'oplogjam/delete'
require 'oplogjam/insert'
require 'oplogjam/noop'
require 'oplogjam/update'

module Oplogjam
  InvalidOperation = Class.new(ArgumentError)

  class Operation
    def self.from(bson)
      op = bson.fetch('op', 'unknown')

      case op
      when 'n' then Noop.from(bson)
      when 'i' then Insert.from(bson)
      when 'u' then Update.from(bson)
      when 'd' then Delete.from(bson)
      when 'c'
        if bson.fetch('o', {}).key?('applyOps')
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
