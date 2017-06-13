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
      op = bson.fetch('op'.freeze, 'unknown'.freeze)

      case op
      when 'n'.freeze then Noop.from(bson)
      when 'i'.freeze then Insert.from(bson)
      when 'u'.freeze then Update.from(bson)
      when 'd'.freeze then Delete.from(bson)
      when 'c'.freeze
        if bson.fetch('o'.freeze, {}).key?('applyOps'.freeze)
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
