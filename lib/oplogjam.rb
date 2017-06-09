require 'oplogjam/apply_ops'
require 'oplogjam/command'
require 'oplogjam/delete'
require 'oplogjam/insert'
require 'oplogjam/noop'
require 'oplogjam/update'

module Oplogjam
  UnknownOperation = Class.new(ArgumentError)

  def self.from(bson)
    case bson[:op]
    when 'n' then Noop.new(bson)
    when 'i' then Insert.new(bson)
    when 'u' then Update.new(bson)
    when 'd' then Delete.new(bson)
    when 'c'
      if bson[:o].key?(:applyOps)
        ApplyOps.new(bson)
      else
        Command.new(bson)
      end
    else
      raise UnknownOperation, "unknown operation: #{bson}"
    end
  end
end
