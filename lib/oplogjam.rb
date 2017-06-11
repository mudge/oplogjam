require 'sequel'
require 'oplogjam/operation'
require 'oplogjam/oplog'
require 'oplogjam/set'

module Oplogjam
  DB = Sequel.connect('mock://postgres')
  Sequel.extension :pg_json
  Sequel.extension :pg_json_ops
  Sequel.extension :pg_array

  def self.Timestamp(ts)
    raise TypeError, "#{ts} is not a BSON Timestamp" unless ts.is_a?(BSON::Timestamp)

    ts
  end

  def self.Document(document)
    raise TypeError, "#{document} is not a BSON Document" unless document.is_a?(BSON::Document)

    document
  end
end
