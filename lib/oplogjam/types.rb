require 'bson'

module Oplogjam
  def self.Timestamp(ts)
    raise TypeError, "#{ts} is not a BSON Timestamp" unless ts.is_a?(BSON::Timestamp)

    ts
  end

  def self.Document(document)
    raise TypeError, "#{document} is not a BSON Document" unless document.is_a?(BSON::Document)

    document
  end
end
