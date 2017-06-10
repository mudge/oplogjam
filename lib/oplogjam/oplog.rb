require 'oplogjam/operation'

module Oplogjam
  class Oplog
    attr_reader :client

    def initialize(client)
      @client = client
    end

    def oplog(query = {})
      cursor = client.use('local')['oplog.rs'].find(query, timeout: false, cursor_type: :tailable_await)

      Enumerator.new do |yielder|
        cursor.each do |document|
          yielder << Operation.from(document)
        end
      end
    end
  end
end
