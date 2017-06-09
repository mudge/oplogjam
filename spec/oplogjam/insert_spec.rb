require 'bson'
require 'oplogjam/insert'

module Oplogjam
  RSpec.describe Insert do
    describe '#namespace' do
      it 'returns the namespace' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1496414570, 11),
          :t => 14,
          :h => -3028027288268436781,
          :v => 2,
          :op => 'i',
          :ns => 'foo.bar',
          :o => BSON::Document.new(:baz => 'quux')
        )
        insert = described_class.new(bson)

        expect(insert.namespace).to eq('foo.bar')
      end
    end
  end
end
