require 'bson'
require 'oplogjam'

module Oplogjam
  RSpec.describe ApplyOps do
    describe '#operations' do
      it 'returns the operations' do
        insert_bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1496414570, 11),
          :t => 14,
          :h => -3028027288268436781,
          :v => 2,
          :op => 'i',
          :ns => 'foo.bar',
          :o => BSON::Document.new(:baz => 'quux')
        )
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479420028, 1),
          :t => 1,
          :h => -1789557309812000233,
          :v => 2,
          :op => 'c',
          :ns => 'foo.$cmd',
          :o => BSON::Document.new(:applyOps => [insert_bson])
        )
        apply_ops = described_class.new(bson)
        insert = Insert.new(insert_bson)

        expect(apply_ops.operations).to contain_exactly(insert)
      end
    end
  end
end
