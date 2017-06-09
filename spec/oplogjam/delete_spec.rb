require 'bson'
require 'oplogjam/delete'

module Oplogjam
  RSpec.describe Delete do
    describe '#query' do
      it 'returns the query' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479421186, 1),
          :t => 1,
          :h => -5457382347563537847,
          :v => 2,
          :op => 'd',
          :ns => 'foo.bar',
          :o => BSON::Document.new(:_id => BSON::ObjectId.from_string('582e287cfedf6fb051b2efdf'))
        )
        delete = described_class.new(bson)

        expect(delete.query).to eq(BSON::Document.new(:_id => BSON::ObjectId.from_string('582e287cfedf6fb051b2efdf')))
      end
    end
  end
end
