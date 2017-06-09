require 'bson'
require 'oplogjam/update'

module Oplogjam
  RSpec.describe Update do
    describe '#query' do
      it 'returns the query' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479561033, 1),
          :t => 2,
          :h => 3511341713062188019,
          :v => 2,
          :op => 'u',
          :ns => 'foo.bar',
          :o2 => BSON::Document.new(:_id => BSON::ObjectId.from_string('583033a3643431ab5be6ec35')),
          :o => BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )
        update = described_class.new(bson)

        expect(update.query).to eq(BSON::Document.new(:_id => BSON::ObjectId.from_string('583033a3643431ab5be6ec35')))
      end
    end

    describe '#update' do
      it 'returns the update' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479561033, 1),
          :t => 2,
          :h => 3511341713062188019,
          :v => 2,
          :op => 'u',
          :ns => 'foo.bar',
          :o2 => BSON::Document.new(:_id => BSON::ObjectId.from_string('583033a3643431ab5be6ec35')),
          :o => BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )
        update = described_class.new(bson)

        expect(update.update).to eq(BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz')))
      end
    end
  end
end
