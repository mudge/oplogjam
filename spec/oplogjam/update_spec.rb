require 'bson'
require 'oplogjam'

module Oplogjam
  RSpec.describe Update do
    describe '.from' do
      it 'converts a BSON update into an Update' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479561033, 1),
          :t => 2,
          :h => 3511341713062188019,
          :v => 2,
          :op => 'u',
          :ns => 'foo.bar',
          :o2 => BSON::Document.new(:_id => BSON::ObjectId('583033a3643431ab5be6ec35')),
          :o => BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )

        expect(described_class.from(bson)).to be_a(described_class)
      end

      it 'raises an error if the query is missing' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479561033, 1),
          :t => 2,
          :h => 3511341713062188019,
          :v => 2,
          :op => 'u',
          :ns => 'foo.bar',
          :o => BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )

        expect { described_class.from(bson) }.to raise_error(InvalidUpdate)
      end
    end

    describe '#timestamp' do
      it 'returns the timestamp of the operation as a Time' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479561033, 1),
          :t => 2,
          :h => 3511341713062188019,
          :v => 2,
          :op => 'u',
          :ns => 'foo.bar',
          :o2 => BSON::Document.new(:_id => BSON::ObjectId('583033a3643431ab5be6ec35')),
          :o => BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )
        update = described_class.from(bson)

        expect(update.timestamp).to eq(Time.at(1479561033, 1))
      end
    end

    describe '#namespace' do
      it 'returns the namespace' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479561033, 1),
          :t => 2,
          :h => 3511341713062188019,
          :v => 2,
          :op => 'u',
          :ns => 'foo.bar',
          :o2 => BSON::Document.new(:_id => BSON::ObjectId('583033a3643431ab5be6ec35')),
          :o => BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )
        update = described_class.from(bson)

        expect(update.namespace).to eq('foo.bar')
      end
    end

    describe '#query' do
      it 'returns the query' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479561033, 1),
          :t => 2,
          :h => 3511341713062188019,
          :v => 2,
          :op => 'u',
          :ns => 'foo.bar',
          :o2 => BSON::Document.new(:_id => BSON::ObjectId('583033a3643431ab5be6ec35')),
          :o => BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )
        update = described_class.from(bson)

        expect(update.query).to eq(BSON::Document.new(:_id => BSON::ObjectId('583033a3643431ab5be6ec35')))
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
          :o2 => BSON::Document.new(:_id => BSON::ObjectId('583033a3643431ab5be6ec35')),
          :o => BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )
        update = described_class.from(bson)

        expect(update.update).to eq(BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz')))
      end
    end
  end
end
