require 'bson'
require 'oplogjam/operation'

module Oplogjam
  RSpec.describe Operation do
    describe '#timestamp' do
      it 'returns the operation timestamp as a Time' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:msg => 'initiating set')
        )
        noop = described_class.new(bson)

        expect(noop.timestamp).to eq(Time.at(1479419535, 1))
      end
    end

    describe '#id' do
      it 'returns a unique ID for this operation' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:msg => 'initiating set')
        )
        noop = described_class.new(bson)

        expect(noop.id).to eq(-2135725856567446411)
      end
    end

    describe '#==' do
      it 'is true for two operations with the same ID' do
        other = described_class.new(:h => 1)
        bson = BSON::Document.new(:h => 1)

        expect(described_class.new(bson)).to eq(other)
      end
    end
  end
end
