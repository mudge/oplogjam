require 'bson'
require 'oplogjam'

module Oplogjam
  RSpec.describe Noop do
    describe '.from' do
      it 'converts a BSON no-op into an Noop' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:msg => 'initiating set')
        )

        expect(described_class.from(bson)).to be_a(described_class)
      end

      it 'raises an error if the message is missing' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:foo => 'bar')
        )

        expect { described_class.from(bson) }.to raise_error(InvalidNoop)
      end
    end

    describe '#message' do
      it 'returns the message' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:msg => 'initiating set')
        )
        noop = described_class.from(bson)

        expect(noop.message).to eq('initiating set')
      end
    end

    describe '#id' do
      it 'returns a unique identifier for the operation' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:msg => 'initiating set')
        )
        noop = described_class.from(bson)

        expect(noop.id).to eq(-2135725856567446411)
      end
    end

    describe '#timestamp' do
      it 'returns the time of the operation as a Time' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:msg => 'initiating set')
        )
        noop = described_class.from(bson)

        expect(noop.timestamp).to eq(Time.at(1479419535, 1))
      end
    end

    describe '#ts' do
      it 'returns the raw underlying BSON timestamp' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:msg => 'initiating set')
        )
        noop = described_class.from(bson)

        expect(noop.ts).to eq(BSON::Timestamp.new(1479419535, 1))
      end
    end

    describe '#==' do
      it 'is equal to another noop with the same ID' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:msg => 'initiating set')
        )
        another_bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:msg => 'initiating set')
        )
        noop = described_class.from(bson)
        another_noop = described_class.from(another_bson)

        expect(noop).to eq(another_noop)
      end
    end

    describe '#to_sql' do
      it 'returns nil' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:msg => 'initiating set')
        )
        noop = described_class.from(bson)

        expect(noop.to_sql).to be_nil
      end
    end
  end
end
