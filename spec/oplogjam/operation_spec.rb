require 'bson'
require 'oplogjam'

module Oplogjam
  RSpec.describe Operation do
    describe '.from' do
      it 'converts BSON no-ops into Noop' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:msg => 'initiating set')
        )

        expect(described_class.from(bson)).to be_a(Noop)
      end

      it 'converts BSON inserts into Inserts' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1496414570, 11),
          :t => 14,
          :h => -3028027288268436781,
          :v => 2,
          :op => 'i',
          :ns => 'foo.bar',
          :o => BSON::Document.new(:_id => 1, :baz => 'quux')
        )

        expect(described_class.from(bson)).to be_a(Insert)
      end

      it 'converts BSON updates into Updates' do
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

        expect(described_class.from(bson)).to be_a(Update)
      end

      it 'converts BSON deletes into Deletes' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479421186, 1),
          :t => 1,
          :h => -5457382347563537847,
          :v => 2,
          :op => 'd',
          :ns => 'foo.bar',
          :o => BSON::Document.new(:_id => BSON::ObjectId('582e287cfedf6fb051b2efdf'))
        )

        expect(described_class.from(bson)).to be_a(Delete)
      end

      it 'converts BSON commands into Commands' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479420028, 1),
          :t => 1,
          :h => -1789557309812000233,
          :v => 2,
          :op => 'c',
          :ns => 'foo.$cmd',
          :o => BSON::Document.new(:create => 'bar')
        )

        expect(described_class.from(bson)).to be_a(Command)
      end

      it 'converts BSON applyOps into ApplyOps' do
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
          :o => BSON::Document.new(
            :applyOps => [
              BSON::Document.new(
                :ts => BSON::Timestamp.new(1496414570, 11),
                :t => 14,
                :h => -3028027288268436781,
                :v => 2,
                :op => 'i',
                :ns => 'foo.bar',
                :o => BSON::Document.new(:_id => 1, :baz => 'quux')
              )
            ]
          )
        )

        expect(described_class.from(bson)).to be_a(Oplogjam::ApplyOps)
      end

      it 'raises an error if given an unknown operation' do
        bson = BSON::Document.new(:not => 'an operation')

        expect { described_class.from(bson) }.to raise_error(InvalidOperation)
      end
    end
  end
end
