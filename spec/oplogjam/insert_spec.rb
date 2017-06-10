require 'bson'
require 'oplogjam'

module Oplogjam
  RSpec.describe Insert do
    describe '.from' do
      it 'converts a BSON insert into an Insert' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1496414570, 11),
          :t => 14,
          :h => -3028027288268436781,
          :v => 2,
          :op => 'i',
          :ns => 'foo.bar',
          :o => BSON::Document.new(
            :_id => BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            :baz => 'quux'
          )
        )

        expect(described_class.from(bson)).to be_a(described_class)
      end

      it 'raises an error if the document is missing' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1496414570, 11),
          :t => 14,
          :h => -3028027288268436781,
          :v => 2,
          :op => 'i',
          :ns => 'foo.bar'
        )

        expect { described_class.from(bson) }.to raise_error(InvalidInsert)
      end
    end

    describe '#namespace' do
      it 'returns the namespace' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1496414570, 11),
          :t => 14,
          :h => -3028027288268436781,
          :v => 2,
          :op => 'i',
          :ns => 'foo.bar',
          :o => BSON::Document.new(
            :_id => BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            :baz => 'quux'
          )
        )
        insert = described_class.from(bson)

        expect(insert.namespace).to eq('foo.bar')
      end
    end

    describe '#id' do
      it 'returns a unique identifier for the insert' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1496414570, 11),
          :t => 14,
          :h => -3028027288268436781,
          :v => 2,
          :op => 'i',
          :ns => 'foo.bar',
          :o => BSON::Document.new(
            :_id => BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            :baz => 'quux'
          )
        )
        insert = described_class.from(bson)

        expect(insert.id).to eq(-3028027288268436781)
      end
    end

    describe '#document' do
      it 'returns the document being inserted' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1496414570, 11),
          :t => 14,
          :h => -3028027288268436781,
          :v => 2,
          :op => 'i',
          :ns => 'foo.bar',
          :o => BSON::Document.new(
            :_id => BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            :baz => 'quux'
          )
        )
        insert = described_class.from(bson)

        expect(insert.document).to eq(
          BSON::Document.new(
            :_id => BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            :baz => 'quux'
          )
        )
      end
    end

    describe '#timestamp' do
      it 'returns the time of the operation as a Time' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1496414570, 11),
          :t => 14,
          :h => -3028027288268436781,
          :v => 2,
          :op => 'i',
          :ns => 'foo.bar',
          :o => BSON::Document.new(
            :_id => BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            :baz => 'quux'
          )
        )
        insert = described_class.from(bson)

        expect(insert.timestamp).to eq(Time.at(1496414570, 11))
      end
    end

    describe '#ts' do
      it 'returns the raw underlying BSON timestamp' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1496414570, 11),
          :t => 14,
          :h => -3028027288268436781,
          :v => 2,
          :op => 'i',
          :ns => 'foo.bar',
          :o => BSON::Document.new(
            :_id => BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            :baz => 'quux'
          )
        )
        insert = described_class.from(bson)

        expect(insert.ts).to eq(BSON::Timestamp.new(1496414570, 11))
      end
    end

    describe '#==' do
      it 'is equal to another operation with the same ID' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1496414570, 11),
          :t => 14,
          :h => -3028027288268436781,
          :v => 2,
          :op => 'i',
          :ns => 'foo.bar',
          :o => BSON::Document.new(
            :_id => BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            :baz => 'quux'
          )
        )
        another_bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1496414570, 11),
          :t => 14,
          :h => -3028027288268436781,
          :v => 2,
          :op => 'i',
          :ns => 'foo.bar',
          :o => BSON::Document.new(
            :_id => BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            :baz => 'quux'
          )
        )
        insert = described_class.from(bson)
        another_insert = described_class.from(another_bson)

        expect(insert).to eq(another_insert)
      end
    end

    describe '#to_sql' do
      it 'returns an equivalent SQL insert' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            :ts => BSON::Timestamp.new(1496414570, 11),
            :t => 14,
            :h => -3028027288268436781,
            :v => 2,
            :op => 'i',
            :ns => 'foo.bar',
            :o => BSON::Document.new(
              :_id => BSON::ObjectId('593bac55da605b0dbf3b25a5'),
              :baz => 'quux'
            )
          )
          insert = described_class.from(bson)

          expect(insert.to_sql).to eq("INSERT INTO \"foo_bar\" (\"id\", \"document\", \"created_at\", \"updated_at\") VALUES ('593bac55da605b0dbf3b25a5', '{\"_id\":{\"$oid\":\"593bac55da605b0dbf3b25a5\"},\"baz\":\"quux\"}'::jsonb, '2001-01-01 00:00:00.000000+0000', '2001-01-01 00:00:00.000000+0000')")
        end
      end
    end
  end
end
