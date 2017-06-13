require 'bson'
require 'oplogjam'

module Oplogjam
  RSpec.describe Update do
    let(:postgres) { Sequel.connect('postgres:///oplogjam_test') }
    let(:table) { postgres.from(:bar) }

    before(:example, :database) do
      Table.new(postgres).create(:bar)
    end

    after(:example, :database) do
      table.truncate
    end

    describe '.from' do
      it 'converts a BSON update into an Update' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_561_033, 1),
          t: 2,
          h: 3_511_341_713_062_188_019,
          v: 2,
          op: 'u',
          ns: 'foo.bar',
          o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
          o: BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )

        expect(described_class.from(bson)).to be_a(described_class)
      end

      it 'raises an error if the query is missing' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_561_033, 1),
          t: 2,
          h: 3_511_341_713_062_188_019,
          v: 2,
          op: 'u',
          ns: 'foo.bar',
          o: BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )

        expect { described_class.from(bson) }.to raise_error(InvalidUpdate)
      end
    end

    describe '#timestamp' do
      it 'returns the timestamp of the operation as a Time' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_561_033, 1),
          t: 2,
          h: 3_511_341_713_062_188_019,
          v: 2,
          op: 'u',
          ns: 'foo.bar',
          o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
          o: BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )
        update = described_class.from(bson)

        expect(update.timestamp).to eq(Time.at(1_479_561_033, 1))
      end
    end

    describe '#namespace' do
      it 'returns the namespace' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_561_033, 1),
          t: 2,
          h: 3_511_341_713_062_188_019,
          v: 2,
          op: 'u',
          ns: 'foo.bar',
          o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
          o: BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )
        update = described_class.from(bson)

        expect(update.namespace).to eq('foo.bar')
      end
    end

    describe '#query' do
      it 'returns the query' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_561_033, 1),
          t: 2,
          h: 3_511_341_713_062_188_019,
          v: 2,
          op: 'u',
          ns: 'foo.bar',
          o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
          o: BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )
        update = described_class.from(bson)

        expect(update.query).to eq(BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')))
      end
    end

    describe '#update' do
      it 'returns the update' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_561_033, 1),
          t: 2,
          h: 3_511_341_713_062_188_019,
          v: 2,
          op: 'u',
          ns: 'foo.bar',
          o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
          o: BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz'))
        )
        update = described_class.from(bson)

        expect(update.update).to eq(BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz')))
      end
    end

    describe '#apply', :database do
      it 'supports replacing the whole document' do
        table.insert(id: '1', document: '{}')
        update = build_update(1, 'name' => 'Alice', 'age' => 42)
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('name' => 'Alice', 'age' => 42))
      end

      it 'supports setting a field' do
        table.insert(id: '1', document: '{"name":"Alice","age":42}')
        update = build_update(1, '$set' => BSON::Document.new('name' => 'Bob'))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('name' => 'Bob', 'age' => 42))
      end

      it 'supports setting multiple fields' do
        table.insert(id: '1', document: '{"name":"Alice","age":42}')
        update = build_update(1, '$set' => BSON::Document.new('name' => 'Bob', 'shoeSize' => 12))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('name' => 'Bob', 'age' => 42, 'shoeSize' => 12))
      end

      it 'supports setting an index on an array' do
        table.insert(id: '1', document: '{"names":[]}')
        update = build_update(1, '$set' => BSON::Document.new('names.0' => 'Alice'))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('names' => ['Alice']))
      end

      it 'supports unsetting a field' do
        table.insert(id: '1', document: '{"name":"Alice","age":42}')
        update = build_update(1, '$unset' => BSON::Document.new('age' => ''))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('name' => 'Alice'))
      end

      it 'supports unsetting an index on an array' do
        table.insert(id: '1', document: '{"names":["Alice"]}')
        update = build_update(1, '$unset' => BSON::Document.new('names.0' => ''))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('names' => [nil]))
      end

      it 'supports setting and unsetting at the same time' do
        table.insert(id: '1', document: '{"name":"Alice"}')
        update = build_update(1, '$set' => BSON::Document.new('age' => 42), '$unset' => BSON::Document.new('name' => ''))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('age' => 42))
      end

      it 'supports setting nested fields' do
        table.insert(id: '1', document: '{"a":{"b":{"c":1}}}')
        update = build_update(1, '$set' => BSON::Document.new('a.b.c' => 2))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('a' => { 'b' => { 'c' => 2 } }))
      end

      it 'supports setting multiple nested fields' do
        table.insert(id: '1', document: '{"a":{"b":{"c":1,"d":2}}}')
        update = build_update(1, '$set' => BSON::Document.new('a.b.c' => 3, 'a.b.d' => 4))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('a' => { 'b' => { 'c' => 3, 'd' => 4 } }))
      end

      it 'supports setting nested fields when intermediates do not exist' do
        table.insert(id: '1', document: '{}')
        update = build_update(1, '$set' => BSON::Document.new('a.b.c' => 2))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('a' => { 'b' => { 'c' => 2 } }))
      end

      it 'supports setting multiple nested fields with missing intermediates' do
        table.insert(id: '1', document: '{}')
        update = build_update(1, '$set' => BSON::Document.new('a.b.c' => 2, 'a.e.f' => 3, 'a.g' => 4))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('a' => { 'b' => { 'c' => 2 }, 'e' => { 'f' => 3 }, 'g' => 4 }))
      end

      it 'supports setting nested indexes' do
        table.insert(id: '1', document: '{"a":{"b":[1]}}')
        update = build_update(1, '$set' => BSON::Document.new('a.b.1' => 2))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('a' => { 'b' => [1, 2] }))
      end

      it 'supports unsetting nested fields' do
        table.insert(id: '1', document: '{"a":{"b":{"c":1}}}')
        update = build_update(1, '$unset' => BSON::Document.new('a.b.c' => ''))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('a' => { 'b' => {} }))
      end

      it 'supports unsetting nested indexes' do
        table.insert(id: '1', document: '{"a":{"b":{"c":[1]}}}')
        update = build_update(1, '$unset' => BSON::Document.new('a.b.c.0' => ''))
        update.apply('foo.bar' => table)

        expect(table.first).to include(:document => Sequel.pg_jsonb('a' => { 'b' => { 'c' => [nil] } }))
      end
    end

    def build_update(id = '1', attributes = { '$set' => BSON::Document.new('bar' => 'baz') })
      bson = BSON::Document.new(
        ts: BSON::Timestamp.new(1_479_561_033, 1),
        t: 2,
        h: 3_511_341_713_062_188_019,
        v: 2,
        op: 'u',
        ns: 'foo.bar',
        o2: BSON::Document.new(_id: id),
        o: BSON::Document.new(attributes)
      )

      described_class.from(bson)
    end
  end
end
