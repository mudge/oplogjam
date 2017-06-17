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
      it 'applies {"a"=>1, "b"=>2} to {}' do
        table.insert(id: '1', document: '{"_id":1}')
        update = build_update(1, 'a' => 1, 'b' => 2)
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => 1, 'b' => 2))
      end

      it 'applies {"$set"=>{"a"=>1}} to {}' do
        table.insert(id: '1', document: '{"_id":1}')
        update = build_update(1, '$set' => { 'a' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => 1))
      end

      it 'applies {"$set"=>{"a"=>1}} to {"a"=>0}' do
        table.insert(id: '1', document: '{"_id":1,"a":0}')
        update = build_update(1, '$set' => { 'a' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => 1))
      end

      it 'applies {"$set"=>{"a"=>1, "b"=>2}} to {}' do
        table.insert(id: '1', document: '{"_id":1}')
        update = build_update(1, '$set' => { 'a' => 1, 'b' => 2 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => 1, 'b' => 2))
      end

      it 'applies {"$set"=>{"a"=>1, "b"=>2}} to {"a"=>0, "b"=>0}' do
        table.insert(id: '1', document: '{"_id":1,"a":0,"b":0}')
        update = build_update(1, '$set' => { 'a' => 1, 'b' => 2 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => 1, 'b' => 2))
      end

      it 'applies {"$unset"=>{"a"=>""}} to {"a"=>0, "b"=>0}' do
        table.insert(id: '1', document: '{"_id":1,"a":0,"b":0}')
        update = build_update(1, '$unset' => { 'a' => '' })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'b' => 0))
      end

      it 'applies {"$unset"=>{"a"=>"", "b"=>""}} to {"a"=>0, "b"=>0}' do
        table.insert(id: '1', document: '{"_id":1,"a":0,"b":0}')
        update = build_update(1, '$unset' => { 'a' => '', 'b' => '' })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1))
      end

      it 'applies {"$unset"=>{"c"=>""}} to {"a"=>0, "b"=>0}' do
        table.insert(id: '1', document: '{"_id":1,"a":0,"b":0}')
        update = build_update(1, '$unset' => { 'c' => '' })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => 0, 'b' => 0))
      end

      it 'applies {"$set"=>{"b"=>1}, "$unset"=>{"a"=>""}} to {"a"=>0}' do
        table.insert(id: '1', document: '{"_id":1,"a":0}')
        update = build_update(1, '$set' => { 'b' => 1 }, '$unset' => { 'a' => '' })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'b' => 1))
      end

      it 'applies {"$set"=>{"a.0"=>1}} to {}' do
        table.insert(id: '1', document: '{"_id":1}')
        update = build_update(1, '$set' => { 'a.0' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => { '0' => 1 }))
      end

      it 'applies {"$set"=>{"a.0"=>1}} to {"a"=>[]}' do
        table.insert(id: '1', document: '{"_id":1,"a":[]}')
        update = build_update(1, '$set' => { 'a.0' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => [1]))
      end

      it 'applies {"$set"=>{"a.0"=>1}} to {"a"=>{}}' do
        table.insert(id: '1', document: '{"_id":1,"a":{}}')
        update = build_update(1, '$set' => { 'a.0' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => { '0' => 1 }))
      end

      it 'applies {"$set"=>{"a.1"=>1}} to {"a"=>[]}' do
        table.insert(id: '1', document: '{"_id":1,"a":[]}')
        update = build_update(1, '$set' => { 'a.1' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => [nil, 1]))
      end

      it 'applies {"$unset"=>{"a.0"=>""}} to {}' do
        table.insert(id: '1', document: '{"_id":1}')
        update = build_update(1, '$unset' => { 'a.0' => '' })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1))
      end

      it 'applies {"$unset"=>{"a.0"=>""}} to {"a"=>[]}' do
        table.insert(id: '1', document: '{"_id":1,"a":[]}')
        update = build_update(1, '$unset' => { 'a.0' => '' })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => []))
      end

      it 'applies {"$unset"=>{"a.0"=>""}} to {"a"=>[1]}' do
        table.insert(id: '1', document: '{"_id":1,"a":[1]}')
        update = build_update(1, '$unset' => { 'a.0' => '' })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => [nil]))
      end

      it 'applies {"$unset"=>{"a.0"=>""}} to {"a"=>[1, 2]}' do
        table.insert(id: '1', document: '{"_id":1,"a":[1,2]}')
        update = build_update(1, '$unset' => { 'a.0' => '' })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => [nil, 2]))
      end

      it 'applies {"$unset"=>{"a.1"=>""}} to {"a"=>[1, 2]}' do
        table.insert(id: '1', document: '{"_id":1,"a":[1,2]}')
        update = build_update(1, '$unset' => { 'a.1' => '' })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => [1, nil]))
      end

      it 'applies {"$unset"=>{"a.2"=>""}} to {"a"=>[1, 2]}' do
        table.insert(id: '1', document: '{"_id":1,"a":[1,2]}')
        update = build_update(1, '$unset' => { 'a.2' => '' })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => [1, 2]))
      end

      it 'applies {"$unset"=>{"a.0"=>""}} to {"a"=>{}}' do
        table.insert(id: '1', document: '{"_id":1,"a":{}}')
        update = build_update(1, '$unset' => { 'a.0' => '' })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => {}))
      end

      it 'applies {"$unset"=>{"a.1"=>""}} to {"a"=>[]}' do
        table.insert(id: '1', document: '{"_id":1,"a":[]}')
        update = build_update(1, '$unset' => { 'a.1' => '' })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => []))
      end

      it 'applies {"$set"=>{"a.b"=>1}} to {}' do
        table.insert(id: '1', document: '{"_id":1}')
        update = build_update(1, '$set' => { 'a.b' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => { 'b' => 1 }))
      end

      it 'applies {"$set"=>{"a.b.c"=>1}} to {}' do
        table.insert(id: '1', document: '{"_id":1}')
        update = build_update(1, '$set' => { 'a.b.c' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => { 'b' => { 'c' => 1 } }))
      end

      it 'applies {"$set"=>{"a.b.c"=>1, "a.d"=>2}} to {}' do
        table.insert(id: '1', document: '{"_id":1}')
        update = build_update(1, '$set' => { 'a.b.c' => 1, 'a.d' => 2 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => { 'b' => { 'c' => 1 }, 'd' => 2 }))
      end

      it 'applies {"$set"=>{"a.b"=>1}} to {"a"=>{}}' do
        table.insert(id: '1', document: '{"_id":1,"a":{}}')
        update = build_update(1, '$set' => { 'a.b' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => { 'b' => 1 }))
      end

      it 'applies {"$set"=>{"a.b"=>1}} to {"a"=>{"b"=>0}}' do
        table.insert(id: '1', document: '{"_id":1,"a":{"b":0}}')
        update = build_update(1, '$set' => { 'a.b' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => { 'b' => 1 }))
      end

      it 'applies {"$set"=>{"a.1.b"=>1}} to {}' do
        table.insert(id: '1', document: '{"_id":1}')
        update = build_update(1, '$set' => { 'a.1.b' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => { '1' => { 'b' => 1 } }))
      end

      it 'applies {"$set"=>{"a.1.b"=>1}} to {"a"=>[]}' do
        table.insert(id: '1', document: '{"_id":1,"a":[]}')
        update = build_update(1, '$set' => { 'a.1.b' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => [nil, { 'b' => 1 }]))
      end

      it 'applies {"$set"=>{"a.1.b.1"=>1}} to {}' do
        table.insert(id: '1', document: '{"_id":1}')
        update = build_update(1, '$set' => { 'a.1.b.1' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => { '1' => { 'b' => { '1' => 1 } } }))
      end

      it 'applies {"$set"=>{"a.1.b.1"=>1}} to {"a"=>[]}' do
        table.insert(id: '1', document: '{"_id":1,"a":[]}')
        update = build_update(1, '$set' => { 'a.1.b.1' => 1 })
        update.apply('foo.bar' => table)

        expect(table.first).to include(document: Sequel.pg_jsonb('_id' => 1, 'a' => [nil, { 'b' => { '1' => 1 } }]))
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
