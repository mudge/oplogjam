require 'oplogjam'

module Oplogjam
  RSpec.describe Insert do
    let(:postgres) { Sequel.connect('postgres:///oplogjam_test') }
    let(:schema) { Schema.new(postgres) }
    let(:table) { postgres.from(:bar) }

    before(:example, :database) do
      postgres.extension :pg_array, :pg_json
      schema.create_table(:bar)
      schema.add_indexes(:bar)
    end

    after(:example, :database) do
      table.truncate
    end

    describe '.from' do
      it 'converts a BSON insert into an Insert' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_496_414_570, 11),
          t: 14,
          h: -3_028_027_288_268_436_781,
          v: 2,
          op: 'i',
          ns: 'foo.bar',
          o: BSON::Document.new(
            _id: BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            baz: 'quux'
          )
        )

        expect(described_class.from(bson)).to be_a(described_class)
      end

      it 'raises an error if the document is missing' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_496_414_570, 11),
          t: 14,
          h: -3_028_027_288_268_436_781,
          v: 2,
          op: 'i',
          ns: 'foo.bar'
        )

        expect { described_class.from(bson) }.to raise_error(InvalidInsert)
      end
    end

    describe '#namespace' do
      it 'returns the namespace' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_496_414_570, 11),
          t: 14,
          h: -3_028_027_288_268_436_781,
          v: 2,
          op: 'i',
          ns: 'foo.bar',
          o: BSON::Document.new(
            _id: BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            baz: 'quux'
          )
        )
        insert = described_class.from(bson)

        expect(insert.namespace).to eq('foo.bar')
      end
    end

    describe '#id' do
      it 'returns a unique identifier for the insert' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_496_414_570, 11),
          t: 14,
          h: -3_028_027_288_268_436_781,
          v: 2,
          op: 'i',
          ns: 'foo.bar',
          o: BSON::Document.new(
            _id: BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            baz: 'quux'
          )
        )
        insert = described_class.from(bson)

        expect(insert.id).to eq(-3_028_027_288_268_436_781)
      end
    end

    describe '#document' do
      it 'returns the document being inserted' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_496_414_570, 11),
          t: 14,
          h: -3_028_027_288_268_436_781,
          v: 2,
          op: 'i',
          ns: 'foo.bar',
          o: BSON::Document.new(
            _id: BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            baz: 'quux'
          )
        )
        insert = described_class.from(bson)

        expect(insert.document).to eq(
          BSON::Document.new(
            _id: BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            baz: 'quux'
          )
        )
      end
    end

    describe '#timestamp' do
      it 'returns the time of the operation as a Time' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_496_414_570, 11),
          t: 14,
          h: -3_028_027_288_268_436_781,
          v: 2,
          op: 'i',
          ns: 'foo.bar',
          o: BSON::Document.new(
            _id: BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            baz: 'quux'
          )
        )
        insert = described_class.from(bson)

        expect(insert.timestamp).to eq(Time.at(1_496_414_570, 11))
      end
    end

    describe '#ts' do
      it 'returns the raw underlying BSON timestamp' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_496_414_570, 11),
          t: 14,
          h: -3_028_027_288_268_436_781,
          v: 2,
          op: 'i',
          ns: 'foo.bar',
          o: BSON::Document.new(
            _id: BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            baz: 'quux'
          )
        )
        insert = described_class.from(bson)

        expect(insert.ts).to eq(BSON::Timestamp.new(1_496_414_570, 11))
      end
    end

    describe '#==' do
      it 'is equal to another operation with the same ID' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_496_414_570, 11),
          t: 14,
          h: -3_028_027_288_268_436_781,
          v: 2,
          op: 'i',
          ns: 'foo.bar',
          o: BSON::Document.new(
            _id: BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            baz: 'quux'
          )
        )
        another_bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_496_414_570, 11),
          t: 14,
          h: -3_028_027_288_268_436_781,
          v: 2,
          op: 'i',
          ns: 'foo.bar',
          o: BSON::Document.new(
            _id: BSON::ObjectId('593bac55da605b0dbf3b25a5'),
            baz: 'quux'
          )
        )
        insert = described_class.from(bson)
        another_insert = described_class.from(another_bson)

        expect(insert).to eq(another_insert)
      end
    end

    describe '#apply', :database do
      it 'inserts a document as JSONB into the corresponding table' do
        insert = build_insert

        expect { insert.apply('foo.bar' => table) }.to change { table.count }.by(1)
      end

      it 'extracts the ID of the document' do
        insert = build_insert(_id: 1)
        insert.apply('foo.bar' => table)

        expect(table.first).to include(id: 1)
      end

      it 'can store IDs of different types in the same table' do
        insert1 = build_insert(_id: 1)
        insert2 = build_insert(_id: '1')

        expect {
          insert1.apply('foo.bar' => table)
          insert2.apply('foo.bar' => table)
        }.to change { table.count }.by(2)
      end

      it 'stores the original document as JSONB' do
        insert = build_insert(_id: 1, baz: 'quux')
        insert.apply('foo.bar' => table)

        expect(table.get(Sequel.pg_jsonb_op(:document).get_text('baz'))).to eq('quux')
      end

      it 'stores BSON Object IDs as equivalent JSON objects' do
        insert = build_insert(_id: BSON::ObjectId.from_string('59b3fbb4c97ba6c8a58a2d0c'))
        insert.apply('foo.bar' => table)

        expect(table.get(:id)).to eq('$oid' => '59b3fbb4c97ba6c8a58a2d0c')
      end

      it 'can reuse IDs if a deleted record exists' do
        insert = build_insert(_id: 1)
        insert.apply('foo.bar' => table)
        table.where(id: '1').update(deleted_at: Time.now.utc)

        expect { insert.apply('foo.bar' => table) }.to change { table.count }.by(1)
      end

      it 'ignores inserts to unmapped tables' do
        insert = build_insert(_id: 1)

        expect { insert.apply('foo.baz' => table) }.not_to change { table.count }
      end

      it 'strips null bytes from the document' do
        insert = build_insert(_id: 1, baz: "quux\x00")
        insert.apply('foo.bar' => table)

        expect(table.get(Sequel.pg_jsonb_op(:document).get_text('baz'))).to eq('quux')
      end

      it 'updates any existing, non-deleted records with the same ID' do
        table.insert(id: '1', document: '{}', created_at: Time.now.utc, updated_at: Time.now.utc)
        insert = build_insert(_id: 1, baz: 'quux')
        insert.apply('foo.bar' => table)

        expect(table.get(Sequel.pg_jsonb_op(:document).get_text('baz'))).to eq('quux')
      end

      it 'updates updated_at for existing records' do
        Timecop.freeze(Time.new(2001, 1, 1, 0, 0, 0)) do
          table.insert(id: '1', document: '{}', created_at: Time.now.utc, updated_at: Time.now.utc)
          insert = build_insert(_id: 1, baz: 'quux')
          insert.apply('foo.bar' => table)

          expect(table.first).to include(updated_at: Time.new(2001, 1, 1, 0, 0, 0))
        end
      end

      it 'only updates non-deleted records with the same ID' do
        table.insert(id: '1', document: '{"a":1}', created_at: Time.now.utc, updated_at: Time.now.utc, deleted_at: Time.now.utc)
        insert = build_insert(_id: 1, a: 2)
        insert.apply('foo.bar' => table)

        expect(table.where(id: '1').exclude(deleted_at: nil).get(Sequel.pg_jsonb_op(:document).get_text('a'))).to eq('1')
      end
    end

    def build_insert(attributes = { _id: 1, baz: 'quux' })
      bson = BSON::Document.new(
        ts: BSON::Timestamp.new(1_496_414_570, 11),
        t: 14,
        h: -3_028_027_288_268_436_781,
        v: 2,
        op: 'i',
        ns: 'foo.bar',
        o: BSON::Document.new(attributes)
      )

      described_class.from(bson)
    end
  end
end
