require 'bson'
require 'oplogjam'

module Oplogjam
  RSpec.describe Delete do
    describe '.from' do
      it 'converts a BSON delete into a Delete' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_421_186, 1),
          t: 1,
          h: -5_457_382_347_563_537_847,
          v: 2,
          op: 'd',
          ns: 'foo.bar',
          o: BSON::Document.new(_id: BSON::ObjectId('582e287cfedf6fb051b2efdf'))
        )

        expect(described_class.from(bson)).to be_a(described_class)
      end

      it 'raises an error if the query is missing' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_421_186, 1),
          t: 1,
          h: -5_457_382_347_563_537_847,
          v: 2,
          op: 'd',
          ns: 'foo.bar'
        )

        expect { described_class.from(bson) }.to raise_error(InvalidDelete)
      end
    end

    describe '#id' do
      it 'returns a unique identifier for the delete' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_421_186, 1),
          t: 1,
          h: -5_457_382_347_563_537_847,
          v: 2,
          op: 'd',
          ns: 'foo.bar',
          o: BSON::Document.new(_id: BSON::ObjectId('582e287cfedf6fb051b2efdf'))
        )
        delete = described_class.from(bson)

        expect(delete.id).to eq(-5_457_382_347_563_537_847)
      end
    end

    describe '#timestamp' do
      it 'returns the timestamp as a Time' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_421_186, 1),
          t: 1,
          h: -5_457_382_347_563_537_847,
          v: 2,
          op: 'd',
          ns: 'foo.bar',
          o: BSON::Document.new(_id: BSON::ObjectId('582e287cfedf6fb051b2efdf'))
        )
        delete = described_class.from(bson)

        expect(delete.timestamp).to eq(Time.at(1_479_421_186, 1))
      end
    end

    describe '#namespace' do
      it 'returns the namespace' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_421_186, 1),
          t: 1,
          h: -5_457_382_347_563_537_847,
          v: 2,
          op: 'd',
          ns: 'foo.bar',
          o: BSON::Document.new(_id: BSON::ObjectId('582e287cfedf6fb051b2efdf'))
        )
        delete = described_class.from(bson)

        expect(delete.namespace).to eq('foo.bar')
      end
    end

    describe '#query' do
      it 'returns the query' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_421_186, 1),
          t: 1,
          h: -5_457_382_347_563_537_847,
          v: 2,
          op: 'd',
          ns: 'foo.bar',
          o: BSON::Document.new(_id: BSON::ObjectId('582e287cfedf6fb051b2efdf'))
        )
        delete = described_class.from(bson)

        expect(delete.query).to eq(BSON::Document.new(_id: BSON::ObjectId('582e287cfedf6fb051b2efdf')))
      end
    end

    describe '#to_sql' do
      it 'returns SQL to soft delete this record' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            ts: BSON::Timestamp.new(1_479_421_186, 1),
            t: 1,
            h: -5_457_382_347_563_537_847,
            v: 2,
            op: 'd',
            ns: 'foo.bar',
            o: BSON::Document.new(_id: BSON::ObjectId('582e287cfedf6fb051b2efdf'))
          )
          delete = described_class.from(bson)

          expect(delete.to_sql).to eq("UPDATE \"foo_bar\" SET \"deleted_at\" = '2001-01-01 00:00:00.000000+0000' WHERE ((\"id\" = '{\"$oid\":\"582e287cfedf6fb051b2efdf\"}') AND (\"deleted_at\" IS NULL))")
        end
      end
    end
  end
end
