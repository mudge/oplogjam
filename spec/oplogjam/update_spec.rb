require 'bson'
require 'oplogjam'

module Oplogjam
  RSpec.describe Update do
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

    describe '#to_sql' do
      it 'returns SQL equivalent to the update' do
        Timecop.freeze(Time.utc(2001)) do
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

          expect(update.to_sql).to eq("UPDATE \"foo_bar\" SET \"document\" = jsonb_set(\"document\", ARRAY['bar'], '\"baz\"', true), \"updated_at\" = '2001-01-01 00:00:00.000000+0000' WHERE ((\"id\" = '{\"$oid\":\"583033a3643431ab5be6ec35\"}') AND (\"deleted_at\" IS NULL))")
        end
      end

      it 'supports nested field updates' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            ts: BSON::Timestamp.new(1_479_561_033, 1),
            t: 2,
            h: 3_511_341_713_062_188_019,
            v: 2,
            op: 'u',
            ns: 'foo.bar',
            o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
            o: BSON::Document.new('$set' => BSON::Document.new('bar.baz' => 'quux'))
          )
          update = described_class.from(bson)

          expect(update.to_sql).to eq("UPDATE \"foo_bar\" SET \"document\" = jsonb_set(jsonb_set(\"document\", ARRAY['bar'], coalesce((\"document\" #> ARRAY['bar']), '{}'::jsonb), true), ARRAY['bar','baz'], '\"quux\"', true), \"updated_at\" = '2001-01-01 00:00:00.000000+0000' WHERE ((\"id\" = '{\"$oid\":\"583033a3643431ab5be6ec35\"}') AND (\"deleted_at\" IS NULL))")
        end
      end

      it 'supports multiple nested field updates efficiently' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            ts: BSON::Timestamp.new(1_479_561_033, 1),
            t: 2,
            h: 3_511_341_713_062_188_019,
            v: 2,
            op: 'u',
            ns: 'foo.bar',
            o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
            o: BSON::Document.new('$set' => BSON::Document.new('a.b.c' => 1, 'a.b.d' => 2))
          )
          update = described_class.from(bson)

          expect(update.to_sql).to eq("UPDATE \"foo_bar\" SET \"document\" = jsonb_set(jsonb_set(jsonb_set(jsonb_set(\"document\", ARRAY['a'], coalesce((\"document\" #> ARRAY['a']), '{}'::jsonb), true), ARRAY['a','b'], coalesce((jsonb_set(\"document\", ARRAY['a'], coalesce((\"document\" #> ARRAY['a']), '{}'::jsonb), true) #> ARRAY['a','b']), '{}'::jsonb), true), ARRAY['a','b','c'], '1', true), ARRAY['a','b','d'], '2', true), \"updated_at\" = '2001-01-01 00:00:00.000000+0000' WHERE ((\"id\" = '{\"$oid\":\"583033a3643431ab5be6ec35\"}') AND (\"deleted_at\" IS NULL))")
        end
      end

      it 'supports deeply nested field updates' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            ts: BSON::Timestamp.new(1_479_561_033, 1),
            t: 2,
            h: 3_511_341_713_062_188_019,
            v: 2,
            op: 'u',
            ns: 'foo.bar',
            o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
            o: BSON::Document.new('$set' => BSON::Document.new('bar.baz.quux' => 'quuz'))
          )
          update = described_class.from(bson)

          expect(update.to_sql).to eq("UPDATE \"foo_bar\" SET \"document\" = jsonb_set(jsonb_set(jsonb_set(\"document\", ARRAY['bar'], coalesce((\"document\" #> ARRAY['bar']), '{}'::jsonb), true), ARRAY['bar','baz'], coalesce((jsonb_set(\"document\", ARRAY['bar'], coalesce((\"document\" #> ARRAY['bar']), '{}'::jsonb), true) #> ARRAY['bar','baz']), '{}'::jsonb), true), ARRAY['bar','baz','quux'], '\"quuz\"', true), \"updated_at\" = '2001-01-01 00:00:00.000000+0000' WHERE ((\"id\" = '{\"$oid\":\"583033a3643431ab5be6ec35\"}') AND (\"deleted_at\" IS NULL))")
        end
      end

      it 'supports nested field updates with indices' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            ts: BSON::Timestamp.new(1_479_561_033, 1),
            t: 2,
            h: 3_511_341_713_062_188_019,
            v: 2,
            op: 'u',
            ns: 'foo.bar',
            o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
            o: BSON::Document.new('$set' => BSON::Document.new('bar.baz.1' => 'quuz'))
          )
          update = described_class.from(bson)

          expect(update.to_sql).to eq("UPDATE \"foo_bar\" SET \"document\" = jsonb_set(jsonb_set(jsonb_set(\"document\", ARRAY['bar'], coalesce((\"document\" #> ARRAY['bar']), '{}'::jsonb), true), ARRAY['bar','baz'], coalesce((jsonb_set(\"document\", ARRAY['bar'], coalesce((\"document\" #> ARRAY['bar']), '{}'::jsonb), true) #> ARRAY['bar','baz']), '{}'::jsonb), true), ARRAY['bar','baz','1'], '\"quuz\"', true), \"updated_at\" = '2001-01-01 00:00:00.000000+0000' WHERE ((\"id\" = '{\"$oid\":\"583033a3643431ab5be6ec35\"}') AND (\"deleted_at\" IS NULL))")
        end
      end

      it 'supports multiple updates' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            ts: BSON::Timestamp.new(1_479_561_033, 1),
            t: 2,
            h: 3_511_341_713_062_188_019,
            v: 2,
            op: 'u',
            ns: 'foo.bar',
            o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
            o: BSON::Document.new('$set' => BSON::Document.new('bar' => 'baz', 'baz' => 'quux'))
          )
          update = described_class.from(bson)

          expect(update.to_sql).to eq("UPDATE \"foo_bar\" SET \"document\" = jsonb_set(jsonb_set(\"document\", ARRAY['bar'], '\"baz\"', true), ARRAY['baz'], '\"quux\"', true), \"updated_at\" = '2001-01-01 00:00:00.000000+0000' WHERE ((\"id\" = '{\"$oid\":\"583033a3643431ab5be6ec35\"}') AND (\"deleted_at\" IS NULL))")
        end
      end

      it 'supports unsetting fields' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            ts: BSON::Timestamp.new(1_479_561_033, 1),
            t: 2,
            h: 3_511_341_713_062_188_019,
            v: 2,
            op: 'u',
            ns: 'foo.bar',
            o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
            o: BSON::Document.new('$unset' => BSON::Document.new('bar' => ''))
          )
          update = described_class.from(bson)

          expect(update.to_sql).to eq("UPDATE \"foo_bar\" SET \"document\" = (\"document\" #- ARRAY['bar']), \"updated_at\" = '2001-01-01 00:00:00.000000+0000' WHERE ((\"id\" = '{\"$oid\":\"583033a3643431ab5be6ec35\"}') AND (\"deleted_at\" IS NULL))")
        end
      end

      it 'supports unsetting multiple fields' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            ts: BSON::Timestamp.new(1_479_561_033, 1),
            t: 2,
            h: 3_511_341_713_062_188_019,
            v: 2,
            op: 'u',
            ns: 'foo.bar',
            o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
            o: BSON::Document.new('$unset' => BSON::Document.new('bar' => '', 'baz' => ''))
          )
          update = described_class.from(bson)

          expect(update.to_sql).to eq("UPDATE \"foo_bar\" SET \"document\" = ((\"document\" #- ARRAY['bar']) #- ARRAY['baz']), \"updated_at\" = '2001-01-01 00:00:00.000000+0000' WHERE ((\"id\" = '{\"$oid\":\"583033a3643431ab5be6ec35\"}') AND (\"deleted_at\" IS NULL))")
        end
      end

      it 'supports setting and unsetting at the same time' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            ts: BSON::Timestamp.new(1_479_561_033, 1),
            t: 2,
            h: 3_511_341_713_062_188_019,
            v: 2,
            op: 'u',
            ns: 'foo.bar',
            o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
            o: BSON::Document.new('$set' => BSON::Document.new('bar' => 'quux'), '$unset' => BSON::Document.new('baz' => ''))
          )
          update = described_class.from(bson)

          expect(update.to_sql).to eq("UPDATE \"foo_bar\" SET \"document\" = (jsonb_set(\"document\", ARRAY['bar'], '\"quux\"', true) #- ARRAY['baz']), \"updated_at\" = '2001-01-01 00:00:00.000000+0000' WHERE ((\"id\" = '{\"$oid\":\"583033a3643431ab5be6ec35\"}') AND (\"deleted_at\" IS NULL))")
        end
      end

      it 'translates unsetting specific array indices into setting nulls' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            ts: BSON::Timestamp.new(1_479_561_033, 1),
            t: 2,
            h: 3_511_341_713_062_188_019,
            v: 2,
            op: 'u',
            ns: 'foo.bar',
            o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
            o: BSON::Document.new('$unset' => BSON::Document.new('baz.1' => ''))
          )
          update = described_class.from(bson)

          expect(update.to_sql).to eq("UPDATE \"foo_bar\" SET \"document\" = jsonb_set(\"document\", ARRAY['baz','1'], 'null', true), \"updated_at\" = '2001-01-01 00:00:00.000000+0000' WHERE ((\"id\" = '{\"$oid\":\"583033a3643431ab5be6ec35\"}') AND (\"deleted_at\" IS NULL))")
        end
      end

      it 'supports replacement' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            ts: BSON::Timestamp.new(1_479_561_033, 1),
            t: 2,
            h: 3_511_341_713_062_188_019,
            v: 2,
            op: 'u',
            ns: 'foo.bar',
            o2: BSON::Document.new(_id: BSON::ObjectId('583033a3643431ab5be6ec35')),
            o: BSON::Document.new(:_id => BSON::ObjectId('583033a3643431ab5be6ec35'), 'bar' => 'baz')
          )
          update = described_class.from(bson)

          expect(update.to_sql).to eq("UPDATE \"foo_bar\" SET \"document\" = '{\"_id\":{\"$oid\":\"583033a3643431ab5be6ec35\"},\"bar\":\"baz\"}'::jsonb, \"updated_at\" = '2001-01-01 00:00:00.000000+0000' WHERE ((\"id\" = '{\"$oid\":\"583033a3643431ab5be6ec35\"}') AND (\"deleted_at\" IS NULL))")
        end
      end
    end
  end
end
