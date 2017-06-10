require 'bson'
require 'oplogjam'

module Oplogjam
  RSpec.describe ApplyOps do
    describe '.from' do
      it 'converts a BSON applyOps to an ApplyOps' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_420_028, 1),
          t: 1,
          h: -1_789_557_309_812_000_233,
          v: 2,
          op: 'c',
          ns: 'foo.$cmd',
          o: BSON::Document.new(
            applyOps: [
              BSON::Document.new(
                ts: BSON::Timestamp.new(1_496_414_570, 11),
                t: 14,
                h: -3_028_027_288_268_436_781,
                v: 2,
                op: 'i',
                ns: 'foo.bar',
                o: BSON::Document.new(_id: 1, baz: 'quux')
              )
            ]
          )
        )

        expect(described_class.from(bson)).to be_a(described_class)
      end

      it 'raises an error if the operations are missing' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_420_028, 1),
          t: 1,
          h: -1_789_557_309_812_000_233,
          v: 2,
          op: 'c',
          ns: 'foo.$cmd'
        )

        expect { described_class.from(bson) }.to raise_error(InvalidApplyOps)
      end
    end

    describe '#timestamp' do
      it 'returns the timestamp as a Time' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_420_028, 1),
          t: 1,
          h: -1_789_557_309_812_000_233,
          v: 2,
          op: 'c',
          ns: 'foo.$cmd',
          o: BSON::Document.new(
            applyOps: [
              BSON::Document.new(
                ts: BSON::Timestamp.new(1_496_414_570, 11),
                t: 14,
                h: -3_028_027_288_268_436_781,
                v: 2,
                op: 'i',
                ns: 'foo.bar',
                o: BSON::Document.new(_id: 1, baz: 'quux')
              )
            ]
          )
        )
        apply_ops = described_class.from(bson)

        expect(apply_ops.timestamp).to eq(Time.at(1_479_420_028, 1))
      end
    end

    describe '#namespace' do
      it 'returns the namespace' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_420_028, 1),
          t: 1,
          h: -1_789_557_309_812_000_233,
          v: 2,
          op: 'c',
          ns: 'foo.$cmd',
          o: BSON::Document.new(
            applyOps: [
              BSON::Document.new(
                ts: BSON::Timestamp.new(1_496_414_570, 11),
                t: 14,
                h: -3_028_027_288_268_436_781,
                v: 2,
                op: 'i',
                ns: 'foo.bar',
                o: BSON::Document.new(_id: 1, baz: 'quux')
              )
            ]
          )
        )
        apply_ops = described_class.from(bson)

        expect(apply_ops.namespace).to eq('foo.$cmd')
      end
    end

    describe '#id' do
      it 'returns a unique identifier for the operation' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_420_028, 1),
          t: 1,
          h: -1_789_557_309_812_000_233,
          v: 2,
          op: 'c',
          ns: 'foo.$cmd',
          o: BSON::Document.new(
            applyOps: [
              BSON::Document.new(
                ts: BSON::Timestamp.new(1_496_414_570, 11),
                t: 14,
                h: -3_028_027_288_268_436_781,
                v: 2,
                op: 'i',
                ns: 'foo.bar',
                o: BSON::Document.new(_id: 1, baz: 'quux')
              )
            ]
          )
        )
        apply_ops = described_class.from(bson)

        expect(apply_ops.id).to eq(-1_789_557_309_812_000_233)
      end
    end

    describe '#operations' do
      it 'returns the operations' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_420_028, 1),
          t: 1,
          h: -1_789_557_309_812_000_233,
          v: 2,
          op: 'c',
          ns: 'foo.$cmd',
          o: BSON::Document.new(
            applyOps: [
              BSON::Document.new(
                ts: BSON::Timestamp.new(1_496_414_570, 11),
                t: 14,
                h: -3_028_027_288_268_436_781,
                v: 2,
                op: 'i',
                ns: 'foo.bar',
                o: BSON::Document.new(_id: 1, baz: 'quux')
              )
            ]
          )
        )
        apply_ops = described_class.from(bson)
        insert = Insert.from(
          BSON::Document.new(
            ts: BSON::Timestamp.new(1_496_414_570, 11),
            t: 14,
            h: -3_028_027_288_268_436_781,
            v: 2,
            op: 'i',
            ns: 'foo.bar',
            o: BSON::Document.new(_id: 1, baz: 'quux')
          )
        )

        expect(apply_ops.operations).to contain_exactly(insert)
      end
    end

    describe '#to_sql' do
      it 'returns the SQL of each operation' do
        Timecop.freeze(Time.utc(2001)) do
          bson = BSON::Document.new(
            ts: BSON::Timestamp.new(1_479_420_028, 1),
            t: 1,
            h: -1_789_557_309_812_000_233,
            v: 2,
            op: 'c',
            ns: 'foo.$cmd',
            o: BSON::Document.new(
              applyOps: [
                BSON::Document.new(
                  ts: BSON::Timestamp.new(1_496_414_570, 11),
                  t: 14,
                  h: -3_028_027_288_268_436_781,
                  v: 2,
                  op: 'i',
                  ns: 'foo.bar',
                  o: BSON::Document.new(_id: 1, baz: 'quux')
                )
              ]
            )
          )
          apply_ops = described_class.from(bson)

          expect(apply_ops.to_sql).to eq("INSERT INTO \"foo_bar\" (\"id\", \"document\", \"created_at\", \"updated_at\") VALUES ('1', '{\"_id\":1,\"baz\":\"quux\"}'::jsonb, '2001-01-01 00:00:00.000000+0000', '2001-01-01 00:00:00.000000+0000')")
        end
      end
    end
  end
end
