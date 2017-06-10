require 'bson'
require 'oplogjam'

module Oplogjam
  RSpec.describe Operation do
    describe '.from' do
      it 'converts BSON no-ops into Noop' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_419_535, 1),
          h: -2_135_725_856_567_446_411,
          v: 2,
          op: 'n',
          ns: '',
          o: BSON::Document.new(msg: 'initiating set')
        )

        expect(described_class.from(bson)).to be_a(Noop)
      end

      it 'converts BSON inserts into Inserts' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_496_414_570, 11),
          t: 14,
          h: -3_028_027_288_268_436_781,
          v: 2,
          op: 'i',
          ns: 'foo.bar',
          o: BSON::Document.new(_id: 1, baz: 'quux')
        )

        expect(described_class.from(bson)).to be_a(Insert)
      end

      it 'converts BSON updates into Updates' do
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

        expect(described_class.from(bson)).to be_a(Update)
      end

      it 'converts BSON deletes into Deletes' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_421_186, 1),
          t: 1,
          h: -5_457_382_347_563_537_847,
          v: 2,
          op: 'd',
          ns: 'foo.bar',
          o: BSON::Document.new(_id: BSON::ObjectId('582e287cfedf6fb051b2efdf'))
        )

        expect(described_class.from(bson)).to be_a(Delete)
      end

      it 'converts BSON commands into Commands' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_420_028, 1),
          t: 1,
          h: -1_789_557_309_812_000_233,
          v: 2,
          op: 'c',
          ns: 'foo.$cmd',
          o: BSON::Document.new(create: 'bar')
        )

        expect(described_class.from(bson)).to be_a(Command)
      end

      it 'converts BSON applyOps into ApplyOps' do
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

        expect(described_class.from(bson)).to be_a(Oplogjam::ApplyOps)
      end

      it 'raises an error if given an unknown operation' do
        bson = BSON::Document.new(not: 'an operation')

        expect { described_class.from(bson) }.to raise_error(InvalidOperation)
      end
    end
  end
end
