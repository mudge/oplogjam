require 'bson'
require 'oplogjam'

module Oplogjam
  RSpec.describe Command do
    describe '.from' do
      it 'converts a BSON command into a Command' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_420_028, 1),
          t: 1,
          h: -1_789_557_309_812_000_233,
          v: 2,
          op: 'c',
          ns: 'foo.$cmd',
          o: BSON::Document.new(create: 'bar')
        )

        expect(described_class.from(bson)).to be_a(described_class)
      end

      it 'raises an error if the command is missing' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_420_028, 1),
          t: 1,
          h: -1_789_557_309_812_000_233,
          v: 2,
          op: 'c',
          ns: 'foo.$cmd'
        )

        expect { described_class.from(bson) }.to raise_error(InvalidCommand)
      end
    end

    describe '#id' do
      it 'returns a unique identifier for the command' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_420_028, 1),
          t: 1,
          h: -1_789_557_309_812_000_233,
          v: 2,
          op: 'c',
          ns: 'foo.$cmd',
          o: BSON::Document.new(create: 'bar')
        )
        command = described_class.from(bson)

        expect(command.id).to eq(-1_789_557_309_812_000_233)
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
          o: BSON::Document.new(create: 'bar')
        )
        command = described_class.from(bson)

        expect(command.namespace).to eq('foo.$cmd')
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
          o: BSON::Document.new(create: 'bar')
        )
        command = described_class.from(bson)

        expect(command.timestamp).to eq(Time.at(1_479_420_028, 1))
      end
    end

    describe '#command' do
      it 'returns the command' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_420_028, 1),
          t: 1,
          h: -1_789_557_309_812_000_233,
          v: 2,
          op: 'c',
          ns: 'foo.$cmd',
          o: BSON::Document.new(create: 'bar')
        )
        command = described_class.from(bson)

        expect(command.command).to eq(BSON::Document.new(create: 'bar'))
      end
    end
  end
end
