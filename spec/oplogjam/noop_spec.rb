require 'bson'
require 'oplogjam'

module Oplogjam
  RSpec.describe Noop do
    describe '.from' do
      it 'converts a BSON no-op into an Noop' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_419_535, 1),
          h: -2_135_725_856_567_446_411,
          v: 2,
          op: 'n',
          ns: '',
          o: BSON::Document.new(msg: 'initiating set')
        )

        expect(described_class.from(bson)).to be_a(described_class)
      end
    end

    describe '#message' do
      it 'returns the message' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_419_535, 1),
          h: -2_135_725_856_567_446_411,
          v: 2,
          op: 'n',
          ns: '',
          o: BSON::Document.new(msg: 'initiating set')
        )
        noop = described_class.from(bson)

        expect(noop.message).to eq('initiating set')
      end

      it 'returns an empty string if the message is not set' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_419_535, 1),
          h: -2_135_725_856_567_446_411,
          v: 2,
          op: 'n',
          ns: '',
          o: BSON::Document.new
        )
        noop = described_class.from(bson)

        expect(noop.message).to be_empty
      end
    end

    describe '#id' do
      it 'returns a unique identifier for the operation' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_419_535, 1),
          h: -2_135_725_856_567_446_411,
          v: 2,
          op: 'n',
          ns: '',
          o: BSON::Document.new(msg: 'initiating set')
        )
        noop = described_class.from(bson)

        expect(noop.id).to eq(-2_135_725_856_567_446_411)
      end
    end

    describe '#timestamp' do
      it 'returns the time of the operation as a Time' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_419_535, 1),
          h: -2_135_725_856_567_446_411,
          v: 2,
          op: 'n',
          ns: '',
          o: BSON::Document.new(msg: 'initiating set')
        )
        noop = described_class.from(bson)

        expect(noop.timestamp).to eq(Time.at(1_479_419_535, 1))
      end
    end

    describe '#ts' do
      it 'returns the raw underlying BSON timestamp' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_419_535, 1),
          h: -2_135_725_856_567_446_411,
          v: 2,
          op: 'n',
          ns: '',
          o: BSON::Document.new(msg: 'initiating set')
        )
        noop = described_class.from(bson)

        expect(noop.ts).to eq(BSON::Timestamp.new(1_479_419_535, 1))
      end
    end

    describe '#==' do
      it 'is equal to another noop with the same ID' do
        bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_419_535, 1),
          h: -2_135_725_856_567_446_411,
          v: 2,
          op: 'n',
          ns: '',
          o: BSON::Document.new(msg: 'initiating set')
        )
        another_bson = BSON::Document.new(
          ts: BSON::Timestamp.new(1_479_419_535, 1),
          h: -2_135_725_856_567_446_411,
          v: 2,
          op: 'n',
          ns: '',
          o: BSON::Document.new(msg: 'initiating set')
        )
        noop = described_class.from(bson)
        another_noop = described_class.from(another_bson)

        expect(noop).to eq(another_noop)
      end
    end
  end
end
