require 'bson'
require 'oplogjam/command'

module Oplogjam
  RSpec.describe Command do
    describe '#command' do
      it 'returns the command' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479420028, 1),
          :t => 1,
          :h => -1789557309812000233,
          :v => 2,
          :op => 'c',
          :ns => 'foo.$cmd',
          :o => BSON::Document.new(:create => 'bar')
        )
        command = described_class.new(bson)

        expect(command.command).to eq(BSON::Document.new(:create => 'bar'))
      end
    end
  end
end
