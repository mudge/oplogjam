require 'bson'
require 'oplogjam/noop'

module Oplogjam
  RSpec.describe Noop do
    describe '#message' do
      it 'returns the message' do
        bson = BSON::Document.new(
          :ts => BSON::Timestamp.new(1479419535, 1),
          :h => -2135725856567446411,
          :v => 2,
          :op => 'n',
          :ns => '',
          :o => BSON::Document.new(:msg => 'initiating set')
        )
        noop = described_class.new(bson)

        expect(noop.message).to eq('initiating set')
      end
    end
  end
end
