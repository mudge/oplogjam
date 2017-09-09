require 'oplogjam'

module Oplogjam
  RSpec.describe Sanitizer do
    describe '.sanitize' do
      it 'strips null bytes from strings' do
        expect(described_class.sanitize("Foo\x00bar\x00")).to eq('Foobar')
      end

      it 'does not strip escaped null bytes from strings' do
        expect(described_class.sanitize('Foo\u0000bar')).to eq('Foo\u0000bar')
      end

      it 'strips null bytes from array elements' do
        expect(described_class.sanitize(["Foo\x00", "\x00Bar"])).to eq(['Foo', 'Bar'])
      end

      it 'strips null bytes from JSONB array elements' do
        expect(described_class.sanitize(Sequel::Postgres::JSONBArray.new(["Foo\x00", "\x00Bar"]))).to eq(['Foo', 'Bar'])
      end

      it 'strips null bytes from hash values' do
        expect(described_class.sanitize('name' => "Foo\x00")).to eq('name' => 'Foo')
      end

      it 'strips null bytes from hash keys' do
        expect(described_class.sanitize("\x00name" => 'Foo')).to eq('name' => 'Foo')
      end

      it 'strips null bytes from JSONB hash elements' do
        expect(described_class.sanitize(Sequel::Postgres::JSONBHash.new('name' => "Foo\x00"))).to eq('name' => 'Foo')
      end
    end
  end
end
