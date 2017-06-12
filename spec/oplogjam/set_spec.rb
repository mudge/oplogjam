require 'oplogjam'

module Oplogjam
  RSpec.describe Set do
    describe '.update' do
      it 'converts a simple $set into SQL' do
        sql = described_class.from('a' => 1).update(Sequel.pg_jsonb(:document))

        expect(sql).to eq(Sequel.pg_jsonb(:document).set(%w'a', '1'))
      end

      it 'converts several simple $sets into SQL' do
        sql = described_class.from('a' => 1, 'b' => 2).update(Sequel.pg_jsonb(:document))

        expect(sql).to eq(Sequel.pg_jsonb(:document).set(%w'a', '1').set(%w'b', '2'))
      end

      it 'converts a single nested $set into SQL' do
        sql = described_class.from('a.b' => 1).update(Sequel.pg_jsonb(:document))

        expect(sql).to eq(
          Sequel
            .pg_jsonb(:document)
            .set(%w'a', Sequel.function(:coalesce, Sequel.pg_jsonb(:document)[%w'a'], Sequel.pg_jsonb({})))
            .set(%w'a b', '1')
        )
      end

      it 'converts a complex $set tree into a SQL expression' do
        sql = described_class.from('a.b.c' => 1, 'a.b.d' => 2, 'a.e' => 3).update(Sequel.pg_jsonb(:document))

        document = Sequel.pg_jsonb(:document)
        a_root = document.set(
          %w'a',
          Sequel.function(:coalesce, document[%w'a'], Sequel.pg_jsonb({}))
        )

        expect(sql).to eq(
          a_root
            .set(
              %w'a b',
              Sequel.function(:coalesce, a_root[%w'a b'], Sequel.pg_jsonb({}))
            )
            .set(%w'a b c', '1')
            .set(%w'a b d', '2')
            .set(%w'a e', '3')
        )
      end
    end
  end
end
