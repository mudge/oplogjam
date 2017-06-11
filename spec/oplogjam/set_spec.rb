require 'oplogjam'

module Oplogjam
  RSpec.describe Set do
    describe '.from' do
      it 'creates a tree from a $set operation' do
        set = described_class.from('a.b.c' => 1, 'a.b.d' => 2, 'a.e' => 3, 'b.f' => 4)

        expect(set).to eq(
          described_class.new(
            %w'a' => {
              %w'a b' => {
                %w'a b c' => 1,
                %w'a b d' => 2
              },
              %w'a e' => 3
            },
            %w'b' => {
              %w'b f' => 4
            }
          )
        )
      end
    end

    describe '.convert' do
      it 'converts a simple $set into SQL' do
        sql = described_class.new(%w'a' => 1).to_sql(Sequel.pg_jsonb(:document))

        expect(sql).to eq(Sequel.pg_jsonb(:document).set(%w'a', '1'))
      end

      it 'converts several simple $sets into SQL' do
        sql = described_class.new(%w'a' => 1, %w'b' => 2).to_sql(Sequel.pg_jsonb(:document))

        expect(sql).to eq(Sequel.pg_jsonb(:document).set(%w'a', '1').set(%w'b', '2'))
      end

      it 'converts a single nested $set into SQL' do
        sql = described_class.new(%w'a' => { %w'a b' => 1 }).to_sql(Sequel.pg_jsonb(:document))

        expect(sql).to eq(
          Sequel
            .pg_jsonb(:document)
            .set(%w'a', Sequel.function(:coalesce, Sequel.pg_jsonb(:document)[%w'a'], Sequel.pg_jsonb({})))
            .set(%w'a b', '1')
        )
      end

      it 'converts a complex $set tree into a SQL expression' do
        sql = described_class.new(
          %w'a' => {
            %w'a b' => {
              %w'a b c' => 1,
              %w'a b d' => 2
            },
            %w'a e' => 3
          }
        ).to_sql(Sequel.pg_jsonb(:document))

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
