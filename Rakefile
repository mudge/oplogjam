require 'json'
require 'mongo'

namespace :spec do
  desc 'Generate RSpec examples for updates from test cases'
  task :generate_updates do
    document = { _id: 1 }
    test_cases = [
      [{}, { 'a' => 1, 'b' => 2 }],
      [{}, { '$set' => { 'a' => 1 } }],
      [{ 'a' => 0 }, { '$set' => { 'a' => 1 } }],
      [{}, { '$set' => { 'a' => 1, 'b' => 2 } }],
      [{ 'a' => 0, 'b' => 0 }, { '$set' => { 'a' => 1, 'b' => 2 } }],
      [{ 'a' => 0, 'b' => 0 }, { '$unset' => { 'a' => '' } }],
      [{ 'a' => 0, 'b' => 0 }, { '$unset' => { 'a' => '', 'b' => '' } }],
      [{ 'a' => 0, 'b' => 0 }, { '$unset' => { 'c' => '' } }],
      [{ 'a' => 0 }, { '$set' => { 'b' => 1 }, '$unset' => { 'a' => '' } }],
      [{}, { '$set' => { 'a.0' => 1 } }],
      [{ 'a' => [] }, { '$set' => { 'a.0' => 1 } }],
      [{ 'a' => {} }, { '$set' => { 'a.0' => 1 } }],
      [{ 'a' => [] }, { '$set' => { 'a.1' => 1 } }],
      [{}, { '$unset' => { 'a.0' => '' } }],
      [{ 'a' => [] }, { '$unset' => { 'a.0' => '' } }],
      [{ 'a' => [1] }, { '$unset' => { 'a.0' => '' } }],
      [{ 'a' => [1, 2] }, { '$unset' => { 'a.0' => '' } }],
      [{ 'a' => [1, 2] }, { '$unset' => { 'a.1' => '' } }],
      [{ 'a' => [1, 2] }, { '$unset' => { 'a.2' => '' } }],
      [{ 'a' => {} }, { '$unset' => { 'a.0' => '' } }],
      [{ 'a' => [] }, { '$unset' => { 'a.1' => '' } }],
      [{}, { '$set' => { 'a.b' => 1 } }],
      [{}, { '$set' => { 'a.b.c' => 1 } }],
      [{}, { '$set' => { 'a.b.c' => 1, 'a.d' => 2 } }],
      [{ 'a' => {} }, { '$set' => { 'a.b' => 1 } }],
      [{ 'a' => { 'b' => 0 } }, { '$set' => { 'a.b' => 1 } }],
      [{}, { '$set' => { 'a.1.b' => 1 } }],
      [{ 'a' => [] }, { '$set' => { 'a.1.b' => 1 } }],
      [{}, { '$set' => { 'a.1.b.1' => 1 } }],
      [{ 'a' => [] }, { '$set' => { 'a.1.b.1' => 1 } }]
    ]

    Mongo::Logger.logger.level = Logger::WARN
    db = Mongo::Client.new('mongodb://localhost:27017/foo')
    db[:bar].drop
    db[:bar].insert_one(document)

    test_cases.each do |(start, update)|
      db[:bar].replace_one(document, start)
      db[:bar].update_one(document, update)
      result = db[:bar].find(document).first

      puts <<-RUBY
        it 'applies #{update.inspect} to #{start.inspect}' do
          table.insert(id: '1', document: '#{document.merge(start).to_json}')
          update = build_update(1, #{update.inspect})
          update.apply('foo.bar' => table)

          expect(table.first).to include(document: Sequel.pg_jsonb(#{result.inspect}))
        end

      RUBY
    end
  end
end
