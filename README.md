# Oplogjam

An experiment in writing a "safe" MongoDB oplog tailer that converts documents
to PostgreSQL JSONB in Ruby.

## Schema

This currently expects to write a MongoDB collection `bar` from the database
`foo` to a table in PostgreSQL called `foo_bar` with the following schema:

```
                             Table "public.foo_bar"
   Column   |            Type             |              Modifiers
------------+-----------------------------+-------------------------------------
 uuid       | uuid                        | not null default uuid_generate_v4()
 id         | jsonb                       | not null
 document   | jsonb                       | not null
 created_at | timestamp without time zone | not null
 updated_at | timestamp without time zone |
 deleted_at | timestamp without time zone |
Indexes:
    "foo_bar_pkey" PRIMARY KEY, btree (uuid)
    "foo_bar_id_deleted_at_key" UNIQUE CONSTRAINT, btree (id, deleted_at)
    "foo_bar_id_index" UNIQUE, btree (id) WHERE deleted_at IS NULL
```

## API Documentation

### `Oplogjam::Oplog.new(client)`

```ruby
mongo = Mongo::Client.new('mongodb://localhost')
Oplogjam::Oplog.new(mongo)
```

Return a new `Oplog` for the given `Mongo::Client` `client` connected to a replica set.

### `Oplogjam::Oplogjam#operations([query])`

```ruby
oplog.operations.each do |operation|
  # Do something with operation
end

oplog.operations('ts' => { '$gt' => BSON::Timestamp.new(123456, 1) })
```

Return an infinite `Enumerator` yielding `Operation`s from the `Oplog` with an optional MongoDB query which will filter the underlying oplog.

### `Oplogjam::Operation.from(bson)`

```ruby
Oplogjam::Operation.from(document)
```

Convert a BSON document representing a MongoDB oplog operation into a corresponding Ruby object:

* `Oplogjam::Noop`
* `Oplogjam::Insert`
* `Oplogjam::Update`
* `Oplogjam::Delete`
* `Oplogjam::ApplyOps`
* `Oplogjam::Command`

Raises a `Oplogjam::InvalidOperation` if the type of operation is not recognised.

### `Oplogjam::Noop.from(bson)`

```ruby
Oplogjam::Noop.from(document)
```

Convert a BSON document representing a MongoDB oplog no-op into an `Oplogjam::Noop` instance.

### `Oplogjam::Noop#message`

```ruby
noop.message
#=> "initiating set"
```

Return the internal message of the no-op.

### `Oplogjam::Noop#id`

```ruby
noop.id
#=> -2135725856567446411
```

Return the internal, unique identifier for the no-op.

### `Oplogjam::Noop#timestamp`

```ruby
noop.timestamp
#=> 2017-09-09 16:11:18 +0100
```

Return the time of the no-op as a `Time`.

### `Oplogjam::Noop#ts`

```ruby
noop.ts
#=> #<BSON::Timestamp:0x007fcadfa44500 @increment=1, @seconds=1479419535>
```

Return the raw, underlying BSON Timestamp of the no-op.

### `Oplogjam::Noop#==(other)`

```ruby
noop == other_noop
#=> false
```

Compares the identifiers of two no-ops and returns true if they are equal.

### `Oplogjam::Noop#apply(mapping)`

```ruby
noop.apply(mapping)
```

Apply this no-op to a mapping of MongoDB namespaces (e.g. `foo.bar`) to Sequel datasets representing PostgreSQL tables. As no-ops do nothing, this performs no operation.

## License

Copyright © 2017 Paul Mucur.

Distributed under the MIT License.
