# Oplogjam

An experiment in writing a "safe" MongoDB oplog tailer that converts documents
to PostgreSQL JSONB in Ruby.

## Requirements

* A [MongoDB replica set](https://docs.mongodb.com/manual/replication/);
* [PostgreSQL 9.5](https://www.postgresql.org/docs/9.5/static/release-9-5.html) or newer (for [`INSERT ON CONFLICT`](https://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT) support).

## Mapping collections to tables

This library expects to replay operations on MongoDB collections on equivalent PostgreSQL tables.

In order to do this, you must provide a _mapping_ between MongoDB namespaces (e.g. a database and collection name such as `foo.bar` for a collection `bar` in the database `foo`) and PostgreSQL tables (represented by [Sequel datasets](https://github.com/jeremyevans/sequel/blob/master/doc/dataset_basics.rdoc)).

For example, if we only want to replay operations on `foo.bar` to a table `foo_bar` in PostgreSQL, we might have a mapping like so:

```ruby
DB = Sequel.connect('postgres:///oplogjam_test')
mapping = { 'foo.bar' => DB[:foo_bar] }
```

Then we can pass this mapping when we call `apply` on an operation, e.g.

```ruby
oplog.operations.each do |operation|
  operation.apply(mapping)
end
```

In order for this to work, the PostgreSQL table `foo_bar` must have the following schema:

```
                             Table "public.foo_bar"
   Column   |            Type             |              Modifiers
------------+-----------------------------+-------------------------------------
 uuid       | uuid                        | not null default uuid_generate_v1()
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

We can create this ourselves or use `Oplogjam::Table` to do it for us:

```ruby
Oplogjam::Table.new(DB).create(:foo_bar)
```

## API Documentation

### `Oplogjam::Oplog`

#### `Oplogjam::Oplog.new(client)`

```ruby
mongo = Mongo::Client.new('mongodb://localhost')
Oplogjam::Oplog.new(mongo)
```

Return a new `Oplog` for the given `Mongo::Client` `client` connected to a replica set.

#### `Oplogjam::Oplogjam#operations([query])`

```ruby
oplog.operations.each do |operation|
  # Do something with operation
end

oplog.operations('ts' => { '$gt' => BSON::Timestamp.new(123456, 1) })
```

Return an infinite `Enumerator` yielding `Operation`s from the `Oplog` with an optional MongoDB query which will filter the underlying oplog.

### `Oplogjam::Table`
#### `Oplogjam::Table.new(db)`

```ruby
DB = Sequel.connect('postgres:///oplogjam_test')
table = Oplogjam::Table.new(DB)
```

Return a new `Oplogjam::Table` for the given Sequel database connection.

#### `Oplogjam::Table#create(name)`

```ruby
table.create(:foo_bar)
```

Attempt to create a table for Oplogjam's use in PostgreSQL with the given `name` if it doesn't already exist.

A table will be created with the following schema:

* `uuid`: a UUID v1 primary key (v1 so that they are sequential);
* `id`: a `jsonb` representation of the primary key of the MongoDB document;
* `document`: a `jsonb` representation of the entire MongoDB document;
* `created_at`: the `timestamp` when this row was created by Oplogjam (_not_ by MongoDB);
* `update_at`: the `timestamp` when this row was last updated by Oplogjam (_not_ by MongoDB);
* `deleted_at`: the `timestamp` when this row was deleted by Oplogjam (_not_ by MongoDB).

It will have two constraints:

* A unique index on `id` and `deleted_at` so no two records can have the same MongoDB ID and deletion time;
* A partial unique index on `id` where `deleted_at` is `NULL` so no two records can have the same ID and not be deleted.

### `Oplogjam::Operation`

#### `Oplogjam::Operation.from(bson)`

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

### `Oplogjam::Noop`

#### `Oplogjam::Noop.from(bson)`

```ruby
Oplogjam::Noop.from(document)
```

Convert a BSON document representing a MongoDB oplog no-op into an `Oplogjam::Noop` instance.

Raises a `Oplogjam::InvalidNoop` error if the given document is not a valid no-op.

#### `Oplogjam::Noop#message`

```ruby
noop.message
#=> "initiating set"
```

Return the internal message of the no-op.

#### `Oplogjam::Noop#id`

```ruby
noop.id
#=> -2135725856567446411
```

Return the internal, unique identifier for the no-op.

#### `Oplogjam::Noop#timestamp`

```ruby
noop.timestamp
#=> 2017-09-09 16:11:18 +0100
```

Return the time of the no-op as a `Time`.

#### `Oplogjam::Noop#ts`

```ruby
noop.ts
#=> #<BSON::Timestamp:0x007fcadfa44500 @increment=1, @seconds=1479419535>
```

Return the raw, underlying BSON Timestamp of the no-op.

#### `Oplogjam::Noop#==(other)`

```ruby
noop == other_noop
#=> false
```

Compares the identifiers of two no-ops and returns true if they are equal.

#### `Oplogjam::Noop#apply(mapping)`

```ruby
noop.apply(mapping)
```

Apply this no-op to a mapping of MongoDB namespaces (e.g. `foo.bar`) to Sequel datasets representing PostgreSQL tables. As no-ops do nothing, this performs no operation.

### `Oplogjam::Insert`
#### `Oplogjam::Insert.from(bson)`
#### `Oplogjam::Insert#id`
#### `Oplogjam::Insert#namespace`
#### `Oplogjam::Insert#document`
#### `Oplogjam::Insert#timestamp`
#### `Oplogjam::Insert#ts`
#### `Oplogjam::Insert#==(other)`
#### `Oplogjam::Insert#apply(mapping)`

### `Oplogjam::Update`
#### `Oplogjam::Update.from(bson)`
#### `Oplogjam::Update#id`
#### `Oplogjam::Update#namespace`
#### `Oplogjam::Update#update`
#### `Oplogjam::Update#query`
#### `Oplogjam::Update#timestamp`
#### `Oplogjam::Update#ts`
#### `Oplogjam::Update#==(other)`
#### `Oplogjam::Update#apply(mapping)`

### `Oplogjam::Delete`
#### `Oplogjam::Delete.from(bson)`
#### `Oplogjam::Delete#id`
#### `Oplogjam::Delete#namespace`
#### `Oplogjam::Delete#query`
#### `Oplogjam::Delete#timestamp`
#### `Oplogjam::Delete#ts`
#### `Oplogjam::Delete#==(other)`
#### `Oplogjam::Delete#apply(mapping)`

### `Oplogjam::ApplyOps`
#### `Oplogjam::ApplyOps.from(bson)`
#### `Oplogjam::ApplyOps#id`
#### `Oplogjam::ApplyOps#namespace`
#### `Oplogjam::ApplyOps#operations`
#### `Oplogjam::ApplyOps#timestamp`
#### `Oplogjam::ApplyOps#ts`
#### `Oplogjam::ApplyOps#==(other)`
#### `Oplogjam::ApplyOps#apply(mapping)`

### `Oplogjam::Command`
#### `Oplogjam::Command.from(bson)`
#### `Oplogjam::Command#id`
#### `Oplogjam::Command#namespace`
#### `Oplogjam::Command#command`
#### `Oplogjam::Command#timestamp`
#### `Oplogjam::Command#ts`
#### `Oplogjam::Command#==(other)`
#### `Oplogjam::Command#apply(mapping)`

## License

Copyright © 2017 Paul Mucur.

Distributed under the MIT License.
