# Oplogjam [![Build Status](https://travis-ci.org/mudge/oplogjam.svg)](https://travis-ci.org/mudge/oplogjam)

**Current version:** 0.1.0  
**Supported Ruby versions:** 2.0, 2.1, 2.2  
**Supported MongoDB versions:** 2.4, 2.6, 3.0, 3.2, 3.4  
**Supported PostgreSQL versions:** 9.5, 9.6

An experiment in writing a "safe" MongoDB oplog tailer that converts documents to PostgreSQL JSONB in Ruby.

Based on experiences running [Stripe's now deprecated MoSQL project](https://github.com/stripe/mosql) in production, this project provides a core library which stores all MongoDB documents in the same standard table schema in PostgreSQL but leaves all configuration and orchestration to the user. This means that this library can be used to _power_ an end-to-end MoSQL replacement but does not provide all functionality itself.

At its heart, the library connects to a [MongoDB replica set oplog](https://docs.mongodb.com/manual/core/replica-set-oplog/) and provides an abstraction to users so they can iterate over the operations in the oplog and transform those into equivalent PostgreSQL SQL statements.

```ruby
DB = Sequel.connect('postgres:///acme')
mongo = Mongo::Client.new('mongodb://localhost')

Oplogjam::Oplog.new(mongo).operations.each do |operation|
  operation.apply('acme.widgets' => DB[:widgets], 'acme.anvils' => DB[:anvils])
end
```

## Requirements

* A [MongoDB replica set](https://docs.mongodb.com/manual/replication/);
* [PostgreSQL 9.5](https://www.postgresql.org/docs/9.5/static/release-9-5.html) or newer (for [`INSERT ON CONFLICT`](https://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT) and [`jsonb_set`](https://www.postgresql.org/docs/9.5/static/functions-json.html#FUNCTIONS-JSON-PROCESSING-TABLE) support);
* A PostgreSQL database with the [`uuid-ossp` extension](https://www.postgresql.org/docs/current/static/uuid-ossp.html).

## Why does `apply` take a mapping?

This library expects to replay operations on MongoDB collections on equivalent PostgreSQL tables. As the MongoDB oplog contains _all_ operations on a replica set in a single collection, you must provide a _mapping_ between MongoDB namespaces (e.g. a database and collection name such as `foo.bar` for a collection `bar` in the database `foo`) and PostgreSQL tables (represented by [Sequel datasets](https://github.com/jeremyevans/sequel/blob/master/doc/dataset_basics.rdoc)). Any operations for namespaces not included in the mapping will be ignored.

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
 updated_at | timestamp without time zone | not null
 deleted_at | timestamp without time zone |
Indexes:
    "foo_bar_pkey" PRIMARY KEY, btree (uuid)
    "foo_bar_id_deleted_at_key" UNIQUE CONSTRAINT, btree (id, deleted_at)
    "foo_bar_id_index" UNIQUE, btree (id) WHERE deleted_at IS NULL
```

We can create this ourselves or use [`Oplogjam::Schema`](#oplogjamschema) to do it for us:

```ruby
schema = Oplogjam::Schema.new(DB)
schema.create_table(:foo_bar)
schema.import(collection, :foo_bar) # Optionally import data from a MongoDB collection
schema.add_indexes(:foo_bar)
```

## Why does this project exist?

Since maintenance of MoSQL by Stripe was ended, there have been several major changes that affect anyone designing a system to replay a MongoDB oplog in PostgreSQL:

* The MongoDB driver ecosystem was overhauled and the Ruby driver API changed significantly;
* PostgreSQL 9.5 introduced new JSONB operations such as `jsonb_set` for updating fields in JSONB objects;
* PostgreSQL 9.5 also introduced `INSERT ON CONFLICT` for effectively "upserting" duplicate records on `INSERT`.

Running MoSQL in production also revealed that we didn't need its rich support for transforming MongoDB documents into typical relational schema with typed columns but instead relied entirely on its JSONB support: effectively mirroring the MongoDB document by storing it in a single JSONB column.

With that specific use case in mind, I wanted to explore whether a library to _safely_ transform arbitrary MongoDB operations into SQL could be done in Ruby and remain somewhat idiomatic.

## Why doesn't this project come with some sort of executable?

While the library is more opinionated about the data schema of PostgreSQL, it doesn't attempt to make any decisions about how you decide which collections you want to replicate and where they should be replicated to. Similarly, connecting to databases, logging, etc. are all left up to the user as something that can might differ wildly.

While in future I may add an executable, for now this is up to the user to manage.

## API Documentation

* [`Oplogjam::Oplog`](#oplogjamoplog)
  * [`Oplogjam::Oplog.new(client)`](#oplogjamoplognewclient)
  * [`Oplogjam::Oplog#operations([query])`](#oplogjamoplogoperationsquery)
* [`Oplogjam::Schema`](#oplogjamschema)
  * [`Oplogjam::Schema.new(db)`](#oplogjamschemadb)
  * [`Oplogjam::Schema#create_table(name)`](#oplogjamschemacreate_tablename)
  * [`Oplogjam::Schema#add_indexes(name)`](#oplogjamschemaadd_indexesname)
  * [`Oplogjam::Schema#import(collection, name, batch_size = 100)`](#oplogjamschemaimportcollection-name-batch_size)
* [`Oplogjam::Operation`](#oplogjamoperation)
  * [`Oplogjam::Operation.from(bson)`](#oplogjamoperationfrombson)
* [`Oplogjam::Noop`](#oplogjamnoop)
  * [`Oplogjam::Noop.from(bson)`](#oplogjamnoopfrombson)
  * [`Oplogjam::Noop#message`](#oplogjamnoopmessage)
  * [`Oplogjam::Noop#id`](#oplogjamnoopid)
  * [`Oplogjam::Noop#timestamp`](#oplogjamnooptimestamp)
  * [`Oplogjam::Noop#ts`](#oplogjamnoopts)
  * [`Oplogjam::Noop#==(other)`](#oplogjamnoopother)
  * [`Oplogjam::Noop#apply(mapping)`](#oplogjamnoopapplymapping)
* [`Oplogjam::Insert`](#oplogjaminsert)
  * [`Oplogjam::Insert.from(bson)`](#oplogjaminsertfrombson)
  * [`Oplogjam::Insert#id`](#oplogjaminsertid)
  * [`Oplogjam::Insert#namespace`](#oplogjaminsertnamespace)
  * [`Oplogjam::Insert#document`](#oplogjaminsertdocument)
  * [`Oplogjam::Insert#timestamp`](#oplogjaminserttimestamp)
  * [`Oplogjam::Insert#ts`](#oplogjaminsertts)
  * [`Oplogjam::Insert#==(other)`](#oplogjaminsertother)
  * [`Oplogjam::Insert#apply(mapping)`](#oplogjaminsertapplymapping)
* [`Oplogjam::Update`](#oplogjamupdate)
  * [`Oplogjam::Update.from(bson)`](#oplogjamupdatefrombson)
  * [`Oplogjam::Update#id`](#oplogjamupdateid)
  * [`Oplogjam::Update#namespace`](#oplogjamupdatenamespace)
  * [`Oplogjam::Update#update`](#oplogjamupdateupdate)
  * [`Oplogjam::Update#query`](#oplogjamupdatequery)
  * [`Oplogjam::Update#timestamp`](#oplogjamupdatetimestamp)
  * [`Oplogjam::Update#ts`](#oplogjamupdatets)
  * [`Oplogjam::Update#==(other)`](#oplogjamupdateother)
  * [`Oplogjam::Update#apply(mapping)`](#oplogjamupdateapplymapping)
* [`Oplogjam::Delete`](#oplogjamdelete)
  * [`Oplogjam::Delete.from(bson)`](#oplogjamdeletefrombson)
  * [`Oplogjam::Delete#id`](#oplogjamdeleteid)
  * [`Oplogjam::Delete#namespace`](#oplogjamdeletenamespace)
  * [`Oplogjam::Delete#query`](#oplogjamdeletequery)
  * [`Oplogjam::Delete#timestamp`](#oplogjamdeletetimestamp)
  * [`Oplogjam::Delete#ts`](#oplogjamdeletets)
  * [`Oplogjam::Delete#==(other)`](#oplogjamdeleteother)
  * [`Oplogjam::Delete#apply(mapping)`](#oplogjamdeleteapplymapping)
* [`Oplogjam::ApplyOps`](#oplogjamapplyops)
  * [`Oplogjam::ApplyOps.from(bson)`](#oplogjamapplyopsfrombson)
  * [`Oplogjam::ApplyOps#id`](#oplogjamapplyopsid)
  * [`Oplogjam::ApplyOps#namespace`](#oplogjamapplyopsnamespace)
  * [`Oplogjam::ApplyOps#operations`](#oplogjamapplyopsoperations)
  * [`Oplogjam::ApplyOps#timestamp`](#oplogjamapplyopstimestamp)
  * [`Oplogjam::ApplyOps#ts`](#oplogjamapplyopsts)
  * [`Oplogjam::ApplyOps#==(other)`](#oplogjamapplyopsother)
  * [`Oplogjam::ApplyOps#apply(mapping)`](#oplogjamapplyopsapplymapping)
* [`Oplogjam::Command`](#oplogjamcommand)
  * [`Oplogjam::Command.from(bson)`](#oplogjamcommandfrombson)
  * [`Oplogjam::Command#id`](#oplogjamcommandid)
  * [`Oplogjam::Command#namespace`](#oplogjamcommandnamespace)
  * [`Oplogjam::Command#command`](#oplogjamcommandcommand)
  * [`Oplogjam::Command#timestamp`](#oplogjamcommandtimestamp)
  * [`Oplogjam::Command#ts`](#oplogjamcommandts)
  * [`Oplogjam::Command#==(other)`](#oplogjamcommandother)
  * [`Oplogjam::Command#apply(mapping)`](#oplogjamcommandapplymapping)

### `Oplogjam::Oplog`

An object representing a MongoDB oplog.

#### `Oplogjam::Oplog.new(client)`

```ruby
mongo = Mongo::Client.new('mongodb://localhost')
Oplogjam::Oplog.new(mongo)
```

Return a new [`Oplogjam::Oplog`](#oplogjamoplog) for the given [`Mongo::Client`](http://api.mongodb.com/ruby/current/Mongo/Client.html) `client` connected to a replica set.

#### `Oplogjam::Oplogjam#operations([query])`

```ruby
oplog.operations.each do |operation|
  # Do something with operation
end

oplog.operations('ts' => { '$gt' => BSON::Timestamp.new(123456, 1) })
```

Return an infinite `Enumerator` yielding [`Operation`](#oplogjamoperation)s from the [`Oplog`](#oplogjamoplog) with an optional MongoDB `query` which will affect the results from the underlying oplog.

### `Oplogjam::Schema`

A class to manage the PostgreSQL schema used by Oplogjam (e.g. creating tables, importing data, etc.).

#### `Oplogjam::Schema.new(db)`

```ruby
DB = Sequel.connect('postgres:///oplogjam_test')
schema = Oplogjam::Schema.new(DB)
```

Return a new [`Oplogjam::Schema`](#oplogjamschema) for the given [Sequel database connection](http://sequel.jeremyevans.net/rdoc/classes/Sequel/Database.html).

#### `Oplogjam::Schema#create_table(name)`

```ruby
schema.create_table(:foo_bar)
```

Attempt to create a table for Oplogjam's use in PostgreSQL with the given `name` if it doesn't already exist. Note that the `name` may be a single `String`, `Symbol` or a [Sequel qualified identifier](https://github.com/jeremyevans/sequel/blob/master/doc/sql.rdoc#identifiers) if you're using [PostgreSQL schema](https://www.postgresql.org/docs/current/static/ddl-schemas.html).

A table will be created with the following schema:

* `uuid`: a UUID v1 primary key (v1 so that they are sequential);
* `id`: a `jsonb` representation of the primary key of the MongoDB document;
* `document`: a `jsonb` representation of the entire MongoDB document;
* `created_at`: the `timestamp` when this row was created by Oplogjam (_not_ by MongoDB);
* `updated_at`: the `timestamp` when this row was last updated by Oplogjam (_not_ by MongoDB);
* `deleted_at`: the `timestamp` when this row was deleted by Oplogjam (_not_ by MongoDB).

If the table already exists, the method will do nothing.

#### `Oplogjam::Schema#add_indexes(name)`

```ruby
schema.add_indexes(name)
```

Add the following indexes and constraints to the table with the given `name` if they don't already exist:

* A unique index on `id` and `deleted_at` so no two records can have the same MongoDB ID and deletion time;
* A partial unique index on `id` where `deleted_at` is `NULL` so no two records can have the same ID and not be deleted.

Note that the `name` may be a single `String`, `Symbol` or a [Sequel qualified identifier](https://github.com/jeremyevans/sequel/blob/master/doc/sql.rdoc#identifiers) if you're using PostgreSQL schema.

If the indexes already exist on the given table, the method will do nothing.

#### `Oplogjam::Schema#import(collection, name)`

```ruby
schema.import(mongo[:bar], :foo_bar)
```

Batch import all existing documents from a given [`Mongo::Collection`](http://api.mongodb.com/ruby/current/Mongo/Collection.html) `collection` into the PostgreSQL table with the given `name`. Note that the `name` may be a single `String`, `Symbol` or [Sequel qualified identifier](https://github.com/jeremyevans/sequel/blob/master/doc/sql.rdoc#identifiers) if you're using PostgreSQL schema.

For performance, it's better to import existing data _before_ adding indexes to the table (hence the separate [`create_table`](#oplogjamschemacreate_tablename) and [`add_indexes`](#oplogjamschemaadd_indexesname) methods).

### `Oplogjam::Operation`

A class representing a single MongoDB oplog operation.

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

A class representing a MongoDB no-op.

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
noop.apply('foo.bar' => DB[:bar])
```

Apply this no-op to a mapping of MongoDB namespaces (e.g. `foo.bar`) to Sequel datasets representing PostgreSQL tables. As no-ops do nothing, this performs no operation.

### `Oplogjam::Insert`

A class representing a MongoDB insert.

#### `Oplogjam::Insert.from(bson)`

```ruby
Oplogjam::Insert.from(document)
```

Convert a BSON document representing a MongoDB oplog insert into an `Oplogjam::Insert` instance.

Raises a `Oplogjam::InvalidInsert` error if the given document is not a valid insert.

#### `Oplogjam::Insert#id`

```ruby
insert.id
#=> -2135725856567446411
```

Return the internal, unique identifier for the insert.

#### `Oplogjam::Insert#namespace`

```ruby
insert.namespace
#=> "foo.bar"
```

Return the namespace the insert affects. This will be a `String` of the form `database.collection`, e.g. `foo.bar`.

#### `Oplogjam::Insert#document`

```ruby
insert.document
#=> {"_id"=>1}
```

Return the `BSON::Document` being inserted.

#### `Oplogjam::Insert#timestamp`

```ruby
insert.timestamp
#=> 2017-09-09 16:11:18 +0100
```

Return the time of the insert as a `Time`.

#### `Oplogjam::Insert#ts`

```ruby
insert.ts
#=> #<BSON::Timestamp:0x007fcadfa44500 @increment=1, @seconds=1479419535>
```

Return the raw, underlying BSON Timestamp of the insert.

#### `Oplogjam::Insert#==(other)`

```ruby
insert == other_insert
#=> false
```

Compares the identifiers of two inserts and returns true if they are equal.

#### `Oplogjam::Insert#apply(mapping)`

```ruby
insert.apply('foo.bar' => DB[:bar])
```

Apply this insert to a mapping of MongoDB namespaces (e.g. `foo.bar`) to Sequel datasets representing PostgreSQL tables. If the namespace of the insert maps to a dataset in the mapping, insert this document into the dataset with the following values:

* A unique UUID v1 identifier;
* The value of the document's `_id` stored as a JSONB value;
* The entire document stored as a JSONB value;
* The current time as `created_at`;
* The current time as `updated_at`.

### `Oplogjam::Update`

A class representing a MongoDB update.

#### `Oplogjam::Update.from(bson)`

```ruby
Oplogjam::Update.from(document)
```

Convert a BSON document representing a MongoDB oplog update into an `Oplogjam::Update` instance.

Raises a `Oplogjam::InvalidUpdate` error if the given document is not a valid update.

#### `Oplogjam::Update#id`

```ruby
update.id
#=> -2135725856567446411
```

Return the internal, unique identifier for the update.

#### `Oplogjam::Update#namespace`

```ruby
update.namespace
#=> "foo.bar"
```

Return the namespace the update affects. This will be a `String` of the form `database.collection`, e.g. `foo.bar`.

#### `Oplogjam::Update#update`

```ruby
update.update
#=> {"$set"=>{"name"=>"Alice"}}
```

Return the update to be applied as a `BSON::Document`.

#### `Oplogjam::Update#query`

```ruby
update.query
#=> {"_id"=>1}
```

Return the query identifying which documents should be updated as a `BSON::Document`.

#### `Oplogjam::Update#timestamp`

```ruby
update.timestamp
#=> 2017-09-09 16:11:18 +0100
```

Return the time of the update as a `Time`.

#### `Oplogjam::Update#ts`

```ruby
update.ts
#=> #<BSON::Timestamp:0x007fcadfa44500 @increment=1, @seconds=1479419535>
```

Return the raw, underlying BSON Timestamp of the update.

#### `Oplogjam::Update#==(other)`

```ruby
update == other_update
#=> false
```

Compares the identifiers of two updates and returns true if they are equal.

#### `Oplogjam::Update#apply(mapping)`

```ruby
update.apply('foo.bar' => DB[:bar])
```

Apply this update to a mapping of MongoDB namespaces (e.g. `foo.bar`) to Sequel datasets representing PostgreSQL tables. If the namespace of the update maps to a dataset in the mapping, perform the update by finding the relevant row based on the query and transforming the MongoDB update into an equivalent PostgreSQL update.

This will also update the `updated_at` column of the target document to the current time.

### `Oplogjam::Delete`

A class representing a MongoDB deletion.

#### `Oplogjam::Delete.from(bson)`

```ruby
Oplogjam::Delete.from(document)
```

Convert a BSON document representing a MongoDB oplog delete into an `Oplogjam::Delete` instance.

Raises a `Oplogjam::InvalidDelete` error if the given document is not a valid delete.

#### `Oplogjam::Delete#id`

```ruby
delete.id
#=> -2135725856567446411
```

Return the internal, unique identifier for the delete.

#### `Oplogjam::Delete#namespace`

```ruby
delete.namespace
#=> "foo.bar"
```

Return the namespace the delete affects. This will be a `String` of the form `database.collection`, e.g. `foo.bar`.

#### `Oplogjam::Delete#query`

```ruby
delete.query
#=> {"_id"=>1}
```

Return the query identifying which documents should be deleted as a `BSON::Document`.

#### `Oplogjam::Delete#timestamp`

```ruby
delete.timestamp
#=> 2017-09-09 16:11:18 +0100
```

Return the time of the delete as a `Time`.

#### `Oplogjam::Delete#ts`

```ruby
delete.ts
#=> #<BSON::Timestamp:0x007fcadfa44500 @increment=1, @seconds=1479419535>
```

Return the raw, underlying BSON Timestamp of the delete.

#### `Oplogjam::Delete#==(other)`

```ruby
delete == other_delete
#=> false
```

Compares the identifiers of two deletes and returns true if they are equal.

#### `Oplogjam::Delete#apply(mapping)`

```ruby
delete.apply('foo.bar' => DB[:bar])
```

Apply this delete to a mapping of MongoDB namespaces (e.g. `foo.bar`) to Sequel datasets representing PostgreSQL tables. If the namespace of the delete maps to a dataset in the mapping, perform a soft deletion by finding the relevant row based on the query and setting `deleted_at` to the current time.

This will also update the `updated_at` column of the target document to the current time.

### `Oplogjam::ApplyOps`

A class representing a series of MongoDB operations in a single operation.

#### `Oplogjam::ApplyOps.from(bson)`

```ruby
Oplogjam::ApplyOps.from(document)
```

Convert a BSON document representing a MongoDB oplog apply ops into an `Oplogjam::ApplyOps` instance.

Raises a `Oplogjam::InvalidApplyOps` error if the given document is not a valid apply ops.

#### `Oplogjam::ApplyOps#id`

```ruby
apply_ops.id
#=> -2135725856567446411
```

Return the internal, unique identifier for the apply ops.

#### `Oplogjam::ApplyOps#namespace`

```ruby
apply_ops.namespace
#=> "foo.bar"
```

Return the namespace the apply ops affects. This will be a `String` of the form `database.collection`, e.g. `foo.bar`.

#### `Oplogjam::ApplyOps#operations`

```ruby
apply_ops.operations
#=> [#<Oplogjam::Insert ...>]
```

Return the operations within the apply ops as Oplogjam operations of the appropriate type.

#### `Oplogjam::ApplyOps#timestamp`

```ruby
apply_ops.timestamp
#=> 2017-09-09 16:11:18 +0100
```

Return the time of the apply ops as a `Time`.

#### `Oplogjam::ApplyOps#ts`

```ruby
apply_ops.ts
#=> #<BSON::Timestamp:0x007fcadfa44500 @increment=1, @seconds=1479419535>
```

Return the raw, underlying BSON Timestamp of the apply ops.

#### `Oplogjam::ApplyOps#==(other)`

```ruby
apply_ops == other_apply_ops
#=> false
```

Compares the identifiers of two apply ops and returns true if they are equal.

#### `Oplogjam::ApplyOps#apply(mapping)`

```ruby
apply_ops.apply('foo.bar' => DB[:bar])
```

Apply all of the operations inside this apply ops to a mapping of MongoDB namespaces (e.g. `foo.bar`) to Sequel datasets representing PostgreSQL tables. If the namespace of the operations map to a dataset in the mapping, apply them as described in each operation type's `apply` method.

### `Oplogjam::Command`

A class representing a MongoDB command.

#### `Oplogjam::Command.from(bson)`

```ruby
Oplogjam::Command.from(document)
```

Convert a BSON document representing a MongoDB oplog command into an `Oplogjam::Command` instance.

Raises a `Oplogjam::InvalidCommand` error if the given document is not a valid command.

#### `Oplogjam::Command#id`

```ruby
command.id
#=> -2135725856567446411
```

Return the internal, unique identifier for the command.

#### `Oplogjam::Command#namespace`

```ruby
command.namespace
#=> "foo.bar"
```

Return the namespace the command affects. This will be a `String` of the form `database.collection`, e.g. `foo.bar`.

#### `Oplogjam::Command#command`

```ruby
command.command
#=> {"create"=>"bar"}
```

Return the contents of the command as a `BSON::Document`.

#### `Oplogjam::Command#timestamp`

```ruby
command.timestamp
#=> 2017-09-09 16:11:18 +0100
```

Return the time of the command as a `Time`.

#### `Oplogjam::Command#ts`

```ruby
command.ts
#=> #<BSON::Timestamp:0x007fcadfa44500 @increment=1, @seconds=1479419535>
```

Return the raw, underlying BSON Timestamp of the command.

#### `Oplogjam::Command#==(other)`

```ruby
command == other_command
#=> false
```

Compares the identifiers of two commands and returns true if they are equal.

#### `Oplogjam::Command#apply(mapping)`

```ruby
command.apply('foo.bar' => DB[:bar])
```

Apply this command to a mapping of MongoDB namespaces (e.g. `foo.bar`) to Sequel datasets representing PostgreSQL tables. As commands have no equivalent in PostgreSQL, this performs no operation.

## Acknowledgements

* [Stripe's MoSQL](https://github.com/stripe/mosql)
* [Stripe's Mongoriver](https://github.com/stripe/mongoriver/)

## License

Copyright © 2017 Paul Mucur.

Distributed under the MIT License.
