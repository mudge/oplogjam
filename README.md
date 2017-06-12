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
 created_at | timestamp without time zone |
 updated_at | timestamp without time zone |
 deleted_at | timestamp without time zone |
Indexes:
    "foo_bar_pkey" PRIMARY KEY, btree (uuid)
    "foo_bar_id_deleted_at_key" UNIQUE CONSTRAINT, btree (id, deleted_at)
```

It also expects to write progress into a table in PostgreSQL called `oplogjam` with the following schema:

```
    Table "public.oplogjam"
  Column   |  Type  | Modifiers
-----------+--------+-----------
 seconds   | bigint |
 increment | bigint |
```

## License

Copyright © 2017 Paul Mucur.

Distributed under the MIT License.
