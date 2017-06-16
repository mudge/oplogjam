require 'oplogjam/jsonb'

module Oplogjam
  APPLY_OPS = 'applyOps'.freeze
  ARRAY_TYPE = 'array'.freeze
  C = 'c'.freeze
  D = 'd'.freeze
  EMPTY_OBJECT = Sequel.pg_jsonb({}.freeze)
  FIELD_SEPARATOR = '.'.freeze
  GREATER_THAN = '$gt'.freeze
  H = 'h'.freeze
  I = 'i'.freeze
  ID = '_id'.freeze
  LOCAL = 'local'.freeze
  MSG = 'msg'.freeze
  N = 'n'.freeze
  NS = 'ns'.freeze
  NULL = 'null'.freeze
  NUMERIC_INDEX = /\A\d+\z/
  O = 'o'.freeze
  O2 = 'o2'.freeze
  OPLOG = 'oplog.rs'.freeze
  SET = '$set'.freeze
  TS = 'ts'.freeze
  U = 'u'.freeze
  UNSET = '$unset'.freeze
end
