module Oplogjam
  module Sanitizer
    # Strip any null bytes from objects as they will be rejected by PostgreSQL
    def self.sanitize(obj)
      case obj
      when Sequel::Postgres::JSONBHash, Hash
        obj.each_with_object({}) do |(key, value), acc|
          acc[sanitize(key)] = sanitize(value)
        end
      when Sequel::Postgres::JSONBArray, Array
        obj.map { |element| sanitize(element) }
      when String
        obj.tr("\x00", '')
      else
        obj
      end
    end
  end
end
