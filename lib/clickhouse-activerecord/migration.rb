require 'active_record/migration'

module ClickhouseActiverecord

  class SchemaMigration < ::ActiveRecord::SchemaMigration
    def create_table
      version_options = connection.internal_string_options_for_primary_key
      table_options = {
        id: false, options: 'ReplacingMergeTree(ver) ORDER BY (version)', if_not_exists: true
      }
      full_config = connection.instance_variable_get(:@full_config) || {}

      if full_config[:distributed_service_tables]
        table_options.merge!(with_distributed: table_name, sharding_key: 'cityHash64(version)')

        distributed_suffix = "_#{full_config[:distributed_service_tables_suffix] || 'distributed'}"
      end

      return if connection.table_exists?(table_name + distributed_suffix.to_s)

      connection.create_table(table_name + distributed_suffix.to_s, **table_options) do |t|
        t.string :version, **version_options
        t.column :active, 'Int8', null: false, default: '1'
        t.datetime :ver, null: false, default: -> { 'now()' }
      end
    end

    def all_versions
      result = connection.do_execute("select version from #{table_name} FINAL WHERE active=1 ORDER BY version")
      result["data"]&.flatten
    end
  end

  class InternalMetadata < ::ActiveRecord::InternalMetadata
    def create_table
      key_options = connection.internal_string_options_for_primary_key
      table_options = {
        id: false,
        options: connection.adapter_name.downcase == 'clickhouse' ? 'MergeTree() PARTITION BY toDate(created_at) ORDER BY (created_at)' : '',
        if_not_exists: true
      }
      full_config = connection.instance_variable_get(:@full_config) || {}

      if full_config[:distributed_service_tables]
        table_options.merge!(with_distributed: table_name, sharding_key: 'cityHash64(created_at)')

        distributed_suffix = "_#{full_config[:distributed_service_tables_suffix] || 'distributed'}"
      end

      return if connection.table_exists?(table_name + distributed_suffix.to_s)

      connection.create_table(table_name + distributed_suffix.to_s, **table_options) do |t|
        t.string :key, **key_options
        t.string :value
        t.timestamps
      end
    end

    def self.enabled?
      true
    end
  end

  class MigrationContext < ::ActiveRecord::MigrationContext #:nodoc:
    attr_reader :migrations_paths, :schema_migration, :internal_metadata

    def initialize(migrations_paths, schema_migration, internal_metadata)
      @migrations_paths = migrations_paths
      @schema_migration = schema_migration
      @internal_metadata = internal_metadata
    end

    def up(target_version = nil)
      selected_migrations = if block_given?
        migrations.select { |m| yield m }
      else
        migrations
      end

      ClickhouseActiverecord::Migrator.new(:up, selected_migrations, schema_migration, internal_metadata, target_version).migrate
    end

    def down(target_version = nil)
      selected_migrations = if block_given?
        migrations.select { |m| yield m }
      else
        migrations
      end

      ClickhouseActiverecord::Migrator.new(:down, selected_migrations, schema_migration, internal_metadata, target_version).migrate
    end

    def get_all_versions
      if schema_migration.table_exists?
        schema_migration.all_versions.map(&:to_i)
      else
        []
      end
    end

  end

  class Migrator < ::ActiveRecord::Migrator

    def initialize(direction, migrations, schema_migration, internal_metadata, target_version = nil)
      @direction         = direction
      @target_version    = target_version
      @migrated_versions = nil
      @migrations        = migrations
      @schema_migration  = schema_migration
      @internal_metadata  = internal_metadata

      validate(@migrations)

      @schema_migration.create_table
      @internal_metadata.create_table
    end

    def record_version_state_after_migrating(version)
      if down?
        migrated.delete(version)
        @schema_migration.create!(version: version.to_s, active: 0)
      else
        super
      end
    end
  end
end
