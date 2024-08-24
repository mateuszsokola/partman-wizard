import pg from "pg";

export const connect = async (connectionString) => {
  const client = new pg.Client({
    connectionString,
  });

  await client.connect();

  return client;
};

export const close = async (client) => await client.end();

export const hasPartmanExtension = async (client) => {
  const query = `
    SELECT EXISTS (
      SELECT 1
      FROM pg_available_extensions
      WHERE name = 'pg_partman'
    ) AS available
  `;

  const result = await client.query(query);

  return result.rows.length > 0 && result.rows[0].available;
};

export const isPartmanEnabled = async (client) => {
  const query = `
        SELECT extname
        FROM pg_extension
        WHERE extname = 'pg_partman';
    `;

  const result = await client.query(query);

  return result.rows.length > 0;
};

export const enablePartmanExtension = async (client) => {
  const schemaCheck = await client.query(`
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name = 'partman';
    `);

  if (schemaCheck.rowCount === 0) {
    await client.query("CREATE SCHEMA partman");
  }
  await client.query("CREATE EXTENSION pg_partman SCHEMA partman");
};

export const listTables = async (client) => {
  const query = `
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
      AND table_name NOT IN (
        SELECT inhrelid::regclass::text
        FROM pg_inherits
      );
    `;

  const result = await client.query(query);
  return result.rows;
};

export const listTableColumns = async (client, tableName) => {
  const query = `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1::text
        AND data_type IN ('date', 'timestamp', 'timestamp with time zone');
    `;

  const result = await client.query(query, [tableName]);
  return result.rows;
};

export const listTableSequences = async (client, tableName) => {
  const query = `
    SELECT sequence_name
    FROM information_schema.sequences
    WHERE sequence_schema = 'public'
    AND sequence_name LIKE $1::text
`;

  const result = await client.query(query, [`${tableName}%`]);
  return result.rows;
};

export const isTableNameFree = async (client, tableName) => {
  const query = `
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name = $1::text
  `;

  const result = await client.query(query, [tableName]);
  return result.rows.length === 0;
};

export const createPartitionedTable = async (
  client,
  sourceTableName,
  destTableName,
  columnName,
) => {
  const query = `
    CREATE TABLE ${destTableName} (LIKE ${sourceTableName} INCLUDING ALL)
    PARTITION BY RANGE (${columnName});
  `;

  await client.query(query);
};

export const updateSequenceValue = async (
  client,
  sourceTableName,
  sourceColumnName,
  sequenceName,
) => {
  const query = `
    SELECT setval($1::text, (SELECT MAX($2::text) FROM ${sourceTableName}));
  `;
  await client.query(query, [
    `public.${sequenceName}`,
    sourceColumnName,
    sourceTableName,
  ]);
};

export const createPartmanParent = async (
  client,
  tableName,
  columnName,
  interval,
) => {
  const query = `
    SELECT partman.create_parent(
        p_parent_table := 'public.${tableName}',
        p_control := '${columnName}',
        p_interval := '${interval}'
    );
  `;
  await client.query(query);
};

export const migrateDataFromSourceToDestTable = async (
  client,
  sourceTableName,
  destSequenceName,
) => {
  const query = `
    CALL partman.partition_data_proc(
      p_parent_table := 'public.${destSequenceName}',
      p_source_table := 'public.${sourceTableName}'
    );
  `;
  await client.query(query);
};

export const vacuumAnalyze = async (client, tableName) => {
  const query = `VACUUM ANALYZE public.${tableName};`;
  await client.query(query);
};
