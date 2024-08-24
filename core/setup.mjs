import assert from "node:assert";
import { confirm, input, rawlist } from "@inquirer/prompts";
import {
  connect,
  close,
  migrateDataFromSourceToDestTable,
  createPartitionedTable,
  createPartmanParent,
  enablePartmanExtension,
  hasPartmanExtension,
  listTables,
  listTableColumns,
  listTableSequences,
  isPartmanEnabled,
  isTableNameFree,
  vacuumAnalyze,
  updateSequenceValue,
} from "./postgres.mjs";

const createWizardResponse = (
  status,
  message = undefined,
  response = undefined,
) => {
  assert(
    ["ongoing", "completed", "error"].indexOf(status) >= 0,
    "Unsupported status",
  );
  return {
    status,
    message,
    response,
  };
};

export const establishPostgresConnection = async () => {
  const result = await input({
    type: "input",
    name: "connectionString",
    default: "postgres://postgres:password@localhost:5432/postgres",
    message: "Enter the connection string for your PostgreSQL instance:",

    validate: async (connectionString) => {
      let postgresClient;
      try {
        postgresClient = await connect(connectionString);
      } catch (e) {
        return "Failed to connect to the database. Please check your connection string and try again.";
      } finally {
        if (postgresClient) {
          await close(postgresClient);
        }
      }

      return true;
    },
  });

  return await connect(result);
};

export const configurePartman = async (postgresClient) => {
  const hasPartman = await hasPartmanExtension(postgresClient);

  if (!hasPartman) {
    return createWizardResponse(
      "error",
      "The pg_partman extension is not available on this PostgreSQL instance. You will need to create partitions manually, but I cannot assist with that. Exiting...",
    );
  }

  const isEnabled = await isPartmanEnabled(postgresClient);

  if (isEnabled) {
    return createWizardResponse(
      "ongoing",
      "The pg_partman extension is already installed. Skipping installation.",
    );
  }

  const result = await confirm({
    default: false,
    message:
      "Would you like to enable pg_partman extension on this PostgreSQL instance?",
  });

  if (result) {
    await enablePartmanExtension(postgresClient);

    return createWizardResponse(
      "ongoing",
      "The pg_partman extension has been installed successfully.",
    );
  }
};

export const selectTableToPartition = async (postgresClient) => {
  const tables = await listTables(postgresClient);

  if (tables.length === 0) {
    return createWizardResponse(
      "completed",
      "Your database appears to be empty, so I can only set up pg_partman for you (which has already been done). Exiting...",
    );
  }

  const tableName = await rawlist({
    message: "Select a table to configure for partitioning:",
    choices: tables.map((t) => ({ name: t.table_name, value: t.table_name })),
  });

  return createWizardResponse("ongoing", undefined, tableName);
};

export const selectColumnToPartition = async (postgresClient, tableName) => {
  const columns = await listTableColumns(postgresClient, tableName);

  if (columns.length === 0) {
    return createWizardResponse(
      "error",
      `The selected table, ${tableName}, doesn't contain any columns suitable for partitioning. This wizard only supports partitioning by date or timestamp columns. Exiting...`,
    );
  }

  const columnName = await rawlist({
    message:
      "Select a column for partitioning (only date and timestamp columns are supported):",
    choices: columns.map((c) => ({
      name: c.column_name,
      value: c.column_name,
    })),
  });

  return createWizardResponse("ongoing", undefined, columnName);
};

export const selectPartitionInterval = async () => {
  const interval = await rawlist({
    message: "Choose a partition interval:",
    choices: [
      { name: "1 day", value: "1 day" },
      { name: "10 days", value: "10 days" },
      { name: "1 week", value: "1 week" },
      { name: "2 weeks", value: "2 weeks" },
      { name: "1 month", value: "1 month" },
      { name: "3 months", value: "3 months" },
      { name: "6 months", value: "6 months" },
      { name: "1 year", value: "1 year" },
      { name: "Custom", value: "custom" },
    ],
  });

  if (interval !== "custom") {
    return createWizardResponse("ongoing", undefined, interval);
  }

  const result = await input({
    default: "10 days",
    message: "Enter a custom partition interval:",
  });

  return createWizardResponse("ongoing", undefined, result);
};

export const enterPartitionedTableName = async (
  postgresClient,
  sourceTableName,
) => {
  const destTableName = await input({
    default: `${sourceTableName}_partitioned`,
    message: "Enter a name for the new partitioned table:",
    validate: async (tableName) => {
      const result = await isTableNameFree(postgresClient, tableName);

      if (!result) {
        return "This table name is already in use. Please choose a different name.";
      }

      return true;
    },
  });

  return createWizardResponse("ongoing", undefined, destTableName);
};

export const createPartitions = async (
  postgresClient,
  sourceTableName,
  destTableName,
  columnName,
  interval,
) => {
  const result = await confirm({
    default: false,
    message:
      "Would you like to create the following table? The original table will remain unchanged.",
  });

  if (!result) {
    return createWizardResponse(
      "completed",
      "Proposed changes have been rejected. Exiting...",
    );
  }

  await createPartitionedTable(
    postgresClient,
    sourceTableName,
    destTableName,
    columnName,
  );

  await createPartmanParent(
    postgresClient,
    destTableName,
    columnName,
    interval,
  );

  const sequences = await listTableSequences(postgresClient, destTableName);
  for (const sequence of sequences) {
    const withoutTablePrefix = sequence.sequence_name.replace(
      `${sourceTableName}_`,
      "",
    );
    const destColumnName = withoutTablePrefix.replace("_seq", "");

    const destSequenceName = `${destTableName}_${destColumnName}_seq`;

    await updateSequenceValue(
      postgresClient,
      sourceTableName,
      destColumnName,
      destSequenceName,
    );
  }

  return createWizardResponse(
    "ongoing",
    `Created the ${destTableName} table and updated the associated sequences.`,
  );
};

export const copyDataBetweenSourceAndDestTable = async (
  postgresClient,
  sourceTableName,
  destTableName,
) => {
  const result = await confirm({
    default: true,
    message:
      "Would you like to transfer data from the source table to the new partitioned table? After this operation, the original table WILL BE WIPED.",
  });

  if (!result) {
    return createWizardResponse(
      "completed",
      "The partitioned table is set up. Now, you need to migrate the data to complete the process.",
    );
  }

  await migrateDataFromSourceToDestTable(
    postgresClient,
    sourceTableName,
    destTableName,
  );
  await vacuumAnalyze(postgresClient, destTableName);

  return createWizardResponse(
    "completed",
    `✅ Data has been successfully migrated from the ${sourceTableName} table to the ${destTableName} table. The process is now completed.`,
  );
};
