import chalk from "chalk";
import {
  configurePartman,
  copyDataBetweenSourceAndDestTable,
  createPartitions,
  enterPartitionedTableName,
  establishPostgresConnection,
  selectColumnToPartition,
  selectPartitionInterval,
  selectTableToPartition,
} from "./setup.mjs";

export const startWizard = async () => {
  displayWelcomeInfo();

  const postgresClient = await establishPostgresConnection();

  handleResponse(await configurePartman(postgresClient));

  const sourceTableName = handleResponse(
    await selectTableToPartition(postgresClient),
  );

  const columnName = handleResponse(
    await selectColumnToPartition(postgresClient, sourceTableName),
  );

  const interval = handleResponse(await selectPartitionInterval());

  const destTableName = handleResponse(
    await enterPartitionedTableName(postgresClient, sourceTableName),
  );

  displayProposedChangeSet(
    sourceTableName,
    destTableName,
    columnName,
    interval,
  );

  handleResponse(
    await createPartitions(
      postgresClient,
      sourceTableName,
      destTableName,
      columnName,
      interval,
    ),
  );

  handleResponse(
    await copyDataBetweenSourceAndDestTable(
      postgresClient,
      sourceTableName,
      destTableName,
    ),
  );
};

const handleResponse = (wizardResponse) => {
  switch (wizardResponse.status) {
    case "ongoing": {
      if (wizardResponse.message) {
        console.log("\n" + wizardResponse.message);
      }
      return wizardResponse.response;
    }
    case "completed": {
      if (wizardResponse.message) {
        console.log("\n" + chalk.green(wizardResponse.message));
      }
      displayFooter();
      process.exit(0);
    }
    case "error": {
      if (wizardResponse.message) {
        console.log("\n" + chalk.red(wizardResponse.message));
      }
      process.exit(1);
    }
    default: {
      throw new Error("Unsupported status");
    }
  }
};

const displayWelcomeInfo = () => {
  console.log(
    chalk.green("\nWelcome to the PostgreSQL Partition Wizard! 🌟\n"),
  );
  console.log(
    "This tool will assist you in partitioning tables in your PostgreSQL database.",
  );
  console.log(chalk.bold("\nIMPORTANT:"));
  console.log(
    chalk.yellow(
      " - Your PostgreSQL instance must have pg_partman installed for this wizard to work.",
    ),
  );
  console.log(
    chalk.yellow(
      " - This wizard will create a new role for partman and partition the selected table based on your settings.",
    ),
  );
  console.log(
    chalk.yellow(
      " - No existing tables, foreign keys, or indices will be altered.\n",
    ),
  );
  console.log(
    chalk.red(
      "⚠️ WARNING: This tool could potentially damage your database. Ensure you have backups before proceeding!\n",
    ),
  );
};

const displayProposedChangeSet = (
  sourceTableName,
  destTableName,
  columnName,
  interval,
) => {
  console.log(chalk.green("\nProposed Changes:\n"));
  console.log(`Source Table:    ${chalk.cyan(sourceTableName)}`);
  console.log(`Partitioned Table: ${chalk.cyan(destTableName)}`);
  console.log(`Partitioning Column: ${chalk.cyan(columnName)}`);
  console.log(`Partition Interval: ${chalk.cyan(interval)}\n`);

  console.log(chalk.yellow("Please review the proposed changes carefully."));
};

const displayFooter = () => {
  console.log(
    chalk.yellow(
      "\nIMPORTANT: To ensure proper operation, pg_partman requires regular maintenance. Don't forget to set up a cron job to run at least once per day (or more frequently) to keep your partitions up-to-date. Use the following command:",
    ),
  );
  console.log(chalk.yellow("\nSELECT partman.run_maintenance();\n"));
  console.log(
    chalk.cyan(
      "If you like this tool, please consider giving it a star ⭐ on GitHub: https://github.com/mateuszsokola/partman-wizard \n",
    ),
  );
  console.log(
    chalk.blue("I hope you enjoyed your partitioning experience! 😊"),
  );
};
