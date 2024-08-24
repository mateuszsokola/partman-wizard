import { Command } from "commander";
import { startWizard } from "./core/wizard.mjs";

const program = new Command();

program
  .description(
    "This utility will help you with partitioning your tables in PostgreSQL databases.\n\n Your postgres instance must have pg_partman extension installed.",
  )
  .version("0.0.1");

program.parse(process.argv);

startWizard();
