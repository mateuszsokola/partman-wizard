# Partman Wizard

**Partman Wizard** is a CLI tool designed to help you partition PostgreSQL tables using the `pg_partman` extension. This tool guides you through the process of setting up table partitions and migrating data efficiently.

If you need PostgreSQL-as-a-Service with support for the `pg_partman` extension, consider [Neon](https://neon.tech/).

PS. They have a FREE-TIER 💸

## What this bad boy can do?

- Create and configure partitions based on existing tables in PostgreSQL
- Migrate data from source tables to partitioned ones

It works like magic ✨

## Usage

To use the Partman Wizard, you need to have Node.js 18+ installed. You can then run the tool directly using `npx`:

```bash
npx partman-wizard
```

Follow the configuration steps and enjoy your partitioned tables.

## Configure partition maintenance

To keep your partitions up-to-date, set up a cron job to run maintenance tasks. Due to the complexity of cloud deployments and PostgreSQL itself, this wizard cannot set it up for you.

Make sure to run the following SQL query at least once per day. It's common to schedule it to run at midnight (or one minute after):

```sql
SELECT partman.run_maintenance();
```

# Feedback & Contributions

If you find this tool useful, consider giving it a star on [GitHub](https://github.com/mateuszsokola/partman-wizard). For bug reports or contributions, please visit the [GitHub repository](https://github.com/mateuszsokola/partman-wizard).

# License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/mateuszsokola/partman-wizard/blob/main/LICENSE) file for details.

<hr>

Enjoy using Partman Wizard! 😊
