# Migrations

Database-specific migrations live under `mysql`, `postgres`, and `kingbase`. Set `migration.path` to the matching directory, for example `migrations/postgres`. Review indexes, collation and online-DDL impact against production data before deployment.

Migration `000005` introduces application ownership. Historical rows retain an empty application because their ownership cannot be inferred safely; unfinished historical jobs are canceled during upgrade and all legacy rows remain inaccessible through scoped APIs until an authoritative backfill assigns the real application.
