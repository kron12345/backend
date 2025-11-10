import { createHash } from 'crypto';
import { promises as fs } from 'fs';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { PoolClient } from 'pg';
import * as path from 'path';
import { DatabaseService } from './database.service';

interface MigrationFile {
  filename: string;
  content: string;
  checksum: string;
}

@Injectable()
export class MigrationService implements OnModuleInit {
  private readonly logger = new Logger(MigrationService.name);
  private readonly migrationsDir = path.join(process.cwd(), 'sql', 'migrations');
  private readonly tableName = 'planning_schema_migration';

  constructor(private readonly database: DatabaseService) {}

  async onModuleInit() {
    if (!this.database.enabled) {
      this.logger.log('Database connection not configured, skipping migrations');
      return;
    }
    await this.runMigrations();
  }

  async runMigrations(): Promise<void> {
    const migrations = await this.readMigrationFiles();
    if (!migrations.length) {
      this.logger.log(
        `No SQL migrations found in ${this.migrationsDir}, skipping`,
      );
      return;
    }

    await this.database.withClient(async (client) => {
      await this.ensureMigrationsTable(client);
      const applied = await this.fetchAppliedMigrations(client);
      const pending = migrations.filter((migration) => {
        const checksum = applied.get(migration.filename);
        if (!checksum) {
          return true;
        }
        if (checksum !== migration.checksum) {
          throw new Error(
            `Checksum mismatch for migration ${migration.filename}. A previously applied migration was modified.`,
          );
        }
        return false;
      });

      if (!pending.length) {
        this.logger.log('Database schema already up to date');
        return;
      }

      for (const migration of pending) {
        await this.applyMigration(client, migration);
      }
    });
  }

  private async ensureMigrationsTable(client: PoolClient) {
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${this.tableName} (
        filename TEXT PRIMARY KEY,
        checksum TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);
  }

  private async fetchAppliedMigrations(
    client: PoolClient,
  ): Promise<Map<string, string>> {
    const result = await client.query<{ filename: string; checksum: string }>(
      `SELECT filename, checksum FROM ${this.tableName}`,
    );
    return result.rows.reduce((map, row) => {
      map.set(row.filename, row.checksum);
      return map;
    }, new Map<string, string>());
  }

  private async applyMigration(
    client: PoolClient,
    migration: MigrationFile,
  ): Promise<void> {
    this.logger.log(`Applying migration ${migration.filename}`);
    await client.query('BEGIN');
    try {
      await client.query(migration.content);
      await client.query(
        `INSERT INTO ${this.tableName} (filename, checksum)
         VALUES ($1, $2)
         ON CONFLICT (filename)
         DO UPDATE SET checksum = EXCLUDED.checksum, applied_at = now()`,
        [migration.filename, migration.checksum],
      );
      await client.query('COMMIT');
      this.logger.log(`Migration ${migration.filename} applied`);
    } catch (error) {
      await client.query('ROLLBACK');
      this.logger.error(
        `Failed to apply migration ${migration.filename}`,
        (error as Error).stack ?? error,
      );
      throw error;
    }
  }

  private async readMigrationFiles(): Promise<MigrationFile[]> {
    let entries: string[];
    try {
      entries = await fs.readdir(this.migrationsDir);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        return [];
      }
      throw error;
    }

    const sqlFiles = entries
      .filter((name) => name.toLowerCase().endsWith('.sql'))
      .sort();

    const migrations: MigrationFile[] = [];
    for (const filename of sqlFiles) {
      const fullPath = path.join(this.migrationsDir, filename);
      const content = await fs.readFile(fullPath, 'utf8');
      migrations.push({
        filename,
        content,
        checksum: this.computeChecksum(content),
      });
    }
    return migrations;
  }

  private computeChecksum(content: string): string {
    return createHash('sha256').update(content).digest('hex');
  }
}
