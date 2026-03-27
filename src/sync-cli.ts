#!/usr/bin/env node

import 'dotenv/config';
import { getDb } from './db.js';
import { isSyncConfigured, runSyncCycle } from './sync.js';

if (!isSyncConfigured()) {
  process.stderr.write(
    'flightlog: FLIGHTLOG_SYNC_URL is not set. Configure it to enable PostgreSQL sync.\n',
  );
  process.exit(1);
}

const db = getDb();
const result = await runSyncCycle(db);
process.stdout.write(JSON.stringify(result, null, 2) + '\n');
process.exit(result.error ? 1 : 0);
