#!/usr/bin/env node

import { runMeter } from './meter.js';

const result = runMeter(process.argv.slice(2));
process.stdout.write(result.stdout);
process.stderr.write(result.stderr);
process.exitCode = result.exitCode;
