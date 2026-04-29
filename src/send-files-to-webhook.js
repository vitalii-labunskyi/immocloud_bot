#!/usr/bin/env node

const fs = require('fs/promises');
const path = require('path');
const dotenv = require('dotenv');
const XLSX = require('xlsx');

dotenv.config();

const WEBHOOK_URL = (process.env.WEBHOOK_URL || '').trim();
const WEBHOOK_TOKEN = (process.env.WEBHOOK_TOKEN || '').trim();
const WEBHOOK_FILE_FIELD = (process.env.WEBHOOK_FILE_FIELD || 'files').trim();
const WEBHOOK_SOURCE_DIR = (process.env.WEBHOOK_SOURCE_DIR || '').trim();
const WEBHOOK_CONVERT_CSV_TO_XLSX = parseBoolean(process.env.WEBHOOK_CONVERT_CSV_TO_XLSX, true);

function parseBoolean(value, fallback) {
  if (value == null || value === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
}

function printUsage() {
  console.log(
    [
      'Usage:',
      '  node src/send-files-to-webhook.js <file-or-dir> [more files or dirs]',
      '',
      'Environment:',
      '  WEBHOOK_URL         Required target URL',
      '  WEBHOOK_TOKEN       Optional Bearer token',
      '  WEBHOOK_FILE_FIELD  Multipart field name, default: files',
      '  WEBHOOK_SOURCE_DIR  Optional fallback path when no CLI args are passed',
      '  WEBHOOK_CONVERT_CSV_TO_XLSX  Convert CSV files to .xlsx before upload, default: true',
    ].join('\n'),
  );
}

function detectDelimiter(text) {
  const firstLine = String(text).replace(/^\uFEFF/, '').split(/\r?\n/, 1)[0] || '';
  const candidates = [';', ',', '\t'];
  let bestDelimiter = ';';
  let bestCount = -1;

  for (const candidate of candidates) {
    const count = firstLine.split(candidate).length - 1;
    if (count > bestCount) {
      bestCount = count;
      bestDelimiter = candidate;
    }
  }

  return bestDelimiter;
}

function stripFirstLine(buffer) {
  const text = buffer.toString('utf8');
  const trimmed = text.replace(/^\uFEFF/, '').replace(/^.*(?:\r?\n|$)/, '');
  return Buffer.from(trimmed, 'utf8');
}

function convertCsvBufferToXlsx(buffer, fileName) {
  const csvText = buffer.toString('utf8');
  const workbook = XLSX.read(csvText, {
    type: 'string',
    FS: detectDelimiter(csvText),
    raw: true,
  });

  return {
    fileName: `${path.basename(fileName, path.extname(fileName))}.xlsx`,
    buffer: XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' }),
    mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  };
}

async function buildUploadEntry(filePath) {
  const fileName = path.basename(filePath);
  const originalBuffer = await fs.readFile(filePath);
  const isCsv = path.extname(fileName).toLowerCase() === '.csv';
  const buffer = isCsv ? stripFirstLine(originalBuffer) : originalBuffer;

  if (WEBHOOK_CONVERT_CSV_TO_XLSX && isCsv) {
    return convertCsvBufferToXlsx(buffer, fileName);
  }

  return {
    fileName,
    buffer,
    mimeType: 'application/octet-stream',
  };
}

async function collectFiles(inputPath) {
  const absolutePath = path.resolve(inputPath);
  const stat = await fs.stat(absolutePath);

  if (stat.isFile()) {
    return [absolutePath];
  }

  if (!stat.isDirectory()) {
    return [];
  }

  const entries = await fs.readdir(absolutePath, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const entryPath = path.join(absolutePath, entry.name);
    if (entry.isDirectory()) {
      files.push(...await collectFiles(entryPath));
      continue;
    }

    if (entry.isFile()) {
      files.push(entryPath);
    }
  }

  return files;
}

async function resolveInputFiles(rawInputs) {
  const inputs = rawInputs.length > 0
    ? rawInputs
    : (WEBHOOK_SOURCE_DIR ? [WEBHOOK_SOURCE_DIR] : []);

  if (inputs.length === 0) {
    throw new Error('Pass at least one file or directory, or set WEBHOOK_SOURCE_DIR.');
  }

  const collected = [];
  for (const input of inputs) {
    collected.push(...await collectFiles(input));
  }

  const uniqueFiles = [...new Set(collected)].sort((left, right) => left.localeCompare(right));
  if (uniqueFiles.length === 0) {
    throw new Error('No files found to upload.');
  }

  return uniqueFiles;
}

async function uploadFiles(filePaths) {
  if (!WEBHOOK_URL) {
    throw new Error('WEBHOOK_URL is required.');
  }

  if (typeof fetch !== 'function' || typeof FormData !== 'function' || typeof Blob !== 'function') {
    throw new Error('This script requires Node.js 18+ with fetch, FormData, and Blob support.');
  }

  const form = new FormData();

  for (const filePath of filePaths) {
    const uploadEntry = await buildUploadEntry(filePath);
    form.append(
      WEBHOOK_FILE_FIELD,
      new Blob([uploadEntry.buffer], { type: uploadEntry.mimeType }),
      uploadEntry.fileName,
    );
  }

  const headers = {};
  if (WEBHOOK_TOKEN) {
    headers.Authorization = `Bearer ${WEBHOOK_TOKEN}`;
  }

  const response = await fetch(WEBHOOK_URL, {
    method: 'POST',
    headers,
    body: form,
  });

  const responseText = await response.text().catch(() => '');
  if (!response.ok) {
    throw new Error(`Webhook upload failed with HTTP ${response.status}: ${responseText.slice(0, 1000)}`);
  }

  console.log(`Uploaded ${filePaths.length} file(s) to ${WEBHOOK_URL}`);
  if (responseText) {
    console.log(responseText.slice(0, 1000));
  }
}

async function main() {
  const args = process.argv.slice(2);

  if (args.includes('--help') || args.includes('-h')) {
    printUsage();
    return;
  }

  const files = await resolveInputFiles(args);
  await uploadFiles(files);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
