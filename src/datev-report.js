#!/usr/bin/env node

const fsSync = require('fs');
const fs = require('fs/promises');
const http = require('http');
const path = require('path');
const { execFile } = require('child_process');
const { promisify } = require('util');
const readline = require('readline');
const { Readable } = require('stream');
const dotenv = require('dotenv');
const { chromium } = require('playwright');

dotenv.config();

const execFileAsync = promisify(execFile);

const BASE_URL = process.env.IMMO_BASE_URL || 'https://app.immocloud.de';
const LOGIN_URL = new URL('/login', BASE_URL).toString();
const DATEV_URL = new URL('/settings/datev', BASE_URL).toString();
const DOWNLOAD_DIR = path.resolve(process.env.DOWNLOAD_DIR || 'downloads');
const AUTH_STATE_FILE = path.resolve(process.env.AUTH_STATE_FILE || '.auth/immocloud-storage-state.json');
const KOST1_MAP_FILE = process.env.DATEV_KOST1_MAP_FILE
  ? path.resolve(process.env.DATEV_KOST1_MAP_FILE)
  : null;
const DATEV_PROCESS_FILES = parseBoolean(process.env.DATEV_PROCESS_FILES, true);
const GOOGLE_DRIVE_UPLOAD = parseBoolean(process.env.GOOGLE_DRIVE_UPLOAD, false);
const GOOGLE_DRIVE_FOLDER_ID = (process.env.GOOGLE_DRIVE_FOLDER_ID || '').trim();
const GOOGLE_DRIVE_CREDENTIALS_FILE = (process.env.GOOGLE_DRIVE_CREDENTIALS_FILE || process.env.GOOGLE_APPLICATION_CREDENTIALS || '').trim();
const GOOGLE_DRIVE_CREDENTIALS_JSON = (process.env.GOOGLE_DRIVE_CREDENTIALS_JSON || '').trim();
const WEBHOOK_UPLOAD = parseBoolean(process.env.WEBHOOK_UPLOAD, false);
const WEBHOOK_URL = (process.env.WEBHOOK_URL || '').trim();
const WEBHOOK_TOKEN = (process.env.WEBHOOK_TOKEN || '').trim();
const WEBHOOK_FILE_FIELD = (process.env.WEBHOOK_FILE_FIELD || 'files').trim();
const GOOGLE_SHEETS_IMPORT = parseBoolean(process.env.GOOGLE_SHEETS_IMPORT, false);
const GOOGLE_OAUTH_CLIENT_ID = (process.env.GOOGLE_OAUTH_CLIENT_ID || '').trim();
const GOOGLE_OAUTH_CLIENT_SECRET = (process.env.GOOGLE_OAUTH_CLIENT_SECRET || '').trim();
const GOOGLE_OAUTH_REDIRECT_URI = (process.env.GOOGLE_OAUTH_REDIRECT_URI || 'http://127.0.0.1:3000/oauth2callback').trim();
const GOOGLE_OAUTH_TOKEN_FILE = path.resolve(process.env.GOOGLE_OAUTH_TOKEN_FILE || '.auth/google-oauth-token.json');
const GOOGLE_OAUTH_TIMEOUT_MS = Number(process.env.GOOGLE_OAUTH_TIMEOUT_MS || 300_000);
const GOOGLE_SHEETS_DEFAULT_TAB_TITLE = 'List';
const GOOGLE_SHEETS_WEBHOOK_URL = (
  process.env.GOOGLE_SHEETS_WEBHOOK_URL
  || 'https://n8n.srv1309825.hstgr.cloud/webhook/4483f020-c90a-44df-b336-ff286848aee9'
).trim();
const GOOGLE_SHEETS_WEBHOOK_ENABLED = parseBoolean(process.env.GOOGLE_SHEETS_WEBHOOK_ENABLED, true);
const GOOGLE_SHEETS_WEBHOOK_TOKEN = (process.env.GOOGLE_SHEETS_WEBHOOK_TOKEN || '').trim();
const HEADLESS = parseBoolean(process.env.HEADLESS, true);
const PERSIST_AUTH = parseBoolean(process.env.PERSIST_AUTH, true);
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS || 120_000);
const GOOGLE_AUTH_SCOPES = [
  'https://www.googleapis.com/auth/drive.file',
  'https://www.googleapis.com/auth/spreadsheets',
];

const LABELS = {
  loginButton: 'Anmelden',
  reportButton: 'Jetzt erstellen',
  period: 'Zeitraum',
  owner: 'Eigentümer',
  ownerRestriction: 'Objekte auf Eigentümer einschränken',
  fiscalYear: 'Wirtschaftsjahr Beginn',
  keepGoing: 'Trotzdem exportieren',
  datevTitle: 'DATEV-Export',
};

function parseBoolean(value, fallback) {
  if (value == null || value === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
}

function parseDate(value) {
  if (!value) return null;

  if (value instanceof Date) return value;

  const raw = String(value).trim();
  if (!raw) return null;

  const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    const [, year, month, day] = isoMatch;
    return new Date(Number(year), Number(month) - 1, Number(day));
  }

  const deMatch = raw.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  if (deMatch) {
    const [, day, month, year] = deMatch;
    return new Date(Number(year), Number(month) - 1, Number(day));
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Cannot parse date value: ${raw}`);
  }
  return parsed;
}

function startOfMonth(date) {
  return new Date(date.getFullYear(), date.getMonth(), 1);
}

function endOfMonth(date) {
  return new Date(date.getFullYear(), date.getMonth() + 1, 0);
}

function previousMonthRange(reference = new Date()) {
  const previous = new Date(reference.getFullYear(), reference.getMonth() - 1, 1);
  return {
    start: startOfMonth(previous),
    end: endOfMonth(previous),
  };
}

function formatDate(date) {
  const day = String(date.getDate()).padStart(2, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const year = String(date.getFullYear());
  return `${day}.${month}.${year}`;
}

function formatIsoDate(date) {
  const day = String(date.getDate()).padStart(2, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const year = String(date.getFullYear());
  return `${year}-${month}-${day}`;
}

function parseCsvOrDateRange(raw) {
  if (!raw) return null;
  const values = String(raw)
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean);
  if (values.length === 0) return null;
  return values;
}

function parseDelimitedText(input, delimiter = ';') {
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;

  for (let i = 0; i < input.length; i += 1) {
    const ch = input[i];
    const next = input[i + 1];

    if (inQuotes) {
      if (ch === '"') {
        if (next === '"') {
          field += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
      continue;
    }

    if (ch === delimiter) {
      row.push(field);
      field = '';
      continue;
    }

    if (ch === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
      continue;
    }

    if (ch === '\r') {
      continue;
    }

    field += ch;
  }

  if (field.length || row.length) {
    row.push(field);
    rows.push(row);
  }

  return rows;
}

function stringifyDelimitedText(rows, delimiter = ';') {
  const escapeCell = (value) => {
    const raw = value == null ? '' : String(value);
    const escaped = raw.replace(/"/g, '""');
    if (/[\"\n\r;]/.test(raw)) {
      return `"${escaped}"`;
    }
    return raw;
  };

  return `\uFEFF${rows.map((row) => row.map(escapeCell).join(delimiter)).join('\n')}`;
}

function normalizeText(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function parseAccountNumber(value) {
  const raw = String(value || '').trim().replace(/\./g, '');
  if (!raw) return null;
  const parsed = Number.parseInt(raw, 10);
  return Number.isNaN(parsed) ? null : parsed;
}

function readMapSourceFile(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const content = fs.readFile(filePath, 'utf8');
  return content.then((text) => {
    if (ext === '.json') {
      return JSON.parse(text);
    }

    const rows = parseDelimitedText(text.replace(/^\uFEFF/, ''), ';');
    if (rows.length < 2) {
      return [];
    }

    const headers = rows[0].map((header) => header.trim());
    return rows.slice(1)
      .filter((row) => row.some((cell) => String(cell || '').trim() !== ''))
      .map((row) => {
        const entry = {};
        headers.forEach((header, index) => {
          entry[header] = row[index] ?? '';
        });
        return entry;
      });
  });
}

function normalizeKost1Rules(rawRules) {
  const rules = Array.isArray(rawRules) ? rawRules : [];

  return rules.map((rule) => {
    const match = rule.match ?? rule.pattern ?? rule.regex ?? rule.search ?? rule.value ?? '';
    const costCenter = rule.datevKostenStelle ?? rule.costCenter ?? rule.kost1 ?? rule.KOST1 ?? '';
    const field = rule.field ?? rule.sourceField ?? rule.column ?? '';
    if (!match || !costCenter) return null;

    return {
      field: String(field || '').trim(),
      match: String(match).trim(),
      costCenter: String(costCenter).trim(),
    };
  }).filter(Boolean);
}

function matchesRule(rule, candidate) {
  const value = String(candidate || '');
  if (!value) return false;

  const raw = rule.match;
  if (raw.startsWith('re:')) {
    return new RegExp(raw.slice(3), 'i').test(value);
  }

  if (raw.startsWith('/') && raw.lastIndexOf('/') > 0) {
    const lastSlash = raw.lastIndexOf('/');
    const pattern = raw.slice(1, lastSlash);
    const flags = raw.slice(lastSlash + 1) || 'i';
    return new RegExp(pattern, flags).test(value);
  }

  return normalizeText(value).includes(normalizeText(raw));
}

async function loadKost1Rules() {
  if (!KOST1_MAP_FILE) {
    return [];
  }

  const rawRules = await readMapSourceFile(KOST1_MAP_FILE);
  return normalizeKost1Rules(rawRules);
}

function resolveKost1Value(row, rules) {
  if (!rules.length) return '';

  const candidates = [
    row['Zusatzinformation - Inhalt 2'],
    row['Zusatzinformation - Inhalt 1'],
    row['Beleginfo - Inhalt 1'],
    row['Buchungstext'],
    row['Abrechnungsreferenz'],
    row['BVV-Position'],
  ].filter(Boolean);

  for (const rule of rules) {
    if (rule.field) {
      const fieldValue = row[rule.field];
      if (matchesRule(rule, fieldValue)) {
        return rule.costCenter;
      }
      continue;
    }

    if (candidates.some((candidate) => matchesRule(rule, candidate))) {
      return rule.costCenter;
    }
  }

  return '';
}

function transformBuchungsstapelCsv(text, kost1Rules) {
  const rows = parseDelimitedText(text.replace(/^\uFEFF/, ''), ';');
  if (rows.length < 3) {
    throw new Error('Buchungsstapel CSV does not contain enough rows.');
  }

  const headers = rows[1];
  const indexByHeader = new Map(headers.map((header, index) => [header, index]));
  const requiredHeaders = [
    'Konto',
    'Gegenkonto (ohne BU-Schlüssel)',
    'Belegfeld 1',
    'KOST1 - Kostenstelle',
    'Zusatzinformation - Inhalt 1',
    'Zusatzinformation - Inhalt 2',
    'Buchungstext',
  ];

  for (const header of requiredHeaders) {
    if (!indexByHeader.has(header)) {
      throw new Error(`Missing required DATEV column: ${header}`);
    }
  }

  const kontoIndex = indexByHeader.get('Konto');
  const gegenkontoIndex = indexByHeader.get('Gegenkonto (ohne BU-Schlüssel)');
  const belegfeld1Index = indexByHeader.get('Belegfeld 1');
  const kost1Index = indexByHeader.get('KOST1 - Kostenstelle');
  const zusatz1Index = indexByHeader.get('Zusatzinformation - Inhalt 1');
  const zusatz2Index = indexByHeader.get('Zusatzinformation - Inhalt 2');

  const keptRows = [rows[0], rows[1]];
  const stats = {
    deletedRows: 0,
    updatedAccountRows: 0,
    updatedKost1Rows: 0,
  };

  for (const row of rows.slice(2)) {
    const konto = parseAccountNumber(row[kontoIndex]);
    const gegenkonto = parseAccountNumber(row[gegenkontoIndex]);

    if (
      (konto != null && konto >= 70000)
      || (gegenkonto != null && gegenkonto >= 70000)
      || konto === 8300
    ) {
      stats.deletedRows += 1;
      continue;
    }

    row[belegfeld1Index] = '';

    const zusatz2 = String(row[zusatz2Index] || '').trim();
    if (/^kleyerstr/i.test(zusatz2) && konto !== 1732) {
      row[kontoIndex] = '8301';
      stats.updatedAccountRows += 1;
    } else if (row[kontoIndex] === '1200') {
      row[kontoIndex] = '1360';
      stats.updatedAccountRows += 1;
    } else if (row[kontoIndex] === '2751') {
      row[kontoIndex] = '8105';
      stats.updatedAccountRows += 1;
    }

    const kost1Value = resolveKost1Value(
      {
        'Zusatzinformation - Inhalt 1': row[zusatz1Index],
        'Zusatzinformation - Inhalt 2': row[zusatz2Index],
        'Buchungstext': row[indexByHeader.get('Buchungstext')],
        'Beleginfo - Inhalt 1': row[indexByHeader.get('Beleginfo - Inhalt 1')],
        'Abrechnungsreferenz': row[indexByHeader.get('Abrechnungsreferenz')],
        'BVV-Position': row[indexByHeader.get('BVV-Position')],
      },
      kost1Rules,
    );

    if (kost1Value) {
      row[kost1Index] = kost1Value;
      stats.updatedKost1Rows += 1;
    }

    keptRows.push(row);
  }

  return {
    text: stringifyDelimitedText(keptRows, ';'),
    stats,
  };
}

async function processExtractedDatevFiles(extractedDir, kost1Rules) {
  const processedDir = path.join(extractedDir, 'processed');
  await fs.mkdir(processedDir, { recursive: true });

  const entries = await fs.readdir(extractedDir);
  const processedFiles = [];
  const transformed = [];

  for (const entry of entries) {
    const sourcePath = path.join(extractedDir, entry);
    const stat = await fs.stat(sourcePath);
    if (!stat.isFile()) continue;

    const targetPath = path.join(processedDir, entry);
    if (/^EXTF_Buchungsstapel/i.test(entry) && entry.toLowerCase().endsWith('.csv')) {
      const sourceText = await fs.readFile(sourcePath, 'utf8');
      const result = transformBuchungsstapelCsv(sourceText, kost1Rules);
      await fs.writeFile(targetPath, result.text, 'utf8');
      processedFiles.push(targetPath);
      transformed.push({ file: entry, ...result.stats });
    } else if (entry.toLowerCase().endsWith('.csv')) {
      await fs.copyFile(sourcePath, targetPath);
      processedFiles.push(targetPath);
    }
  }

  return { processedDir, processedFiles, transformed };
}

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function loadStorageState() {
  if (!PERSIST_AUTH) return undefined;
  try {
    await fs.access(AUTH_STATE_FILE);
    return AUTH_STATE_FILE;
  } catch {
    return undefined;
  }
}

async function saveStorageState(context) {
  if (!PERSIST_AUTH) return;
  await ensureDir(path.dirname(AUTH_STATE_FILE));
  await context.storageState({ path: AUTH_STATE_FILE });
}

async function clearStorageState() {
  if (!PERSIST_AUTH) return;
  await fs.rm(AUTH_STATE_FILE, { force: true }).catch(() => {});
}

async function uploadFilesToGoogleDrive(filePaths) {
  if (!GOOGLE_DRIVE_UPLOAD) {
    return [];
  }

  if (!GOOGLE_DRIVE_FOLDER_ID) {
    throw new Error('GOOGLE_DRIVE_FOLDER_ID is required when GOOGLE_DRIVE_UPLOAD=true.');
  }

  const hasInlineCredentials = GOOGLE_DRIVE_CREDENTIALS_JSON.length > 0;
  const hasFileCredentials = GOOGLE_DRIVE_CREDENTIALS_FILE.length > 0;
  if (!hasInlineCredentials && !hasFileCredentials) {
    throw new Error('Set GOOGLE_DRIVE_CREDENTIALS_FILE (or GOOGLE_APPLICATION_CREDENTIALS) or GOOGLE_DRIVE_CREDENTIALS_JSON for Drive uploads.');
  }

  const { google } = require('googleapis');
  let authConfig;

  if (hasInlineCredentials) {
    authConfig = { credentials: JSON.parse(GOOGLE_DRIVE_CREDENTIALS_JSON) };
  } else {
    authConfig = { keyFile: path.resolve(GOOGLE_DRIVE_CREDENTIALS_FILE) };
  }

  const auth = new google.auth.GoogleAuth({
    ...authConfig,
    scopes: ['https://www.googleapis.com/auth/drive.file'],
  });

  const drive = google.drive({ version: 'v3', auth });
  const uploaded = [];

  for (const filePath of filePaths) {
    const absolutePath = path.resolve(filePath);
    const fileName = path.basename(absolutePath);

    await fs.access(absolutePath);

    let response;
    try {
      response = await drive.files.create({
        requestBody: {
          name: fileName,
          parents: [GOOGLE_DRIVE_FOLDER_ID],
        },
        media: {
          mimeType: 'text/csv',
          body: fsSync.createReadStream(absolutePath),
        },
        fields: 'id,name,webViewLink,webContentLink',
        supportsAllDrives: true,
      });
    } catch (error) {
      const message = String(
        error?.response?.data?.error?.message
        || error?.message
        || '',
      );

      if (message.includes('Service Accounts do not have storage quota')) {
        throw new Error(
          [
            'Google Drive upload failed: the target folder is not usable with a plain service account in My Drive.',
            'Use a Shared Drive and add the service account there, or switch to OAuth/domain-wide delegation.',
            `Folder ID: ${GOOGLE_DRIVE_FOLDER_ID}`,
          ].join(' '),
        );
      }

      if (message.includes('File not found')) {
        throw new Error(
          [
            'Google Drive upload failed: folder not found or not shared with the service account.',
            `Folder ID: ${GOOGLE_DRIVE_FOLDER_ID}`,
          ].join(' '),
        );
      }

      throw error;
    }

    uploaded.push({
      localPath: absolutePath,
      id: response.data.id || '',
      name: response.data.name || fileName,
      webViewLink: response.data.webViewLink || null,
      webContentLink: response.data.webContentLink || null,
    });
  }

  return uploaded;
}

async function uploadFilesToWebhook(filePaths, metadata = {}) {
  if (!WEBHOOK_UPLOAD) {
    return null;
  }

  if (!WEBHOOK_URL) {
    throw new Error('WEBHOOK_URL is required when WEBHOOK_UPLOAD=true.');
  }

  if (typeof fetch !== 'function' || typeof FormData !== 'function' || typeof Blob !== 'function') {
    throw new Error('Webhook upload requires Node.js 18+ with fetch, FormData, and Blob support.');
  }

  const csvFiles = filePaths.filter((filePath) => filePath.toLowerCase().endsWith('.csv'));
  if (csvFiles.length === 0) {
    throw new Error('Webhook upload is enabled, but no original CSV files were found.');
  }

  const form = new FormData();
  for (const [key, value] of Object.entries(metadata)) {
    if (value != null) {
      form.append(key, String(value));
    }
  }

  for (const filePath of csvFiles) {
    const absolutePath = path.resolve(filePath);
    const fileName = path.basename(absolutePath);
    const buffer = await fs.readFile(absolutePath);
    form.append(WEBHOOK_FILE_FIELD, new Blob([buffer], { type: 'text/csv;charset=utf-8' }), fileName);
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

  return {
    url: WEBHOOK_URL,
    status: response.status,
    fileField: WEBHOOK_FILE_FIELD,
    files: csvFiles.map((filePath) => path.resolve(filePath)),
    response: responseText.slice(0, 1000),
  };
}

function stripFirstLine(buffer) {
  const text = buffer.toString('utf8');
  const trimmed = text.replace(/^\uFEFF/, '').replace(/^.*(?:\r?\n|$)/, '');
  return Buffer.from(trimmed, 'utf8');
}

function createGoogleAuth() {
  if (!GOOGLE_DRIVE_FOLDER_ID) {
    throw new Error('GOOGLE_DRIVE_FOLDER_ID is required for Google Sheets import.');
  }

  if (GOOGLE_OAUTH_CLIENT_ID && GOOGLE_OAUTH_CLIENT_SECRET) {
    return authorizeWithOAuth();
  }

  return createServiceAccountSheetsAuth();
}

function createServiceAccountSheetsAuth() {
  const hasInlineCredentials = GOOGLE_DRIVE_CREDENTIALS_JSON.length > 0;
  const hasFileCredentials = GOOGLE_DRIVE_CREDENTIALS_FILE.length > 0;

  if (!hasInlineCredentials && !hasFileCredentials) {
    throw new Error(
      'Set GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET for My Drive OAuth, or provide GOOGLE_DRIVE_CREDENTIALS_FILE / GOOGLE_DRIVE_CREDENTIALS_JSON.',
    );
  }

  let authConfig = null;

  if (hasInlineCredentials) {
    try {
      authConfig = { credentials: JSON.parse(GOOGLE_DRIVE_CREDENTIALS_JSON) };
    } catch (error) {
      if (!hasFileCredentials) {
        throw new Error(`GOOGLE_DRIVE_CREDENTIALS_JSON is invalid JSON: ${error.message}`);
      }
    }
  }

  if (!authConfig && hasFileCredentials) {
    authConfig = { keyFile: path.resolve(GOOGLE_DRIVE_CREDENTIALS_FILE) };
  }

  const { google } = require('googleapis');
  return new google.auth.GoogleAuth({
    ...authConfig,
    scopes: GOOGLE_AUTH_SCOPES,
  });
}

async function readCachedOAuthTokens() {
  try {
    const raw = await fs.readFile(GOOGLE_OAUTH_TOKEN_FILE, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function writeOAuthTokens(tokens) {
  await ensureDir(path.dirname(GOOGLE_OAUTH_TOKEN_FILE));
  await fs.writeFile(GOOGLE_OAUTH_TOKEN_FILE, JSON.stringify(tokens, null, 2));
}

function isInvalidGrantError(error) {
  return error?.message === 'invalid_grant'
    || error?.response?.data?.error === 'invalid_grant'
    || error?.errors?.some?.((entry) => entry?.reason === 'invalid_grant');
}

function askForCodeInteractively(authUrl) {
  return new Promise((resolve) => {
    console.log('Open this URL, approve access, then paste the returned code:');
    console.log(authUrl);

    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });

    rl.question('Authorization code: ', (answer) => {
      rl.close();
      resolve(String(answer || '').trim());
    });
  });
}

function waitForOAuthCodeViaHttpServer(authUrl, redirectUri) {
  return new Promise((resolve, reject) => {
    const redirect = new URL(redirectUri);
    const server = http.createServer((req, res) => {
      const requestUrl = new URL(req.url || '/', redirectUri);
      const code = requestUrl.searchParams.get('code');
      const error = requestUrl.searchParams.get('error');

      if (error) {
        res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end(`OAuth failed: ${error}`);
        server.close(() => reject(new Error(`OAuth failed: ${error}`)));
        return;
      }

      if (!code) {
        res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Missing OAuth code.');
        return;
      }

      res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Google authorization received. You can close this tab and return to the terminal.');
      server.close(() => resolve(code));
    });

    const timer = setTimeout(() => {
      server.close(() => reject(new Error('Timed out waiting for Google OAuth callback.')));
    }, GOOGLE_OAUTH_TIMEOUT_MS);

    server.on('close', () => clearTimeout(timer));
    server.on('error', (error) => reject(error));
    server.listen(Number(redirect.port || 80), redirect.hostname, () => {
      console.log('Open this URL to authorize Google Drive access:');
      console.log(authUrl);
    });
  });
}

async function getOAuthCode(oauth2Client) {
  const authUrl = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    prompt: 'consent',
    scope: GOOGLE_AUTH_SCOPES,
  });

  if (/^http:\/\/(127\.0\.0\.1|localhost)/i.test(GOOGLE_OAUTH_REDIRECT_URI)) {
    return waitForOAuthCodeViaHttpServer(authUrl, GOOGLE_OAUTH_REDIRECT_URI);
  }

  return askForCodeInteractively(authUrl);
}

async function authorizeWithOAuth() {
  const { google } = require('googleapis');
  const oauth2Client = new google.auth.OAuth2(
    GOOGLE_OAUTH_CLIENT_ID,
    GOOGLE_OAUTH_CLIENT_SECRET,
    GOOGLE_OAUTH_REDIRECT_URI,
  );

  oauth2Client.on('tokens', async (tokens) => {
    const cached = (await readCachedOAuthTokens()) || {};
    await writeOAuthTokens({
      ...cached,
      ...tokens,
      refresh_token: tokens.refresh_token || cached.refresh_token || null,
    });
  });

  const authorizeFresh = async () => {
    const code = await getOAuthCode(oauth2Client);
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);
    await writeOAuthTokens(tokens);
    return oauth2Client;
  };

  const cachedTokens = await readCachedOAuthTokens();
  if (cachedTokens) {
    oauth2Client.setCredentials(cachedTokens);
    try {
      await oauth2Client.getAccessToken();
      return oauth2Client;
    } catch (error) {
      if (!isInvalidGrantError(error)) throw error;
      await fs.unlink(GOOGLE_OAUTH_TOKEN_FILE).catch(() => {});
      console.log(`Cached Google OAuth token is invalid. Re-authorizing ${GOOGLE_OAUTH_TOKEN_FILE}.`);
    }
  }

  return authorizeFresh();
}

async function ensureSpreadsheetDefaultSheetTitle(sheetsClient, spreadsheetId) {
  try {
    const response = await sheetsClient.spreadsheets.get({
      spreadsheetId,
      fields: 'sheets(properties(sheetId,title))',
    });

    const sheets = response.data.sheets || [];
    if (sheets.some((sheet) => sheet?.properties?.title === GOOGLE_SHEETS_DEFAULT_TAB_TITLE)) {
      return;
    }

    const firstSheetId = sheets[0]?.properties?.sheetId;
    if (!Number.isInteger(firstSheetId)) {
      throw new Error('First sheet is missing a numeric sheetId.');
    }

    await sheetsClient.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: {
        requests: [
          {
            updateSheetProperties: {
              properties: {
                sheetId: firstSheetId,
                title: GOOGLE_SHEETS_DEFAULT_TAB_TITLE,
              },
              fields: 'title',
            },
          },
        ],
      },
    });
  } catch (error) {
    const message = String(error?.message || error);
    throw new Error(`Failed to set sheet title to "${GOOGLE_SHEETS_DEFAULT_TAB_TITLE}" for spreadsheet ${spreadsheetId}: ${message}`);
  }
}

async function importCsvFilesToGoogleSheets(filePaths) {
  if (!GOOGLE_SHEETS_IMPORT) {
    return [];
  }

  const csvFiles = filePaths.filter((filePath) => filePath.toLowerCase().endsWith('.csv'));
  if (csvFiles.length === 0) {
    return [];
  }

  const auth = await createGoogleAuth();
  const { google } = require('googleapis');
  const drive = google.drive({ version: 'v3', auth });
  const sheets = google.sheets({ version: 'v4', auth });
  const imported = [];

  for (const filePath of csvFiles) {
    const absolutePath = path.resolve(filePath);
    const fileName = path.basename(absolutePath, path.extname(absolutePath));
    const sourceBuffer = await fs.readFile(absolutePath);
    const csvBuffer = stripFirstLine(sourceBuffer);

    let response;
    try {
      response = await drive.files.create({
        requestBody: {
          name: fileName,
          mimeType: 'application/vnd.google-apps.spreadsheet',
          parents: [GOOGLE_DRIVE_FOLDER_ID],
        },
        media: {
          mimeType: 'text/csv',
          body: Readable.from(csvBuffer),
        },
        fields: 'id,name,webViewLink',
        supportsAllDrives: true,
      });
    } catch (error) {
      const message = String(
        error?.response?.data?.error?.message
        || error?.message
        || '',
      );

      if (message.includes('Service Accounts do not have storage quota')) {
        throw new Error(
          [
            'Google Sheets import failed: the target folder is not usable with a plain service account in My Drive.',
            'Use a Shared Drive and add the service account there, or switch to OAuth/domain-wide delegation.',
            `Folder ID: ${GOOGLE_DRIVE_FOLDER_ID}`,
          ].join(' '),
        );
      }

      if (message.includes('File not found')) {
        throw new Error(
          [
            'Google Sheets import failed: folder not found or not shared with the service account.',
            `Folder ID: ${GOOGLE_DRIVE_FOLDER_ID}`,
          ].join(' '),
        );
      }

      throw error;
    }

    const spreadsheetId = response.data.id || '';
    if (!spreadsheetId) {
      throw new Error('Google Sheets import failed: missing spreadsheet ID in the Drive API response.');
    }

    await ensureSpreadsheetDefaultSheetTitle(sheets, spreadsheetId);

    imported.push({
      localPath: absolutePath,
      id: spreadsheetId,
      name: response.data.name || fileName,
      webViewLink: response.data.webViewLink || null,
    });
  }

  return imported;
}

async function sendImportedSheetsToWebhook(imported, metadata = {}) {
  if (!GOOGLE_SHEETS_WEBHOOK_ENABLED || imported.length === 0) {
    return null;
  }

  if (!GOOGLE_SHEETS_WEBHOOK_URL) {
    throw new Error('GOOGLE_SHEETS_WEBHOOK_URL is required when GOOGLE_SHEETS_WEBHOOK_ENABLED=true.');
  }

  if (typeof fetch !== 'function') {
    throw new Error('Webhook delivery requires Node.js 18+ with fetch support.');
  }

  const payload = {
    ...metadata,
    folderId: GOOGLE_DRIVE_FOLDER_ID,
    count: imported.length,
    files: imported.map((item) => ({
      id: item.id,
      name: item.name,
      url: item.webViewLink,
      localPath: item.localPath,
    })),
  };

  const headers = {
    'Content-Type': 'application/json',
  };

  if (GOOGLE_SHEETS_WEBHOOK_TOKEN) {
    headers.Authorization = `Bearer ${GOOGLE_SHEETS_WEBHOOK_TOKEN}`;
  }

  const response = await fetch(GOOGLE_SHEETS_WEBHOOK_URL, {
    method: 'POST',
    headers,
    body: JSON.stringify(payload),
  });

  const responseText = await response.text().catch(() => '');
  if (!response.ok) {
    throw new Error(`Google Sheets webhook failed with HTTP ${response.status}: ${responseText.slice(0, 1000)}`);
  }

  return {
    url: GOOGLE_SHEETS_WEBHOOK_URL,
    status: response.status,
    response: responseText.slice(0, 1000),
  };
}

async function maybeDismissCookieBanner(page) {
  for (const name of ['Ablehnen', 'Akzeptieren', 'Alle akzeptieren', 'Accept all']) {
    const button = page.getByRole('button', { name });
    if (await button.count()) {
      try {
        await button.first().click({ timeout: 3_000, force: true });
        return;
      } catch {
        // Ignore banners that are present but not actionable.
      }
    }
  }
}

async function waitForVisibleText(page, text, timeout = TIMEOUT_MS) {
  await page.getByText(text, { exact: false }).first().waitFor({ state: 'visible', timeout });
}

async function waitForAnyButton(page, names, timeout = TIMEOUT_MS) {
  const deadline = Date.now() + timeout;
  let lastError = null;

  for (const name of names) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) break;
    try {
      const button = page.getByRole('button', { name });
      await button.first().waitFor({ state: 'visible', timeout: remaining });
      return button.first();
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error(`None of the buttons were visible: ${names.join(', ')}`);
}

async function isLoginScreenVisible(page) {
  const loginFormVisible = await page.locator('form').filter({ hasText: LABELS.loginButton }).isVisible().catch(() => false);
  if (loginFormVisible) return true;

  const emailInputVisible = await page.locator('input[type="email"]').isVisible().catch(() => false);
  const passwordInputVisible = await page.locator('input[type="password"]').isVisible().catch(() => false);
  return emailInputVisible && passwordInputVisible;
}

async function waitForDatevReady(page, timeout = TIMEOUT_MS) {
  const deadline = Date.now() + timeout;
  let lastState = 'unknown';

  while (Date.now() < deadline) {
    await maybeDismissCookieBanner(page);

    if (await isLoginScreenVisible(page)) {
      lastState = 'login';
      throw new Error('Login screen is visible instead of DATEV export page.');
    }

    const exportButtonVisible = await page.getByRole('button', { name: /Jetzt erstellen|Bericht erstellen/i }).first().isVisible().catch(() => false);
    const periodVisible = await page.getByText(LABELS.period, { exact: false }).first().isVisible().catch(() => false);
    const ownerVisible = await page.getByText(LABELS.owner, { exact: false }).first().isVisible().catch(() => false);
    const fiscalVisible = await page.getByText(LABELS.fiscalYear, { exact: false }).first().isVisible().catch(() => false);
    const datevVisible = await page.getByText(LABELS.datevTitle, { exact: false }).first().isVisible().catch(() => false);

    if ((exportButtonVisible && periodVisible && ownerVisible) || (datevVisible && fiscalVisible)) {
      return;
    }

    lastState = JSON.stringify({
      url: page.url(),
      exportButtonVisible,
      periodVisible,
      ownerVisible,
      fiscalVisible,
      datevVisible,
    });

    await page.waitForTimeout(1000);
  }

  throw new Error(`DATEV page did not become ready. Last observed state: ${lastState}`);
}

async function login(page) {
  await page.goto(LOGIN_URL, { waitUntil: 'domcontentloaded' });
  await maybeDismissCookieBanner(page);

  await page.locator('input[type="email"]').fill(process.env.IMMO_EMAIL || '');
  await page.locator('input[type="password"]').fill(process.env.IMMO_PASSWORD || '');
  await page.getByRole('button', { name: /Anmelden|Login/ }).click({ force: true });

  await page.waitForLoadState('domcontentloaded').catch(() => {});
  await page.waitForTimeout(3000);
  await maybeDismissCookieBanner(page);
  await page.goto(DATEV_URL, { waitUntil: 'domcontentloaded' });

  if (await isLoginScreenVisible(page)) {
    throw new Error('Login failed. Immocloud still shows the login screen after submitting credentials.');
  }
}

async function ensureAuthenticated(page) {
  await page.goto(DATEV_URL, { waitUntil: 'domcontentloaded' });
  await maybeDismissCookieBanner(page);

  if (page.url().includes('/login') || await isLoginScreenVisible(page)) {
    await login(page);
    await page.goto(DATEV_URL, { waitUntil: 'domcontentloaded' });
  }

  try {
    await waitForDatevReady(page, 30_000);
  } catch {
    if (await isLoginScreenVisible(page) || page.url().includes('/login')) {
      await clearStorageState();
      await login(page);
    } else {
      await page.goto(DATEV_URL, { waitUntil: 'domcontentloaded' });
    }

    if (await isLoginScreenVisible(page) || page.url().includes('/login')) {
      await clearStorageState();
      await login(page);
    }

    await page.goto(DATEV_URL, { waitUntil: 'domcontentloaded' });
    await waitForDatevReady(page, TIMEOUT_MS);
  }
}

async function getFieldInput(page, labelText) {
  const column = page.locator('div.col-4').filter({ hasText: labelText }).first();
  await column.waitFor({ state: 'visible', timeout: TIMEOUT_MS });

  const input = column.locator('input').first();
  if (await input.count()) return input;

  return column.locator('[role="combobox"], .p-multiselect, button').first();
}

async function fillDateRange(page, startDate, endDate) {
  const input = await getFieldInput(page, LABELS.period);
  await input.click({ clickCount: 3 });
  await input.fill(`${formatDate(startDate)} - ${formatDate(endDate)}`);
  await input.press('Tab');
}

async function fillFiscalYear(page, fiscalYearStart) {
  const input = await getFieldInput(page, LABELS.fiscalYear);
  await input.click({ clickCount: 3 });
  await input.fill(formatDate(fiscalYearStart));
  await input.press('Tab');
}

function getOwnerColumn(page) {
  return page.locator(
    'xpath=//div[contains(concat(" ", normalize-space(@class), " "), " col-4 ")][.//div[contains(concat(" ", normalize-space(@class), " "), " input-title ") and normalize-space(.)="Eigentümer"]]',
  ).first();
}

async function ensureToggleEnabled(page, labelText) {
  const row = page.locator(`xpath=//p[normalize-space(.)="${labelText}"]/..`).first();
  if (!(await row.count().catch(() => 0))) return false;

  const input = row.locator('input[type="checkbox"], .p-toggleswitch-input').first();
  if (!(await input.count().catch(() => 0))) return false;

  const checked = await input.isChecked().catch(async () => {
    const root = row.locator('.p-toggleswitch').first();
    return (await root.getAttribute('data-p-checked').catch(() => 'true')) === 'true';
  });
  if (checked) return true;

  await input.evaluate((element) => element.click()).catch(async () => {
    await row.locator('.p-toggleswitch, .p-toggleswitch-slider').first().click({ force: true }).catch(() => {});
  });
  await page.waitForTimeout(750);

  return await input.isChecked().catch(async () => {
    const root = row.locator('.p-toggleswitch').first();
    return (await root.getAttribute('data-p-checked').catch(() => 'false')) === 'true';
  });
}

function getOwnerOptionLocator(page, ownerListId = '') {
  const selectors = [];
  if (ownerListId) {
    selectors.push(
      `#${ownerListId} [role="option"]`,
      `#${ownerListId} .p-multiselect-item`,
      `#${ownerListId} .p-multiselect-option`,
      `#${ownerListId} .p-select-option`,
      `#${ownerListId} [data-pc-section="option"]`,
      `#${ownerListId} li`,
    );
  }

  selectors.push(
    '[role="option"]',
    '.p-multiselect-item',
    '.p-multiselect-option',
    '.p-select-option',
    '[data-pc-section="option"]',
  );

  return page.locator(selectors.join(', '));
}

async function openOwnerSelector(page) {
  await page.keyboard.press('Escape').catch(() => {});

  const column = getOwnerColumn(page);
  await column.waitFor({ state: 'visible', timeout: TIMEOUT_MS });

  const multiselect = column.locator('.p-multiselect').first();
  const hiddenInput = column.locator('.p-hidden-accessible input[role="combobox"]').first();
  const dropdownTrigger = column.locator('.p-multiselect-dropdown').first();
  const labelTrigger = column.locator('.p-multiselect-label-container, .p-multiselect-label').first();
  const fallbackTrigger = column.locator('[role="combobox"], input, button').first();

  let ownerListId = await hiddenInput.getAttribute('aria-controls').catch(() => '');
  const clickTargets = [dropdownTrigger, labelTrigger, multiselect, hiddenInput, fallbackTrigger];

  for (const target of clickTargets) {
    if (!(await target.count().catch(() => 0))) continue;
    await target.scrollIntoViewIfNeeded().catch(() => {});
    await target.click({ force: true }).catch(() => {});
    await page.waitForTimeout(300);

    const expanded = await hiddenInput.getAttribute('aria-expanded').catch(() => '');
    ownerListId = ownerListId || await hiddenInput.getAttribute('aria-controls').catch(() => '');
    if (expanded === 'true') break;
  }

  const expandedAfterClicks = await hiddenInput.getAttribute('aria-expanded').catch(() => '');
  if (expandedAfterClicks !== 'true' && (await hiddenInput.count().catch(() => 0))) {
    await hiddenInput.focus().catch(() => {});
    await hiddenInput.press('Space').catch(() => {});
    await page.waitForTimeout(300);
  }

  const expandedAfterSpace = await hiddenInput.getAttribute('aria-expanded').catch(() => '');
  if (expandedAfterSpace !== 'true' && (await hiddenInput.count().catch(() => 0))) {
    await hiddenInput.press('ArrowDown').catch(() => {});
    await page.waitForTimeout(300);
  }

  if (ownerListId) {
    const ownerList = page.locator(`#${ownerListId}`).first();
    await ownerList.waitFor({ state: 'visible', timeout: 2_000 }).catch(() => {});
  }

  return { column, ownerListId };
}

async function readSelectedOwnerText(page) {
  const column = getOwnerColumn(page);
  const inputValue = await column.locator('input').first().inputValue().catch(() => '');
  if (inputValue.trim()) return inputValue.trim();

  const selectedLabel = await column
    .locator('.p-multiselect-label, .p-dropdown-label, [data-pc-section="label"]')
    .first()
    .innerText()
    .catch(() => '');
  if (selectedLabel.trim() && normalizeText(selectedLabel) !== 'empty') return selectedLabel.trim();

  return (await column.innerText().catch(() => ''))
    .split('\n')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .filter((entry) => normalizeText(entry) !== normalizeText(LABELS.owner))
    .join(' ')
    .trim();
}

async function tryTypeOwnerDirectly(page, requestedOwners) {
  if (!requestedOwners || requestedOwners.length === 0) return false;

  const column = getOwnerColumn(page);
  const input = column.locator('input').first();
  if (!(await input.count().catch(() => 0))) return false;

  const ownerName = String(requestedOwners[0] || '').trim();
  if (!ownerName) return false;

  await input.click({ force: true }).catch(() => {});
  await input.fill(ownerName).catch(() => {});
  await page.waitForTimeout(1000);
  await page.keyboard.press('ArrowDown').catch(() => {});
  await page.keyboard.press('Enter').catch(() => {});
  await page.keyboard.press('Tab').catch(() => {});
  await page.keyboard.press('Escape').catch(() => {});
  await page.waitForTimeout(500);

  const selectedOwner = await readSelectedOwnerText(page);
  return normalizeText(selectedOwner).includes(normalizeText(ownerName));
}

async function tryClickOwnerByVisibleText(page, requestedOwners, ownerListId = '') {
  if (!requestedOwners || requestedOwners.length === 0) return false;

  const optionLocator = getOwnerOptionLocator(page, ownerListId);
  for (const ownerName of requestedOwners) {
    const normalizedOwnerName = String(ownerName || '').trim();
    if (!normalizedOwnerName) continue;

    const ownerText = optionLocator.filter({ hasText: normalizedOwnerName }).first();
    const isVisible = await ownerText.isVisible({ timeout: 3_000 }).catch(() => false);
    if (!isVisible) continue;

    await ownerText.click({ force: true }).catch(() => {});
    await page.keyboard.press('Tab').catch(() => {});
    await page.keyboard.press('Escape').catch(() => {});
    await page.waitForTimeout(500);

    const selectedOwner = await readSelectedOwnerText(page);
    if (normalizeText(selectedOwner).includes(normalizeText(normalizedOwnerName))) return true;
  }

  return false;
}

async function saveOwnerSelectionDebugArtifacts(page) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const debugDir = path.join(DOWNLOAD_DIR, 'debug');
  await ensureDir(debugDir);

  const screenshotPath = path.join(debugDir, `owner-selection-${timestamp}.png`);
  const htmlPath = path.join(debugDir, `owner-selection-${timestamp}.html`);

  await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
  const html = await page.content().catch(() => '');
  if (html) await fs.writeFile(htmlPath, html, 'utf8').catch(() => {});

  return { screenshotPath, htmlPath };
}

async function selectOwners(page) {
  const ownerFilterProvided = Boolean(process.env.DATEV_OWNER_NAMES || process.env.DATEV_OWNER_IDS);
  const requested = parseCsvOrDateRange(process.env.DATEV_OWNER_NAMES || process.env.DATEV_OWNER_IDS || 'Arona GmbH');

  if (requested.length > 0) {
    await ensureToggleEnabled(page, LABELS.ownerRestriction);
  }

  const { ownerListId } = await openOwnerSelector(page);

  const clickedOwnerText = await tryClickOwnerByVisibleText(page, requested, ownerListId);
  if (clickedOwnerText) return;

  const optionLocator = getOwnerOptionLocator(page, ownerListId);
  await optionLocator.first().waitFor({ state: 'visible', timeout: TIMEOUT_MS }).catch(() => {});

  const options = [];
  const count = await optionLocator.count().catch(() => 0);
  for (let i = 0; i < count; i += 1) {
    const option = optionLocator.nth(i);
    const text = (await option.innerText().catch(() => '')).trim();
    if (text) options.push({ option, text });
  }

  if (options.length === 0) {
    const selectedOwner = await readSelectedOwnerText(page);
    const requestedOwnerAlreadySelected = requested && requested.length > 0
      ? requested.some((needle) => normalizeText(selectedOwner).includes(normalizeText(needle)))
      : Boolean(selectedOwner);

    if (requestedOwnerAlreadySelected) {
      await page.keyboard.press('Escape').catch(() => {});
      return;
    }

    const typedOwner = await tryTypeOwnerDirectly(page, requested);
    if (typedOwner) return;

    const debugArtifacts = await saveOwnerSelectionDebugArtifacts(page);
    throw new Error(
      `Could not read DATEV owner options from the dropdown. Debug saved to ${debugArtifacts.screenshotPath} and ${debugArtifacts.htmlPath}.`,
    );
  }

  const targets = requested && requested.length > 0
    ? options.filter(({ text }) => requested.some((needle) => text.includes(needle)))
    : options;

  if (targets.length === 0 && !ownerFilterProvided) {
    for (const { option } of options) {
      await option.click();
    }
    await page.keyboard.press('Escape').catch(() => {});
    return;
  }

  if (targets.length === 0) {
    throw new Error(`No owner options matched DATEV_OWNER_NAMES/DATEV_OWNER_IDS: ${requested.join(', ')}`);
  }

  for (const { option } of targets) {
    await option.click();
  }

  await page.keyboard.press('Escape').catch(() => {});
}

async function createExport(page) {
  const button = await waitForAnyButton(page, [LABELS.reportButton, 'Bericht erstellen', 'Jetzt erstellen']);

  await button.click();

  const acceptButton = page.getByRole('button', { name: LABELS.keepGoing }).first();
  await acceptButton.waitFor({ state: 'visible', timeout: 5_000 }).catch(() => {});
  if (await acceptButton.count()) {
    await acceptButton.click({ timeout: 5_000 });
  }
}

async function waitForNewExportRow(page) {
  const row = page.locator('.row-highlight-background').first();
  await row.waitFor({ state: 'visible', timeout: TIMEOUT_MS });
  return row;
}

async function downloadFromHighlightedRow(page, downloadDir) {
  const row = await waitForNewExportRow(page);

  const downloadPromise = page.waitForEvent('download', { timeout: TIMEOUT_MS });

  const downloadIcon = row.locator('.edit-button').first();
  await downloadIcon.click();

  const download = await downloadPromise;
  const suggestedName = download.suggestedFilename();
  const finalName = suggestedName || `datev-export${path.extname(suggestedName || '') || '.csv'}`;
  const targetPath = path.join(downloadDir, finalName);
  await download.saveAs(targetPath);

  const result = {
    archivePath: targetPath,
    extractedDir: null,
    extractedFiles: [],
    originalCsvFiles: targetPath.toLowerCase().endsWith('.csv') ? [targetPath] : [],
    processedDir: null,
    processedFiles: [],
    transformedFiles: [],
  };

  if (targetPath.toLowerCase().endsWith('.zip')) {
    const extractedDir = targetPath.slice(0, -4);
    await fs.mkdir(extractedDir, { recursive: true });
    await execFileAsync('unzip', ['-o', targetPath, '-d', extractedDir]);

    const files = await fs.readdir(extractedDir);
    result.extractedDir = extractedDir;
    result.extractedFiles = files.sort();
    result.originalCsvFiles = result.extractedFiles
      .filter((fileName) => fileName.toLowerCase().endsWith('.csv'))
      .map((fileName) => path.join(extractedDir, fileName));

    if (DATEV_PROCESS_FILES) {
      const kost1Rules = await loadKost1Rules();
      const processed = await processExtractedDatevFiles(extractedDir, kost1Rules);
      result.processedDir = processed.processedDir;
      result.processedFiles = processed.processedFiles;
      result.transformedFiles = processed.transformed;
    }
  }

  return result;
}

async function main() {
  if (!process.env.IMMO_EMAIL || !process.env.IMMO_PASSWORD) {
    throw new Error('Missing IMMO_EMAIL or IMMO_PASSWORD in environment.');
  }

  const exportRange = process.env.DATEV_EXPORT_RANGE;
  const [rawStart, rawEnd] = exportRange ? exportRange.split(':') : [];
  const defaultRange = previousMonthRange();
  const startDate = parseDate(process.env.DATEV_EXPORT_START || rawStart || defaultRange.start);
  const endDate = parseDate(process.env.DATEV_EXPORT_END || rawEnd || defaultRange.end);
  const fiscalYearStart = parseDate(process.env.DATEV_FISCAL_YEAR_START || new Date(startDate.getFullYear(), 0, 1));

  await ensureDir(DOWNLOAD_DIR);
  if (DATEV_PROCESS_FILES && !KOST1_MAP_FILE) {
    console.warn('DATEV_KOST1_MAP_FILE is not set. KOST1 will be left unchanged unless already provided by the export.');
  }
  if (GOOGLE_DRIVE_UPLOAD && !GOOGLE_DRIVE_FOLDER_ID) {
    throw new Error('GOOGLE_DRIVE_UPLOAD=true but GOOGLE_DRIVE_FOLDER_ID is missing.');
  }
  if (GOOGLE_SHEETS_IMPORT && !GOOGLE_DRIVE_FOLDER_ID) {
    throw new Error('GOOGLE_SHEETS_IMPORT=true but GOOGLE_DRIVE_FOLDER_ID is missing.');
  }
  if (WEBHOOK_UPLOAD && !WEBHOOK_URL) {
    throw new Error('WEBHOOK_UPLOAD=true but WEBHOOK_URL is missing.');
  }
  if (GOOGLE_SHEETS_WEBHOOK_ENABLED && !GOOGLE_SHEETS_WEBHOOK_URL) {
    throw new Error('GOOGLE_SHEETS_WEBHOOK_ENABLED=true but GOOGLE_SHEETS_WEBHOOK_URL is missing.');
  }

  const browser = await chromium.launch({ headless: HEADLESS });
  const storageState = await loadStorageState();
  const context = await browser.newContext({
    acceptDownloads: true,
    locale: 'de-DE',
    timezoneId: 'Europe/Berlin',
    storageState,
  });
  const page = await context.newPage();

  try {
    await ensureAuthenticated(page);
    await saveStorageState(context);
    await fillDateRange(page, startDate, endDate);
    await selectOwners(page);
    await fillFiscalYear(page, fiscalYearStart);
    await createExport(page);
    const downloadResult = await downloadFromHighlightedRow(page, DOWNLOAD_DIR);
    const filesToUpload = downloadResult.processedFiles.length > 0
      ? downloadResult.processedFiles
      : [downloadResult.archivePath];
    const driveUploads = await uploadFilesToGoogleDrive(filesToUpload);
    const importedSheets = await importCsvFilesToGoogleSheets(downloadResult.originalCsvFiles);
    const googleSheetsWebhook = await sendImportedSheetsToWebhook(importedSheets, {
      startDate: formatIsoDate(startDate),
      endDate: formatIsoDate(endDate),
      fiscalYearStart: formatIsoDate(fiscalYearStart),
      archiveName: path.basename(downloadResult.archivePath),
    });
    const webhookUpload = await uploadFilesToWebhook(downloadResult.originalCsvFiles, {
      startDate: formatIsoDate(startDate),
      endDate: formatIsoDate(endDate),
      fiscalYearStart: formatIsoDate(fiscalYearStart),
      archiveName: path.basename(downloadResult.archivePath),
    });

    console.log(JSON.stringify({
      status: 'ok',
      startDate: formatIsoDate(startDate),
      endDate: formatIsoDate(endDate),
      fiscalYearStart: formatIsoDate(fiscalYearStart),
      ...downloadResult,
      driveUploads,
      importedSheets,
      googleSheetsWebhook,
      webhookUpload,
    }, null, 2));
  } finally {
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

main().catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exitCode = 1;
});
