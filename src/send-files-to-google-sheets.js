#!/usr/bin/env node

const fs = require('fs/promises');
const http = require('http');
const path = require('path');
const readline = require('readline');
const { Readable } = require('stream');
const dotenv = require('dotenv');
const { google } = require('googleapis');

dotenv.config();

const GOOGLE_DRIVE_FOLDER_ID = (process.env.GOOGLE_DRIVE_FOLDER_ID || '').trim();
const GOOGLE_DRIVE_CREDENTIALS_FILE = (process.env.GOOGLE_DRIVE_CREDENTIALS_FILE || process.env.GOOGLE_APPLICATION_CREDENTIALS || '').trim();
const GOOGLE_DRIVE_CREDENTIALS_JSON = (process.env.GOOGLE_DRIVE_CREDENTIALS_JSON || '').trim();
const GOOGLE_SHEETS_SOURCE_DIR = (process.env.GOOGLE_SHEETS_SOURCE_DIR || '').trim();
const GOOGLE_OAUTH_CLIENT_ID = (process.env.GOOGLE_OAUTH_CLIENT_ID || '').trim();
const GOOGLE_OAUTH_CLIENT_SECRET = (process.env.GOOGLE_OAUTH_CLIENT_SECRET || '').trim();
const GOOGLE_OAUTH_REDIRECT_URI = (process.env.GOOGLE_OAUTH_REDIRECT_URI || 'http://127.0.0.1:3000/oauth2callback').trim();
const GOOGLE_OAUTH_TOKEN_FILE = path.resolve(process.env.GOOGLE_OAUTH_TOKEN_FILE || '.auth/google-oauth-token.json');
const GOOGLE_OAUTH_TIMEOUT_MS = Number(process.env.GOOGLE_OAUTH_TIMEOUT_MS || 300_000);
const GOOGLE_AUTH_SCOPES = [
  'https://www.googleapis.com/auth/drive.file',
  'https://www.googleapis.com/auth/spreadsheets',
];
const GOOGLE_SHEETS_DEFAULT_TAB_TITLE = 'List';
const GOOGLE_SHEETS_WEBHOOK_URL = (
  process.env.GOOGLE_SHEETS_WEBHOOK_URL
  || 'https://n8n.srv1309825.hstgr.cloud/webhook/4483f020-c90a-44df-b336-ff286848aee9'
).trim();
const GOOGLE_SHEETS_WEBHOOK_ENABLED = parseBoolean(process.env.GOOGLE_SHEETS_WEBHOOK_ENABLED, true);
const GOOGLE_SHEETS_WEBHOOK_TOKEN = (process.env.GOOGLE_SHEETS_WEBHOOK_TOKEN || '').trim();

function parseBoolean(value, fallback) {
  if (value == null || value === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
}

function printUsage() {
  console.log(
    [
      'Usage:',
      '  node src/send-files-to-google-sheets.js <file-or-dir> [more files or dirs]',
      '',
      'Environment:',
      '  GOOGLE_DRIVE_FOLDER_ID        Required Drive folder ID',
      '  GOOGLE_OAUTH_CLIENT_ID        Preferred OAuth client id',
      '  GOOGLE_OAUTH_CLIENT_SECRET    Preferred OAuth client secret',
      '  GOOGLE_OAUTH_REDIRECT_URI     OAuth redirect URI',
      '  GOOGLE_OAUTH_TOKEN_FILE       Cached OAuth token path',
      '  GOOGLE_SHEETS_SOURCE_DIR      Optional fallback path when no CLI args are passed',
      '  GOOGLE_SHEETS_WEBHOOK_URL     Webhook target for imported sheet metadata',
      '  GOOGLE_SHEETS_WEBHOOK_ENABLED Send imported sheet metadata to webhook, default: true',
      '  GOOGLE_SHEETS_WEBHOOK_TOKEN   Optional Bearer token for webhook',
      '',
      'Fallback service-account auth is still supported via:',
      '  GOOGLE_DRIVE_CREDENTIALS_FILE',
      '  GOOGLE_DRIVE_CREDENTIALS_JSON',
    ].join('\n'),
  );
}

function stripFirstLine(buffer) {
  const text = buffer.toString('utf8');
  const trimmed = text.replace(/^\uFEFF/, '').replace(/^.*(?:\r?\n|$)/, '');
  return Buffer.from(trimmed, 'utf8');
}

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
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
    : (GOOGLE_SHEETS_SOURCE_DIR ? [GOOGLE_SHEETS_SOURCE_DIR] : []);

  if (inputs.length === 0) {
    throw new Error('Pass at least one file or directory, or set GOOGLE_SHEETS_SOURCE_DIR.');
  }

  const collected = [];
  for (const input of inputs) {
    collected.push(...await collectFiles(input));
  }

  const uniqueFiles = [...new Set(collected)]
    .filter((filePath) => path.extname(filePath).toLowerCase() === '.csv')
    .sort((left, right) => left.localeCompare(right));

  if (uniqueFiles.length === 0) {
    throw new Error('No CSV files found to import into Google Sheets.');
  }

  return uniqueFiles;
}

function createAuth() {
  if (!GOOGLE_DRIVE_FOLDER_ID) {
    throw new Error('GOOGLE_DRIVE_FOLDER_ID is required.');
  }

  if (GOOGLE_OAUTH_CLIENT_ID && GOOGLE_OAUTH_CLIENT_SECRET) {
    return authorizeWithOAuth();
  }

  return createServiceAccountAuth();
}

function createServiceAccountAuth() {
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

  const cachedTokens = await readCachedOAuthTokens();
  if (cachedTokens) {
    oauth2Client.setCredentials(cachedTokens);
    return oauth2Client;
  }

  const code = await getOAuthCode(oauth2Client);
  const { tokens } = await oauth2Client.getToken(code);
  oauth2Client.setCredentials(tokens);
  await writeOAuthTokens(tokens);
  return oauth2Client;
}

async function ensureSpreadsheetDefaultSheetTitle(sheetsClient, spreadsheetId) {
  const desiredTitle = GOOGLE_SHEETS_DEFAULT_TAB_TITLE;

  try {
    const response = await sheetsClient.spreadsheets.get({
      spreadsheetId,
      fields: 'sheets(properties(sheetId,title))',
    });

    const sheets = response.data.sheets || [];
    if (sheets.some((sheet) => sheet?.properties?.title === desiredTitle)) {
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
                title: desiredTitle,
              },
              fields: 'title',
            },
          },
        ],
      },
    });
  } catch (error) {
    const message = String(error?.message || error);
    throw new Error(`Failed to set sheet title to "${desiredTitle}" for spreadsheet ${spreadsheetId}: ${message}`);
  }
}

async function importCsvFilesToGoogleSheets(filePaths) {
  const auth = await createAuth();
  const drive = google.drive({ version: 'v3', auth });
  const sheets = google.sheets({ version: 'v4', auth });
  const imported = [];

  for (const filePath of filePaths) {
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

async function sendImportedSheetsToWebhook(imported) {
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

async function main() {
  const args = process.argv.slice(2);

  if (args.includes('--help') || args.includes('-h')) {
    printUsage();
    return;
  }

  const files = await resolveInputFiles(args);
  const imported = await importCsvFilesToGoogleSheets(files);
  const webhookResult = await sendImportedSheetsToWebhook(imported);

  for (const item of imported) {
    console.log(`${item.name}: ${item.webViewLink || item.id}`);
  }

  if (webhookResult) {
    console.log(`Webhook delivered: ${webhookResult.status} ${webhookResult.url}`);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
