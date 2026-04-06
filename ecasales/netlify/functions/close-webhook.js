const SUPA_URL = 'https://sifinrypprjfuyvtxowt.supabase.co';
const SUPA_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNpZmlucnlwcHJqZnV5dnR4b3d0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU1MDM3MDcsImV4cCI6MjA5MTA3OTcwN30.XG03GAdtpuuQiO3pSjbl8WoPGOk_FZBCw9DPwBVNi34';

const HEADERS = {
  'Content-Type': 'application/json',
  'apikey': SUPA_KEY,
  'Authorization': 'Bearer ' + SUPA_KEY,
  'Prefer': 'resolution=merge-duplicates'
};

// Maps Close activity type names to handler keys
const ACTIVITY_MAP = {
  'Strategy Call Completed': 'completed',
  'Strategy Call Not Completed': 'not_completed',
  'Deal Won': 'deal_won'
};

// Extracts field value from Close custom activity fields array by label
function getField(fields, label) {
  if (!fields || !Array.isArray(fields)) return null;
  const field = fields.find(f =>
    f.label && f.label.toLowerCase() === label.toLowerCase()
  );
  return field ? field.value : null;
}

// Parses Close date format (YYYY-MM-DD or ISO string) to YYYY-MM-DD
function parseDate(raw) {
  if (!raw) return null;
  return raw.substring(0, 10);
}

async function upsertCloser(name, date, increment) {
  // First try to get existing row
  const getRes = await fetch(
    `${SUPA_URL}/rest/v1/closer?name=eq.${encodeURIComponent(name)}&date=eq.${date}&select=*`,
    { headers: { 'apikey': SUPA_KEY, 'Authorization': 'Bearer ' + SUPA_KEY } }
  );
  const existing = await getRes.json();

  if (existing && existing.length > 0) {
    // Row exists — increment the relevant fields
    const row = existing[0];
    const updated = {
      sched: row.sched + (increment.sched || 0),
      live:  row.live  + (increment.live  || 0),
      offers: row.offers + (increment.offers || 0),
      closed: row.closed + (increment.closed || 0),
      rev:   row.rev   + (increment.rev   || 0),
      cash:  row.cash  + (increment.cash  || 0)
    };
    const patchRes = await fetch(
      `${SUPA_URL}/rest/v1/closer?id=eq.${row.id}`,
      {
        method: 'PATCH',
        headers: { ...HEADERS, 'Prefer': 'return=minimal' },
        body: JSON.stringify(updated)
      }
    );
    if (!patchRes.ok) {
      const err = await patchRes.text();
      throw new Error('Patch failed: ' + err);
    }
  } else {
    // No row yet — insert fresh
    const newRow = {
      name,
      date,
      sched:  increment.sched  || 0,
      live:   increment.live   || 0,
      offers: increment.offers || 0,
      closed: increment.closed || 0,
      rev:    increment.rev    || 0,
      cash:   increment.cash   || 0
    };
    const insertRes = await fetch(
      `${SUPA_URL}/rest/v1/closer`,
      {
        method: 'POST',
        headers: { ...HEADERS, 'Prefer': 'return=minimal' },
        body: JSON.stringify(newRow)
      }
    );
    if (!insertRes.ok) {
      const err = await insertRes.text();
      throw new Error('Insert failed: ' + err);
    }
  }
}

exports.handler = async function(event) {
  // Only accept POST requests
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method not allowed' };
  }

  let payload;
  try {
    payload = JSON.parse(event.body);
  } catch (e) {
    return { statusCode: 400, body: 'Invalid JSON' };
  }

  // Close sends webhooks with event.data containing the activity
  const data = payload.data || payload;
  const activityType = data.activity_type || (data._type === 'Activity' ? data.type : null);

  // Resolve the activity name — Close may send type name directly or as a string
  // We match against known activity type names in ACTIVITY_MAP
  let handlerKey = null;
  for (const [typeName, key] of Object.entries(ACTIVITY_MAP)) {
    if (
      activityType === typeName ||
      (data.type && data.type === typeName) ||
      (data.activity_type_name && data.activity_type_name === typeName)
    ) {
      handlerKey = key;
      break;
    }
  }

  if (!handlerKey) {
    // Not an activity type we handle — acknowledge and ignore
    return { statusCode: 200, body: JSON.stringify({ ignored: true, type: activityType }) };
  }

  // Extract rep name from Closer Lead Owner field
  const fields = data.fields || data.custom || [];
  let repName = getField(fields, 'Closer Lead Owner');

  // Fallback: try created_by_name if field not found
  if (!repName) {
    repName = data.created_by_name || data.user_name || null;
  }

  if (!repName) {
    return { statusCode: 422, body: 'Could not identify rep name' };
  }

  // Extract date from Date Of Call or Date Won field, fallback to today
  let date =
    getField(fields, 'Date Of Call') ||
    getField(fields, 'Date Won') ||
    data.date_created ||
    new Date().toISOString();
  date = parseDate(date);

  if (!date) {
    return { statusCode: 422, body: 'Could not parse date' };
  }

  try {
    if (handlerKey === 'completed') {
      // Strategy Call Completed: counts as scheduled + live
      await upsertCloser(repName, date, { sched: 1, live: 1 });

    } else if (handlerKey === 'not_completed') {
      // Strategy Call Not Completed: counts as scheduled only
      await upsertCloser(repName, date, { sched: 1 });

    } else if (handlerKey === 'deal_won') {
      // Deal Won: closed deal + revenue + cash
      // Contract Value and Cash Collected are stored as numbers (dollars, not cents)
      const contractValue = parseFloat(getField(fields, 'Contract Value') || 0);
      const cashCollected = parseFloat(getField(fields, 'Cash Collected') || 0);
      await upsertCloser(repName, date, {
        closed: 1,
        rev: contractValue,
        cash: cashCollected
      });
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ success: true, handler: handlerKey, rep: repName, date })
    };

  } catch (e) {
    console.error('Webhook handler error:', e.message);
    return { statusCode: 500, body: 'Internal error: ' + e.message };
  }
};
