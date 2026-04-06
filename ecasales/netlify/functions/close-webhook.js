const SUPA_URL = 'https://sifinrypprjfuyvtxowt.supabase.co';
const SUPA_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNpZmlucnlwcHJqZnV5dnR4b3d0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU1MDM3MDcsImV4cCI6MjA5MTA3OTcwN30.XG03GAdtpuuQiO3pSjbl8WoPGOk_FZBCw9DPwBVNi34';

const HEADERS = {
  'Content-Type': 'application/json',
  'apikey': SUPA_KEY,
  'Authorization': 'Bearer ' + SUPA_KEY,
  'Prefer': 'resolution=merge-duplicates'
};

const ACTIVITY_MAP = {
  'Strategy Call Completed': 'completed',
  'Strategy Call Not Completed': 'not_completed',
  'Deal Won': 'deal_won'
};

function getField(fields, label) {
  if (!fields || !Array.isArray(fields)) return null;
  const field = fields.find(f =>
    f.label && f.label.toLowerCase() === label.toLowerCase()
  );
  return field ? field.value : null;
}

function parseDate(raw) {
  if (!raw) return null;
  return raw.substring(0, 10);
}

async function upsertCloser(name, date, increment) {
  const getRes = await fetch(
    `${SUPA_URL}/rest/v1/closer?name=eq.${encodeURIComponent(name)}&date=eq.${date}&select=*`,
    { headers: { 'apikey': SUPA_KEY, 'Authorization': 'Bearer ' + SUPA_KEY } }
  );
  const existing = await getRes.json();

  if (existing && existing.length > 0) {
    const row = existing[0];
    const updated = {
      sched:  row.sched  + (increment.sched  || 0),
      live:   row.live   + (increment.live   || 0),
      offers: row.offers + (increment.offers || 0),
      closed: row.closed + (increment.closed || 0),
      rev:    row.rev    + (increment.rev    || 0),
      cash:   row.cash   + (increment.cash   || 0)
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
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method not allowed' };
  }

  let payload;
  try {
    payload = JSON.parse(event.body);
  } catch (e) {
    return { statusCode: 400, body: 'Invalid JSON' };
  }

  // DEBUG: log the full payload so we can see exactly what Close sends
  console.log('FULL PAYLOAD:', JSON.stringify(payload, null, 2));

  const data = payload.data || payload;

  console.log('DATA KEYS:', Object.keys(data).join(', '));
  console.log('activity_type:', data.activity_type);
  console.log('activity_type_name:', data.activity_type_name);
  console.log('type:', data.type);
  console.log('_type:', data._type);
  console.log('fields:', JSON.stringify(data.fields || data.custom || []));

  const activityType = data.activity_type || (data._type === 'Activity' ? data.type : null);

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

  console.log('RESOLVED handlerKey:', handlerKey);

  if (!handlerKey) {
    return { statusCode: 200, body: JSON.stringify({ ignored: true, type: activityType }) };
  }

  const fields = data.fields || data.custom || [];
  let repName = getField(fields, 'Closer Lead Owner');

  if (!repName) {
    repName = data.created_by_name || data.user_name || null;
  }

  console.log('REP NAME:', repName);

  if (!repName) {
    return { statusCode: 422, body: 'Could not identify rep name' };
  }

  let date =
    getField(fields, 'Date Of Call') ||
    getField(fields, 'Date Won') ||
    data.date_created ||
    new Date().toISOString();
  date = parseDate(date);

  console.log('DATE:', date);

  if (!date) {
    return { statusCode: 422, body: 'Could not parse date' };
  }

  try {
    if (handlerKey === 'completed') {
      await upsertCloser(repName, date, { sched: 1, live: 1 });
    } else if (handlerKey === 'not_completed') {
      await upsertCloser(repName, date, { sched: 1 });
    } else if (handlerKey === 'deal_won') {
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
    console.error('ERROR:', e.message);
    return { statusCode: 500, body: 'Internal error: ' + e.message };
  }
};
