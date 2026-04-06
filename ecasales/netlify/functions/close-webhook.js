const SUPA_URL = 'https://sifinrypprjfuyvtxowt.supabase.co';
const SUPA_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNpZmlucnlwcHJqZnV5dnR4b3d0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU1MDM3MDcsImV4cCI6MjA5MTA3OTcwN30.XG03GAdtpuuQiO3pSjbl8WoPGOk_FZBCw9DPwBVNi34';

const HEADERS = {
  'Content-Type': 'application/json',
  'apikey': SUPA_KEY,
  'Authorization': 'Bearer ' + SUPA_KEY
};

const ACTIVITY_TYPE_MAP = {
  'actitype_0gmrSF3cyjFoUzLw1mRyae': 'completed',
  'actitype_4uVyF0QGlLaOTOzn2F8P2M': 'not_completed',
  'actitype_2z8SRuCyy309OLnSUz7oSi': 'deal_won'
};

const FIELDS = {
  completed: {
    date: 'custom.cf_mxQkF7rW5qpUmYfArG3LmKuUQNm3ZHnNZcSZvIhYe89',
    rep:  'custom.cf_oj8HhXOPL8f9aiAoY3oSLFw6rSCtFcuWWc0jc8oyXkU'
  },
  not_completed: {
    date: 'custom.cf_XVxzZf70sQ9YY8mvHgV1suIBpKKN5hjYGxjF98V5f4f',
    rep:  'custom.cf_4D8Qdkwg3igcj1CLiI6aZMFmKgfGkR31NeihZDVFTX4'
  },
  deal_won: {
    date: 'custom.cf_9pEoBLwX23aeeJBCs7XMWLH6K1UhSykPOp0nZwthHMm',
    rep:  'custom.cf_XrWciUXWFWqVBgCv637WhuQaN4nNGyBj8bHpSf9Uv6J',
    rev:  'custom.cf_USVRWjGuIuwao4VkV5R63Ov5Mr7apoANNBHhTEz6Enw',
    cash: 'custom.cf_A0RETOl11g1ZhVsC2w57chW0jAyFxoBUd6pUcCJabnf'
  }
};

function parseDate(raw) {
  if (!raw) return null;
  return String(raw).substring(0, 10);
}

async function upsertCloser(name, date, increment) {
  const getRes = await fetch(
    `${SUPA_URL}/rest/v1/closer?name=eq.${encodeURIComponent(name)}&date=eq.${date}&select=*`,
    { headers: HEADERS }
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
    if (!patchRes.ok) throw new Error('Patch failed: ' + await patchRes.text());
  } else {
    const newRow = {
      name, date,
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
    if (!insertRes.ok) throw new Error('Insert failed: ' + await insertRes.text());
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

  const ev = payload.event;
  if (!ev) {
    return { statusCode: 200, body: JSON.stringify({ ignored: true, reason: 'no event key' }) };
  }

  const action = ev.action;
  const data = ev.data;
  const previousData = ev.previous_data || {};

  console.log('action:', action, 'status:', data && data.status, 'prev status:', previousData.status);

  if (!data) {
    return { statusCode: 200, body: JSON.stringify({ ignored: true, reason: 'no data' }) };
  }

  // Process if:
  // - created and not draft (activity saved directly without draft step)
  // - updated and status changed from draft to published (rep hit publish)
  const isDirectCreate = action === 'created' && data.status !== 'draft';
  const isPublishFromDraft = action === 'updated' && previousData.status === 'draft' && data.status !== 'draft';

  console.log('isDirectCreate:', isDirectCreate, 'isPublishFromDraft:', isPublishFromDraft);

  if (!isDirectCreate && !isPublishFromDraft) {
    return { statusCode: 200, body: JSON.stringify({ ignored: true, reason: 'not a publish event', action, status: data.status }) };
  }

  const typeId = data.custom_activity_type_id;
  const handlerKey = ACTIVITY_TYPE_MAP[typeId];

  console.log('type_id:', typeId, 'handlerKey:', handlerKey);

  if (!handlerKey) {
    return { statusCode: 200, body: JSON.stringify({ ignored: true, reason: 'unknown type', typeId }) };
  }

  const fieldMap = FIELDS[handlerKey];
  const repName = data[fieldMap.rep] || null;
  const rawDate = data[fieldMap.date] || data.date_created;
  const date = parseDate(rawDate);

  console.log('rep:', repName, 'date:', date);

  if (!repName) {
    return { statusCode: 422, body: 'Could not identify rep name' };
  }
  if (!date) {
    return { statusCode: 422, body: 'Could not parse date' };
  }

  try {
    if (handlerKey === 'completed') {
      await upsertCloser(repName, date, { sched: 1, live: 1 });
    } else if (handlerKey === 'not_completed') {
      await upsertCloser(repName, date, { sched: 1 });
    } else if (handlerKey === 'deal_won') {
      const rev  = parseFloat(data[fieldMap.rev]  || 0);
      const cash = parseFloat(data[fieldMap.cash] || 0);
      await upsertCloser(repName, date, { closed: 1, rev, cash });
    }

    console.log('SUCCESS:', handlerKey, repName, date);
    return {
      statusCode: 200,
      body: JSON.stringify({ success: true, handler: handlerKey, rep: repName, date })
    };

  } catch (e) {
    console.error('ERROR:', e.message);
    return { statusCode: 500, body: 'Internal error: ' + e.message };
  }
};
