async function init() {
  const { serverUrl, apiKey } = await chrome.storage.sync.get(['serverUrl', 'apiKey']);

  if (!serverUrl) {
    show('setupView');
    return;
  }

  document.getElementById('serverLink').href = serverUrl;

  // Instagram sessionid 읽기 (chrome.cookies는 HttpOnly도 읽을 수 있음)
  const cookies = await chrome.cookies.getAll({ domain: '.instagram.com' });
  const sessionCookie = cookies.find(c => c.name === 'sessionid');
  const dsCookie = cookies.find(c => c.name === 'ds_user_id');

  if (!sessionCookie) {
    show('notInstaView');
    return;
  }

  show('mainView');

  // 로그인된 유저 표시
  const igUsername = dsCookie ? `@user_${dsCookie.value}` : '로그인됨';
  document.getElementById('igUser').textContent = sessionCookie ? igUsername : '로그인 필요';
  if (sessionCookie) {
    document.getElementById('statusDot').classList.remove('off');
  }

  // 계정 목록 로드
  try {
    const res = await fetch(`${serverUrl}/api/accounts`, {
      headers: { 'X-Api-Key': apiKey || '' }
    });
    if (!res.ok) throw new Error('인증 실패');
    const accounts = await res.json();

    const sel = document.getElementById('accSelect');
    sel.innerHTML = accounts.length
      ? accounts.map(a => `<option value="${a.id}">${a.username}</option>`).join('')
      : '<option value="">등록된 계정 없음</option>';

    if (accounts.length > 0) {
      document.getElementById('syncBtn').disabled = false;
    }
  } catch (e) {
    document.getElementById('accSelect').innerHTML = `<option value="">❌ 서버 연결 실패: ${e.message}</option>`;
  }
}

async function doSync() {
  const { serverUrl, apiKey } = await chrome.storage.sync.get(['serverUrl', 'apiKey']);
  const accId = document.getElementById('accSelect').value;
  if (!accId) return;

  const cookies = await chrome.cookies.getAll({ domain: '.instagram.com' });
  const sessionCookie = cookies.find(c => c.name === 'sessionid');
  if (!sessionCookie) {
    showResult('❌ sessionid 쿠키를 찾을 수 없습니다', false);
    return;
  }

  const btn = document.getElementById('syncBtn');
  btn.disabled = true;
  btn.textContent = '등록 중...';

  try {
    const fd = new FormData();
    fd.append('sessionid_cookie', sessionCookie.value);
    const res = await fetch(`${serverUrl}/accounts/${accId}/sessionid`, {
      method: 'POST',
      headers: { 'X-Api-Key': apiKey || '' },
      body: fd
    });
    const d = await res.json();
    if (d.ok) {
      showResult('✅ 세션 등록 완료! 이제 수집이 가능합니다.', true);
    } else {
      showResult('❌ ' + (d.error || '등록 실패'), false);
    }
  } catch (e) {
    showResult('❌ 서버 연결 오류: ' + e.message, false);
  }

  btn.disabled = false;
  btn.textContent = '⚡ 세션 자동 등록';
}

function show(viewId) {
  ['setupView', 'notInstaView', 'mainView'].forEach(id => {
    document.getElementById(id).style.display = id === viewId ? '' : 'none';
  });
}

function showResult(msg, ok) {
  const el = document.getElementById('result');
  el.textContent = msg;
  el.className = 'result ' + (ok ? 'ok' : 'err');
  el.style.display = 'block';
}

init();
