(function () {
  'use strict';

  var $ = function (id) { return document.getElementById(id); };

  var state = {
    sessionId: null,
    sending: false,             // 只表示「当前视图这个会话在跑」，真正来源是 state.running
    running: {},                // sessionId -> {since}：每个会话各自的执行状态
    ws: null,
    messageMap: new Map(),
    listTimer: null,
    toastTimer: null,
    startedAt: 0,
    statusTimer: null,
    tab: 'active',              // active | archived
    sessions: [],
    archived: [],
    workspaces: [],
    options: null,              // /api/options 结果
    modes: [],
    providers: [],
    provScope: 'official',      // official | custom（默认停在「官方配置」，与 HTML 里 active 的按钮一致）
    editingProvider: null,
    editingModels: [],          // 打开弹窗时该厂商原有的模型 ID（算「删了哪些」用）
    cur: null,                  // 当前正在流式渲染的助手消息
    msgPage: { hasMore: false, oldestSeq: null, loading: false,
               total: 0, rendered: 0 },   // 历史消息分页：最近50条 + 上滑加载更早
    confirmResolve: null,
    // --- WebSocket 保活与自动重连 ---
    wsEnabled: false,           // 已进入控制台才允许连接/重连（未登录时不重连，避免 401 空转）
    wsAttempt: 0,               // 连续重连次数，用于退避
    wsTimer: null,              // 重连定时器
    wsPingTimer: null,          // 应用层心跳
    wsIdleTimer: null,          // 无入站帧看门狗
    wsLastInbound: 0,           // 最后一次收到任何帧的时间
    wsManualClose: false,       // 主动关闭（登出）时不重连
    wsResumeTimer: null,        // 重连后等待 resumed 的兜底定时器
    pendingSend: null,          // 断线期间暂存的消息，重连后自动补发
    // --- 工作区 ---
    wsRoot: '/root',            // 工作区根目录（来自 /api/workspaces）
    wsCollapsed: {},            // 折叠状态：{工作区路径: true}
    wsEditing: null,            // 正在编辑的工作区
    wsOnDone: null,             // 工作区弹窗完成后的回调
    // --- 使用统计 ---
    usage: null,
    usageRange: 'today',
    // --- 模型定时 ---
    schedules: [],
    scheduleRuntime: { active: null },
    schedEditing: null,         // 正在编辑的规则（null = 新建）
    // --- 实时尾随（外部进程跑的会话只能靠读库刷新）---
    tail: { sid: null, timer: null, cursor: 0, busy: false, external: false, quiet: 0 },
    localUserMsgs: [],          // 本地已渲染、还没绑上库里 id 的用户消息（防重复）
  };

  var WS_COLLAPSE_KEY = 'zwc_ws_collapsed';
  var LAST_SESSION_KEY = 'zwc_last_session';

  function rememberSession(sid) {
    try {
      if (sid) localStorage.setItem(LAST_SESSION_KEY, sid);
      else localStorage.removeItem(LAST_SESSION_KEY);
    } catch (e) {}
  }

  // 重开页面时回到上次的会话：如果它在后台跑着，正好直接接回输出
  function restoreLastSession() {
    if (state.sessionId) return;
    var sid = null;
    try { sid = localStorage.getItem(LAST_SESSION_KEY); } catch (e) {}
    if (!sid) return;
    var hit = (state.sessions || []).filter(function (s) { return s.id === sid; })[0];
    if (!hit) return;
    openSession(sid);
  }

  try {
    state.wsCollapsed = JSON.parse(localStorage.getItem(WS_COLLAPSE_KEY) || '{}') || {};
  } catch (e) {
    state.wsCollapsed = {};
  }

  function saveWsCollapsed() {
    try { localStorage.setItem(WS_COLLAPSE_KEY, JSON.stringify(state.wsCollapsed)); } catch (e) {}
  }

  // 服务端 handle_ws 已支持 {"type":"ping"} -> {"type":"pong"}，用它做应用层保活。
  // 移动网络下 NAT 会静默回收空闲映射，TCP 层收不到 FIN，所以必须有主动心跳。
  var WS_PING_INTERVAL = 20000;   // 20s 一次
  var WS_IDLE_LIMIT = 50000;      // 50s 无任何入站帧即判定链路已死
  var WS_IDLE_CHECK = 10000;

  // 外部进程（禅道自动执行器等）自己拉起 app-server，它的事件进不了面板的事件流，
  // 只能定时读库做增量尾随。库里按「步骤」落盘，1.2s 一轮足够接近实时；长时间
  // 没动静就退到 4s，省得空转。
  var TAIL_INTERVAL = 1200;
  var TAIL_QUIET_INTERVAL = 4000;

  /* ============================================ 基础工具 */

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function mdParse(text) {
    var M = window.marked;
    if (!M) return esc(text).replace(/\n/g, '<br>');
    var fn = typeof M === 'function' ? M : M.parse;
    if (typeof fn !== 'function') return esc(text).replace(/\n/g, '<br>');
    var html;
    try {
      html = fn.call(M, String(text || ''));
    } catch (e) {
      return esc(text).replace(/\n/g, '<br>');
    }
    return html
      .replace(/<script[\s\S]*?<\/script>/gi, '')
      .replace(/<iframe[\s\S]*?<\/iframe>/gi, '')
      .replace(/\son\w+\s*=\s*"[^"]*"/gi, '')
      .replace(/\son\w+\s*=\s*'[^']*'/gi, '')
      .replace(/\son\w+\s*=\s*[^\s>]+/gi, '')
      .replace(/javascript:/gi, '');
  }

  function toast(msg, kind) {
    var el = $('toast');
    el.textContent = msg;
    el.className = 'toast' + (kind ? ' ' + kind : '');
    clearTimeout(state.toastTimer);
    state.toastTimer = setTimeout(function () { el.classList.add('hidden'); }, 3600);
  }

  function req(path, opts) {
    opts = opts || {};
    opts.credentials = 'same-origin';
    opts.headers = Object.assign({ 'Content-Type': 'application/json' }, opts.headers || {});
    return fetch(path, opts).then(function (res) {
      return res.json().catch(function () { return {}; }).then(function (data) {
        return { status: res.status, data: data || {} };
      });
    });
  }

  function pad2(n) { return String(n).length < 2 ? '0' + n : String(n); }

  function relTime(ms) {
    if (!ms) return '';
    var diff = Date.now() - ms;
    if (diff < 60000) return '刚刚';
    if (diff < 3600000) return Math.floor(diff / 60000) + ' 分钟前';
    if (diff < 86400000) return Math.floor(diff / 3600000) + ' 小时前';
    if (diff < 604800000) return Math.floor(diff / 86400000) + ' 天前';
    var d = new Date(ms);
    return (d.getMonth() + 1) + '/' + d.getDate();
  }

  // 消息末尾的本地时间，历史接口和实时流都使用毫秒时间戳
  function fmtMsgTime(ms) {
    if (!ms) return '';
    var d = new Date(ms);
    return d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate()) +
      ' ' + pad2(d.getHours()) + ':' + pad2(d.getMinutes());
  }

  /* ============================================ 数字格式化 */

  // token 用量按 k / m / b 缩写（1000 进制）
  function fmtTokens(n) {
    n = Number(n) || 0;
    if (n < 1000) return String(n);
    var units = [['b', 1e9], ['m', 1e6], ['k', 1e3]];
    for (var i = 0; i < units.length; i++) {
      var base = units[i][1];
      if (n >= base) {
        var v = n / base;
        var s = v >= 100 ? v.toFixed(0) : (v >= 10 ? v.toFixed(1) : v.toFixed(2));
        if (s.indexOf('.') >= 0) s = s.replace(/0+$/, '').replace(/\.$/, '');
        return s + units[i][0];
      }
    }
    return String(n);
  }

  // 精确值加千分位，用于 tooltip 与统计表
  function fmtNum(n) {
    n = Math.round(Number(n) || 0);
    return String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  }

  function fmtDuration(ms) {
    ms = Number(ms) || 0;
    if (ms < 1000) return ms + 'ms';
    var s = ms / 1000;
    if (s < 60) return s.toFixed(1) + 's';
    var m = Math.floor(s / 60);
    return m + 'm' + Math.round(s - m * 60) + 's';
  }

  function scrollToBottom(force) {
    var box = $('messages');
    if (!box) return;
    var near = box.scrollHeight - box.scrollTop - box.clientHeight < 220;
    if (force || near) box.scrollTop = box.scrollHeight;
  }

  /* ============================================ 弹窗 */

  function openModal(id) { $(id).classList.remove('hidden'); }
  function closeModal(id) {
    $(id).classList.add('hidden');
    if (id === 'settings-modal') { /* 保留内容，下次打开会重新拉 */ }
  }

  function confirmBox(title, text, okLabel) {
    $('cf-title').textContent = title;
    $('cf-text').textContent = text;
    $('cf-ok').textContent = okLabel || '确定';
    openModal('confirm-modal');
    return new Promise(function (resolve) {
      state.confirmResolve = resolve;
    });
  }

  $('cf-ok').addEventListener('click', function () {
    closeModal('confirm-modal');
    if (state.confirmResolve) { state.confirmResolve(true); state.confirmResolve = null; }
  });

  document.addEventListener('click', function (e) {
    var t = e.target;
    if (!t || !t.getAttribute) return;
    var c = t.getAttribute('data-close');
    if (c) {
      closeModal(c);
      if (c === 'confirm-modal' && state.confirmResolve) {
        state.confirmResolve(false);
        state.confirmResolve = null;
      }
    }
    // 点空白处关掉会话菜单
    if (!t.closest || !t.closest('.session-menu') && !t.closest('.session-more')) {
      var menus = document.querySelectorAll('.session-menu');
      for (var i = 0; i < menus.length; i++) {
        if (menus[i].parentNode) menus[i].parentNode.removeChild(menus[i]);
      }
    }
  });

  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Escape') return;
    var mods = document.querySelectorAll('.modal:not(.hidden)');
    if (mods.length) {
      var top = mods[mods.length - 1];
      closeModal(top.id);
      if (top.id === 'confirm-modal' && state.confirmResolve) {
        state.confirmResolve(false);
        state.confirmResolve = null;
      }
    }
  });

  /* ============================================ 视图切换 */

  function showLogin(msg, isWarn) {
    stopListTimer();
    $('app-view').classList.add('hidden');
    $('login-view').classList.remove('hidden');
    if (msg) {
      $('login-msg').textContent = msg;
      $('login-msg').className = 'msg' + (isWarn ? ' warn' : '');
    }
  }

  function showApp() {
    $('login-view').classList.add('hidden');
    $('app-view').classList.remove('hidden');
    startListTimer();
    state.wsEnabled = true;
    syncComposer();
    connectWs();
    loadOptions();
    loadUsage('today');
    loadSchedules();
    // 会话列表要按工作区分组，所以先拿到工作区清单再渲染列表
    loadWorkspaces().then(function () {
      loadSessions(true).then(restoreLastSession);
    });
    if (isMobileLayout()) setSidebarOpen(false);
  }

  /* ============================================ 登录 */

  $('login-form').addEventListener('submit', function (e) {
    e.preventDefault();
    var input = $('key-input');
    var key = input.value;
    if (!key) { input.focus(); return; }

    var btn = $('login-btn');
    btn.disabled = true;
    btn.textContent = '验证中…';

    req('/api/login', { method: 'POST', body: JSON.stringify({ key: key }) })
      .then(function (r) {
        var d = r.data;
        if (d.ok) {
          input.value = '';
          $('login-msg').textContent = '';
          $('attempt-hint').textContent = '';
          showApp();
          syncMe();
          return;
        }
        if (d.banned) {
          showLogin(d.error || '此 IP 已被永久封禁');
          $('attempt-hint').textContent = '尝试次数已用尽，该地址已被永久拒绝访问。';
          btn.disabled = true;
          btn.textContent = '已封禁';
          return;
        }
        showLogin(d.error || '密钥错误');
        if (typeof d.remaining === 'number') {
          $('attempt-hint').textContent = '剩余尝试次数 ' + d.remaining + ' 次。用尽后此 IP 将被永久封禁。';
        }
        btn.disabled = false;
        btn.textContent = '进入控制台';
        input.select();
      })
      .catch(function () {
        showLogin('网络异常，请稍后重试');
        btn.disabled = false;
        btn.textContent = '进入控制台';
      });
  });

  /* ============================================ WebSocket（保活 + 自动重连） */

  function wsUrl() {
    var proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    return proto + '//' + location.host + '/ws';
  }

  function wsOpen() {
    return !!(state.ws && state.ws.readyState === WebSocket.OPEN);
  }

  function connectWs() {
    if (!state.wsEnabled) return;
    if (state.ws) {
      try { state.ws.close(); } catch (e) {}
      state.ws = null;
    }
    state.wsManualClose = false;

    var sock;
    try {
      sock = new WebSocket(wsUrl());
    } catch (e) {
      scheduleReconnect();
      return;
    }
    state.ws = sock;

    sock.onopen = function () {
      if (state.ws !== sock) return;
      state.wsAttempt = 0;
      state.wsLastInbound = Date.now();
      startWsKeepalive();
      onWsReady();
    };

    sock.onmessage = function (ev) {
      if (state.ws !== sock) return;
      state.wsLastInbound = Date.now();   // 任何帧（含 pong）都算链路活着
      var data;
      try { data = JSON.parse(ev.data); } catch (e) { return; }
      handleEvent(data);
    };

    sock.onclose = function () {
      if (state.ws !== sock) return;      // 已被新连接取代，忽略旧连接的关闭
      state.ws = null;
      stopWsKeepalive();
      if (state.wsManualClose) return;
      scheduleReconnect();
    };

    sock.onerror = function () {};
  }

  function startWsKeepalive() {
    stopWsKeepalive();
    state.wsPingTimer = setInterval(function () {
      wsSend({ type: 'ping' });
    }, WS_PING_INTERVAL);
    state.wsIdleTimer = setInterval(function () {
      // 切到后台时浏览器会把定时器节流到分钟级，此时不能据此判定断线，
      // 否则手机锁屏/切 App 回来会看到一次无谓的重连。
      if (document.hidden) { state.wsLastInbound = Date.now(); return; }
      if (!state.wsLastInbound) return;
      if (Date.now() - state.wsLastInbound > WS_IDLE_LIMIT) forceReconnect();
    }, WS_IDLE_CHECK);
  }

  function stopWsKeepalive() {
    clearInterval(state.wsPingTimer); state.wsPingTimer = null;
    clearInterval(state.wsIdleTimer); state.wsIdleTimer = null;
  }

  function forceReconnect() {
    if (!state.wsEnabled) return;
    var sock = state.ws;
    if (sock) { try { sock.close(); } catch (e) {} }
    state.ws = null;
    stopWsKeepalive();
    // 半开连接不一定触发 onclose，这里直接排程，避免永久卡死
    scheduleReconnect();
  }

  function scheduleReconnect() {
    if (!state.wsEnabled || state.wsTimer || state.wsManualClose) return;
    state.wsAttempt += 1;
    // 连续失败多次后确认一次登录态：会话令牌过期时不该无限重连
    if (state.wsAttempt === 5) {
      req('/api/me').then(function (r) {
        if (!r.data || !r.data.ok) {
          state.wsEnabled = false;
          state.wsManualClose = true;
          if (state.wsTimer) { clearTimeout(state.wsTimer); state.wsTimer = null; }
          clearMessages();
          showLogin('登录状态已失效，请重新输入密钥');
        }
      }).catch(function () {});
    }
    var delay = Math.min(800 * Math.pow(1.7, state.wsAttempt - 1), 15000);
    delay = Math.round(delay * (0.85 + Math.random() * 0.3));
    setStatus('连接中断，' + Math.max(1, Math.round(delay / 1000)) + ' 秒后自动重连…');
    state.wsTimer = setTimeout(function () {
      state.wsTimer = null;
      connectWs();
    }, delay);
  }

  // 回到前台立刻自检：断线就马上重连，不必等看门狗周期
  document.addEventListener('visibilitychange', function () {
    if (document.hidden || !state.wsEnabled) return;
    if (!state.ws) {
      scheduleReconnect();
      return;
    }
    if (state.ws.readyState !== WebSocket.OPEN) {
      forceReconnect();
      return;
    }
    state.wsLastInbound = Date.now();
    // 后台标签页会节流定时器，回前台立刻补一次增量
    if (state.tail && state.tail.sid) scheduleTail(0);
  });

  function onWsReady() {
    // 断线期间用户发过消息：直接补发，不走 watch（那一轮还没开始，回放会串内容）
    if (state.pendingSend) {
      setStatus('已重新连接，正在提交…');
      flushPendingSend();
      return;
    }
    // 生成过程中掉线：丢掉半截气泡，靠服务端回放把本轮续上
    if (state.sessionId && isRunning(state.sessionId)) {
      if ((state.running[state.sessionId] || {}).external) {
        // 外部进程的轮次本来就没有事件流可续，内容靠库里的增量尾随刷新，
        // 重连时别把已经渲染好的气泡清掉
        return;
      }
      resetLiveBubble();
      wsSend({ type: 'watch', sessionId: state.sessionId });
      setStatus('连接已恢复，正在恢复输出…');
      clearTimeout(state.wsResumeTimer);
      state.wsResumeTimer = setTimeout(reconcileAfterReconnect, 6000);
      return;
    }
    setStatus('');
    if (state.sessionId) loadSessions(true);
  }

  // 清掉正在流式渲染的半截气泡，让回放的事件重新生成它
  function resetLiveBubble() {
    var cur = state.cur;
    if (!cur) return;
    if (cur.row && cur.row.parentNode) cur.row.parentNode.removeChild(cur.row);
    if (cur.mid) state.messageMap.delete(cur.mid);
    state.cur = null;
  }

  // 重连后 6 秒内没等到 resumed：说明那一轮其实已经跑完，用历史接口对齐并收尾
  function reconcileAfterReconnect() {
    state.wsResumeTimer = null;
    if (!state.sessionId || !isRunning(state.sessionId)) return;
    var sid = state.sessionId;
    req('/api/sessions/' + encodeURIComponent(sid) + '/messages').then(function (r) {
      if (state.sessionId !== sid) return;
      if (r.data && r.data.ok) {
        clearMessages();
        (r.data.messages || []).forEach(function (m) { renderHistoryMessage(m); });
        clearCursors();
        scrollToBottom(true);
      }
    }).catch(function () {}).then(function () {
      if (state.sessionId !== sid) return;
      removeThinking();
      markRunning(sid, false);
      setStatus('');
    });
  }

  function flushPendingSend() {
    var p = state.pendingSend;
    if (!p || !wsOpen()) return;
    state.pendingSend = null;
    wsSend({
      type: 'send', sessionId: p.sessionId, content: p.content,
      mode: p.mode, model: p.model, thoughtLevel: p.thoughtLevel,
    });
  }

  function wsSend(obj) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      state.ws.send(JSON.stringify(obj));
      return true;
    }
    return false;
  }

  // 切走之后仍在后台跑的会话：只更新它的状态并提示，绝不写进当前视图
  function handleBackgroundEvent(d) {
    var sid = d.sessionId;
    switch (d.type) {
      case 'accepted':
      case 'started':
      case 'resumed':
        markRunning(sid, true, { external: false });
        markAcked(sid);
        break;
      case 'done':
        markRunning(sid, false);
        toast('「' + titleOf(sid) + '」已在后台执行完成', 'ok');
        loadSessions(true);
        loadUsage(null, true);
        break;
      case 'cancelled':
        markRunning(sid, false);
        toast('「' + titleOf(sid) + '」已停止生成');
        loadSessions(true);
        break;
      case 'error':
        markRunning(sid, false);
        toast('「' + titleOf(sid) + '」' + (d.message || '出错了'), 'err');
        loadSessions(true);
        break;
      case 'title':
        loadSessions(true);
        break;
      case 'notice':
        toast('「' + titleOf(sid) + '」' + (d.message || ''));
        break;
    }
  }

  function handleEvent(d) {
    // 会话级事件只渲染给「当前正在看的那个会话」——后台会话的输出绝不能
    // 混进当前视图，否则切回来会发现两个会话的内容串在一起。
    if (d.sessionId && d.sessionId !== state.sessionId) {
      handleBackgroundEvent(d);
      return;
    }
    switch (d.type) {
      case 'ready':
        break;

      case 'accepted':
        markAcked(state.sessionId);
        setStatus('已提交，模型正在处理…');
        break;

      case 'started':
        markRunning(state.sessionId, true, { external: false });
        ensureAssistant(d.messageId);
        setStatus('模型正在处理…');
        break;

      case 'title':
        if (d.title) $('chat-title').textContent = d.title;
        break;

      case 'delta':
        appendDelta(d.messageId, d.text);
        break;

      case 'reasoning':
        appendReasoning(d.messageId, d.text);
        break;

      case 'tool':
        handleTool(d);
        break;

      case 'final':
        applyFinalText(d.messageId, d.text);
        break;

      case 'turnEnd':
        if (d.response) applyFinalText(d.messageId, d.response);
        break;

      case 'meta':
        break;

      case 'resumed':
        clearTimeout(state.wsResumeTimer);
        state.wsResumeTimer = null;
        markRunning(state.sessionId, true, { external: false });
        setStatus('正在恢复输出…');
        break;

      case 'idle':
        // 服务端明确说「没有可续的流」：不用再等 resumed，交给库里的增量尾随
        clearTimeout(state.wsResumeTimer);
        state.wsResumeTimer = null;
        break;

      case 'cancelling':
        setStatus('正在停止…');
        var sb = $('stop-btn');
        if (sb) sb.disabled = true;
        break;

      case 'cancelled':
        toast('已停止生成');
        break;

      case 'notice':
        // 自动续跑 / 自动重试的进度提示
        toast(d.message || '');
        break;

      case 'model-scheduled':
        // 服务端定时切换模型的通知：scope = switch | revert | resume | sync
        if (d.scope === 'switch') {
          var msg = '模型定时已生效：' + ((d.active && d.active.modelLabel) || '');
          // 有会话因为在跑长时间工具没停下来时，如实说明「还没接上」，
          // 免得用户以为任务已经被续上了（它会在空闲后自动补发）。
          var rp = d.report || {};
          if (rp.pending) {
            msg += '（' + rp.pending + ' 个会话在跑长任务，空闲后会自动续跑）';
          }
          if (rp.resumeSkipped) {
            msg += '（' + rp.resumeSkipped + ' 个会话的续跑提示未被接受）';
          }
          toast(msg, (rp.pending || rp.resumeSkipped) ? 'err' : 'ok');
          refreshSchedViews();
        } else if (d.scope === 'revert') {
          toast('定时时段结束，模型已切回');
          refreshSchedViews();
        } else if (d.scope === 'resume') {
          // 之前没停下来的会话，现在空闲了、续跑提示已补发成功
          if (d.ok) toast('定时切换：已自动接上之前未中断的任务');
          refreshSchedViews();
        } else if (d.scope === 'sync') {
          // 老服务端可能不带 pending 字段，这种情况下别把已经拿到的值抹成 0
          var prevRt = state.scheduleRuntime || {};
          var prevPend = prevRt.pending || 0;
          state.scheduleRuntime = Object.assign({}, prevRt, {
            active: d.active || null,
            pending: (d.pending === undefined ? prevPend : d.pending) || 0,
          });
          renderSchedBanner();
          renderModelSelects();
        }
        break;

      case 'done':
        finishTurn(d);
        break;

      case 'error':
        if (d.sessionId && (d.message || '').indexOf('正在执行中') >= 0) {
          // 服务端说它在跑，说明本地状态落后了：纠正成「执行中」，别清成空闲，
          // 否则用户以为能发消息，而 app-server 只会把新消息压进队列。
          markRunning(d.sessionId, true);
          markAcked(d.sessionId);
          setStatus('');
          toast(d.message, 'err');
          break;
        }
        // 只有明确的会话级错误才动「执行中」标记。协议级错误（不带 sessionId，
        // 例如「当前没有正在执行的对话」）跟会话是否在跑无关，误清会让用户
        // 以为可以发新消息，而 app-server 那边其实还在跑，消息会被静默压进队列。
        // 唯一例外是「乐观等确认」的窗口期：刚发出去就被拒了，得把界面放回来。
        if (d.sessionId
            || (state.sessionId && state.running[state.sessionId]
                && !state.running[state.sessionId].acked)) {
          setSending(false);
        }
        setStatus('');
        toast(d.message || '出错了', 'err');
        clearCursors();
        break;
    }
  }

  /* ============================================ 消息渲染 */

  function clearMessages() {
    var box = $('messages');
    box.innerHTML = '';
    state.messageMap.clear();
    state.cur = null;
    state.localUserMsgs = [];
    state.msgPage = { hasMore: false, oldestSeq: null, loading: false,
                      total: 0, rendered: 0 };
  }

  /* ---------- 历史消息分页：最近 50 条，滚到顶部继续加载更早 ---------- */

  function olderLoaderEl() { return document.querySelector('#messages .load-older'); }

  function updateOlderLoader() {
    var box = $('messages');
    var p = state.msgPage;
    var el = olderLoaderEl();
    if (!p.hasMore || p.oldestSeq == null || !state.sessionId) {
      if (el && el.parentNode) el.parentNode.removeChild(el);
      return;
    }
    var rest = Math.max(0, p.total - p.rendered);
    if (!el) {
      el = document.createElement('div');
      el.className = 'load-older';
      el.addEventListener('click', function () { loadOlderMessages(); });
      box.insertBefore(el, box.firstChild);
    }
    el.textContent = '加载更早的消息（还有 ' + rest + ' 条）';
  }

  function loadOlderMessages() {
    var p = state.msgPage;
    var box = $('messages');
    if (!p.hasMore || p.loading || p.oldestSeq == null || !state.sessionId) return;
    var sid = state.sessionId;
    p.loading = true;
    var prevH = box.scrollHeight, prevTop = box.scrollTop;
    var el = olderLoaderEl();
    if (el) { el.textContent = '加载中…'; }
    req('/api/sessions/' + encodeURIComponent(sid) +
        '/messages?limit=50&before=' + p.oldestSeq).then(function (r) {
      p.loading = false;
      if (state.sessionId !== sid) return;
      if (!r.data.ok) {
        toast(r.data.error || '加载更早消息失败', 'err');
        updateOlderLoader();
        return;
      }
      var msgs = r.data.messages || [];
      // msgs 是时间正序；逐条“插到最上面”必须从新到旧遍历，否则整页顺序颠倒
      for (var i = msgs.length - 1; i >= 0; i--) renderHistoryMessage(msgs[i], true);
      p.rendered += msgs.length;
      p.hasMore = !!r.data.hasMore;
      p.oldestSeq = (msgs.length ? msgs[0].seq : null);
      if (!p.hasMore && el) { el = olderLoaderEl(); if (el && el.parentNode) el.parentNode.removeChild(el); }
      // 插入后补偿滚动位置，让视口停在与之前相同的消息上，而不是跳到底部
      box.scrollTop = box.scrollHeight - prevH + prevTop;
      updateOlderLoader();
    }).catch(function () {
      p.loading = false;
      if (state.sessionId === sid) updateOlderLoader();
    });
  }

  function hideWelcome() {
    var w = $('welcome');
    if (w && w.parentNode) w.parentNode.removeChild(w);
  }

  function showThinking() {
    removeThinking();
    hideWelcome();
    var box = $('messages');
    var row = document.createElement('div');
    row.className = 'row';
    row.id = 'thinking-row';
    row.innerHTML =
      '<div class="assistant"><div class="avatar">Z</div>' +
      '<div class="assistant-body"><div class="thinking">' +
      '<span>正在处理</span><span class="dots"><i></i><i></i><i></i></span>' +
      '<span id="thinking-elapsed"></span></div></div></div>';
    box.appendChild(row);
    scrollToBottom(true);
  }

  function removeThinking() {
    var row = $('thinking-row');
    if (row && row.parentNode) row.parentNode.removeChild(row);
  }

  function ensureAssistant(messageId) {
    var mid = messageId || 'live';
    // 一轮对话只会有一个助手气泡。上游各事件带的 ID 字段并不统一：
    //   model.streaming -> assistantMessageId
    //   session.updated -> messageId
    //   turn.completed  -> 没有 ID
    // 若按 ID 严格比对，一条长回答会被渲染成两个（甚至三个）气泡。
    // 因此只要本轮气泡还活着（state.cur 非空）就直接复用，顺便把真实 ID 补上。
    if (state.cur) {
      if (state.cur.mid === 'live' && mid !== 'live') state.cur.mid = mid;
      return state.cur;
    }
    var entry = state.messageMap.get(mid);
    if (entry) { state.cur = entry; return entry; }

    removeThinking();
    hideWelcome();

    var box = $('messages');
    var row = document.createElement('div');
    row.className = 'row';
    var wrap = document.createElement('div');
    wrap.className = 'assistant';
    var avatar = document.createElement('div');
    avatar.className = 'avatar';
    avatar.textContent = 'Z';
    var body = document.createElement('div');
    body.className = 'assistant-body';
    wrap.appendChild(avatar);
    wrap.appendChild(body);
    row.appendChild(wrap);
    box.appendChild(row);

    entry = { mid: mid, role: 'assistant', row: row, body: body, mdEl: null, raw: '' };
    state.messageMap.set(mid, entry);
    state.cur = entry;
    scrollToBottom();
    return entry;
  }

  function mdElOf(entry) {
    if (!entry.mdEl || !entry.mdEl.parentNode) {
      var el = document.createElement('div');
      el.className = 'md';
      entry.body.appendChild(el);
      entry.mdEl = el;
    }
    return entry.mdEl;
  }

  function addMsgTimeEl(entry, timeMs) {
    if (!entry || !entry.body || entry.timeEl) return;
    var el = document.createElement('div');
    el.className = 'msg-time';
    el.textContent = fmtMsgTime(timeMs || Date.now());
    entry.body.appendChild(el);
    entry.timeEl = el;
  }

  function appendDelta(messageId, text) {
    if (!text) return;
    var entry = ensureAssistant(messageId);
    entry.raw += text;
    var el = mdElOf(entry);
    el.dataset.raw = entry.raw;
    el.innerHTML = mdParse(entry.raw) + '<span class="cursor-blink"></span>';
    scrollToBottom();
  }

  function applyFinalText(messageId, text) {
    if (!text) return;
    var entry = ensureAssistant(messageId);
    // 短回复上游不吐分片，整段文本只在 final / turnEnd 里出现一次；
    // 这里做「对账」：和已流出的文本不一致就用权威文本整段替换。
    if (text === entry.raw) return;
    entry.raw = text;
    var el = mdElOf(entry);
    el.dataset.raw = text;
    el.innerHTML = mdParse(text);
    scrollToBottom();
  }

  function appendReasoning(messageId, text) {
    if (!text) return;
    var entry = ensureAssistant(messageId);
    var last = entry.body.lastElementChild;
    if (!last || !last.classList.contains('fold') || last.dataset.kind !== 'reason') {
      var fold = document.createElement('details');
      fold.className = 'fold';
      fold.dataset.kind = 'reason';
      var sum = document.createElement('summary');
      sum.textContent = '思考过程';
      var fb = document.createElement('div');
      fb.className = 'fold-body';
      fold.appendChild(sum);
      fold.appendChild(fb);
      entry.body.appendChild(fold);
      entry.mdEl = null;
      last = fold;
    }
    var body = last.querySelector('.fold-body');
    body.textContent = (body.textContent || '') + text;
    scrollToBottom();
  }

  function handleTool(d) {
    var entry = ensureAssistant(d.messageId);
    var id = d.toolCallId || 'tool';
    var card = entry.body.querySelector('[data-tcid="' + id + '"]');
    if (!card) {
      card = document.createElement('div');
      card.className = 'tool';
      card.dataset.tcid = id;
      card.innerHTML =
        '<div class="tool-head"><span class="tool-name"></span>' +
        '<span class="tool-desc"></span><span class="tool-status"></span></div>';
      entry.body.appendChild(card);
      entry.mdEl = null;
    }
    var nameEl = card.querySelector('.tool-name');
    var descEl = card.querySelector('.tool-desc');
    var statEl = card.querySelector('.tool-status');

    if (d.toolName) nameEl.textContent = d.toolName;
    if (d.input !== undefined && d.input !== null) {
      descEl.textContent = describeTool(d.toolName, d.input);
      var old = card.querySelector('.tool-body.in');
      if (old) old.parentNode.removeChild(old);
      var ib = document.createElement('div');
      ib.className = 'tool-body in';
      ib.textContent = formatToolInput(d.toolName, d.input);
      card.appendChild(ib);
    }
    var label = {
      call: '调用中', input_delta: '调用中', input_end: '调用中',
      scheduled: '排队', started: '执行中', running: '执行中',
      completed: '完成', result: '完成', failed: '失败', error: '失败',
    }[d.state] || d.state || '';
    statEl.textContent = label;
    statEl.className = 'tool-status' +
      (d.state === 'completed' || d.state === 'result' ? '' :
        (d.state === 'failed' || d.state === 'error' ? ' error' : ' running'));

    if (d.payload) {
      var out = extractToolOutput(d.payload);
      if (out) {
        var ob = card.querySelector('.tool-body.out');
        if (!ob) {
          ob = document.createElement('div');
          ob.className = 'tool-body out';
          card.appendChild(ob);
        }
        ob.textContent = out.length > 4000 ? out.slice(0, 4000) + '\n…（输出过长已截断）' : out;
      }
    }
    scrollToBottom();
  }

  function extractToolOutput(payload) {
    var cands = [payload.output, payload.result, payload.text, payload.error];
    for (var i = 0; i < cands.length; i++) {
      if (typeof cands[i] === 'string' && cands[i]) return cands[i];
      if (cands[i] && typeof cands[i] === 'object') {
        try { return JSON.stringify(cands[i], null, 2); } catch (e) {}
      }
    }
    return '';
  }

  function describeTool(tool, input) {
    if (!input || typeof input !== 'object') return '';
    if (input.description) return String(input.description);
    if (input.command) return String(input.command);
    if (input.filePath) return String(input.filePath);
    if (input.path) return String(input.path);
    if (input.pattern) return String(input.pattern);
    if (input.query) return String(input.query);
    if (input.skill) return String(input.skill);
    var keys = Object.keys(input);
    if (!keys.length) return '';
    var v = input[keys[0]];
    return typeof v === 'string' ? v : JSON.stringify(v);
  }

  function formatToolInput(tool, input) {
    if (!input) return '';
    if (typeof input === 'string') return input;
    if (input.command) return '$ ' + input.command;
    if (input.skill) return '技能：' + input.skill + (input.args ? '\n任务：' + input.args : '');
    try { return JSON.stringify(input, null, 2); } catch (e) { return String(input); }
  }

  function appendUserMessage(text, prepend, timeMs, beforeEl) {
    hideWelcome();
    var box = $('messages');
    var row = document.createElement('div');
    row.className = 'row user';
    var bubble = document.createElement('div');
    bubble.className = 'bubble-user';
    bubble.textContent = text;
    row.appendChild(bubble);
    var timeEl = document.createElement('div');
    timeEl.className = 'msg-time';
    timeEl.textContent = fmtMsgTime(timeMs || Date.now());
    row.appendChild(timeEl);
    if (prepend) box.insertBefore(row, box.firstChild);
    else if (beforeEl) box.insertBefore(row, beforeEl);
    else { box.appendChild(row); scrollToBottom(true); }
    // 记一笔：库里那条同样的用户消息稍后会被尾随取回来，靠它认领，别重复渲染
    state.localUserMsgs.push({ text: text, row: row, bound: false });
    if (state.localUserMsgs.length > 30) state.localUserMsgs.shift();
    return row;
  }

  function clearCursors() {
    var cursors = document.querySelectorAll('.cursor-blink');
    for (var i = 0; i < cursors.length; i++) {
      if (cursors[i].parentNode) cursors[i].parentNode.removeChild(cursors[i]);
    }
  }

  function finishTurn(d) {
    clearCursors();
    removeThinking();
    setSending(false);
    setStatus('');
    if (state.cur && state.cur.raw) {
      var el = state.cur.mdEl;
      if (el) el.innerHTML = mdParse(state.cur.raw);
    }
    if (state.cur) addMsgTimeEl(state.cur, d && d.time ? d.time : Date.now());
    state.cur = null;
    loadSessions(true).then(function () {
      if (state.tab === 'active') renderActive();
      var item = document.querySelector('.session-item[data-sid="' + state.sessionId + '"]');
      if (item) {
        var tEl = item.querySelector('.session-item-title');
        if (tEl && tEl.textContent) $('chat-title').textContent = tEl.textContent;
      } else {
        setTimeout(function () { loadSessions(true); }, 2500);
      }
    });
    if (d && d.usage && d.usage.totalTokens) {
      $('chat-meta').textContent = '本轮 ' + fmtTokens(d.usage.totalTokens) + ' tokens';
    }
    // 一轮跑完刷新右上角的今日用量（用量行可能晚一两秒落库，再补一次）
    loadUsage(null, true);
    setTimeout(function () { loadUsage(null, true); }, 3000);
  }

  /* ============================================ 状态与发送 */

  // ---- 执行状态：按会话分开记 ----------------------------------------
  // 一个会话在跑，不影响你切到别的会话接着干活；切回来时状态要还在，
  // 后台跑完了也要能通知到。
  function isRunning(sid) { return !!(sid && state.running[sid]); }

  function titleOf(sid) {
    var list = state.tab === 'archived' ? (state.archived || []) : (state.sessions || []);
    var s = list.filter(function (x) { return x.id === sid; })[0];
    return (s && s.title) || '会话';
  }

  // 模型 ID 常常带厂商前缀（z-ai/glm-5.3-flash），侧栏只显示末段省地方
  function shortModel(mid) {
    var s = String(mid || '');
    var i = s.lastIndexOf('/');
    return i >= 0 ? s.slice(i + 1) : s;
  }

  function markRunning(sid, on, opts) {
    if (!sid) return;
    if (on) {
      if (!state.running[sid]) state.running[sid] = { since: Date.now() };
      // external：外部进程在跑（面板停不掉它，输入框要如实说明）
      if (opts && opts.external === false) delete state.running[sid].external;
      else if (opts && opts.external) state.running[sid].external = true;
    } else if (state.running[sid]) {
      delete state.running[sid];
    }
    if (sid === state.sessionId) syncComposer();
    renderRunningMarks();
  }

  // 服务端确认收到这一轮之后才叫 acked：在那之前失败要允许界面回退
  function markAcked(sid) {
    var r = state.running[sid];
    if (r) r.acked = true;
  }

  // 左侧列表上的「执行中」小圆点
  function renderRunningMarks() {
    var list = state.tab === 'archived' ? (state.archived || []) : (state.sessions || []);
    var items = document.querySelectorAll('.session-item');
    for (var i = 0; i < items.length; i++) {
      var sid = items[i].dataset.sid;
      var s = list.filter(function (x) { return x.id === sid; })[0];
      var on = isRunning(sid) || !!(s && s.status === 'running');
      items[i].classList.toggle('running', on);
    }
  }

  // 输入区只反映「当前正在看的这个会话」
  function syncComposer() {
    var on = isRunning(state.sessionId);
    var ext = !!(on && (state.running[state.sessionId] || {}).external);
    state.sending = on;
    $('send-btn').disabled = on;
    var stop = $('stop-btn');
    if (stop) {
      stop.classList.toggle('hidden', !on);
      // 外部进程在跑的轮次面板停不掉，按钮如实说明，别让用户点了没反应
      stop.disabled = ext;
      stop.textContent = ext ? '外部任务执行中' : '停止生成';
    }
    if (on) {
      state.startedAt = (state.running[state.sessionId] || {}).since || Date.now();
      clearInterval(state.statusTimer);
      state.statusTimer = setInterval(function () {
        var el = $('thinking-elapsed');
        if (el) el.textContent = ' ' + Math.round((Date.now() - state.startedAt) / 1000) + 's';
      }, 1000);
    } else {
      clearInterval(state.statusTimer);
      state.statusTimer = null;
    }
  }

  // 兼容旧调用：不传 sid 就作用于当前会话
  function setSending(on, sid) {
    markRunning(sid || state.sessionId, on);
  }

  function setStatus(text) { $('status-text').textContent = text || ''; }

  // 上一次「生效过」的模型选择。切换失败时用它把界面退回去，不让选择器说谎。
  var lastModelValue = '';

  function currentModelValue() {
    var v = $('model-select').value;
    if (!v) return null;
    var p = v.split('|');
    return { providerId: p[0], modelId: p.slice(1).join('|') };
  }

  // 每个会话各用各的模型：改选择器立刻下发给「当前这个会话」。
  // 会话正在执行时不打断它 —— 服务端会把改动登记成下一轮生效。
  function applySessionModel(sel) {
    if (!state.sessionId) {
      toast('请先新建会话', 'err');
      return;
    }
    var mv = currentModelValue();
    if (!mv) { toast('没有可用的模型', 'err'); return; }
    var label = sel.options[sel.selectedIndex]
      ? sel.options[sel.selectedIndex].textContent : mv.modelId;
    sel.disabled = true;
    req('/api/sessions/' + encodeURIComponent(state.sessionId) + '/settings', {
      method: 'POST',
      body: JSON.stringify({ model: mv }),
    }).then(function (r) {
      sel.disabled = false;
      if (r.status === 401) { showLogin('登录已过期，请重新输入密钥'); return; }
      if (!r.data.ok) {
        toast(r.data.error || '切换模型失败', 'err');
        if (lastModelValue) sel.value = lastModelValue;
        renderModelSelects();
        return;
      }
      lastModelValue = sel.value;
      if (r.data.pending) {
        toast('已记下：这个会话本轮跑完后用「' + label + '」', 'ok');
      } else {
        toast('本会话已切到「' + label + '」', 'ok');
      }
      loadSessions(true);
    }).catch(function () {
      sel.disabled = false;
      toast('网络异常', 'err');
      if (lastModelValue) sel.value = lastModelValue;
    });
  }

  function send() {
    var input = $('input');
    var text = input.value.trim();
    if (!text || isRunning(state.sessionId)) return;
    if (!state.sessionId) {
      toast('请先新建会话', 'err');
      openNewChat();
      return;
    }
    if (!wsOpen()) {
      // 断线不丢消息：先把消息落到界面，等重连成功后自动补发
      appendUserMessage(text, false, Date.now());
      input.value = '';
      input.style.height = 'auto';
      setSending(true);
      showThinking();
      state.pendingSend = {
        sessionId: state.sessionId,
        content: text,
        mode: $('mode-select').value,
        model: currentModelValue(),
        thoughtLevel: $('thought-select').value || null,
      };
      setStatus('连接中断，正在重连，恢复后自动发送…');
      if (!state.ws) forceReconnect();
      return;
    }

    appendUserMessage(text, false, Date.now());
    input.value = '';
    input.style.height = 'auto';

    setSending(true);
    showThinking();

    var ok = wsSend({
      type: 'send',
      sessionId: state.sessionId,
      content: text,
      mode: $('mode-select').value,
      model: currentModelValue(),
      thoughtLevel: $('thought-select').value || null,
    });

    if (!ok) {
      setSending(false);
      removeThinking();
      toast('发送失败，请刷新页面', 'err');
    }
  }

  $('send-btn').addEventListener('click', send);

  $('stop-btn').addEventListener('click', function () {
    if (!isRunning(state.sessionId)) return;
    if (!wsOpen()) {
      toast('连接中断，正在重连，请稍候再停止', 'err');
      return;
    }
    this.disabled = true;
    this.textContent = '正在停止…';
    wsSend({ type: 'cancel', sessionId: state.sessionId });
  });

  var MQ_COARSE = window.matchMedia ? window.matchMedia('(pointer: coarse)') : null;
  if (MQ_COARSE && MQ_COARSE.matches) {
    $('input').placeholder = '输入消息…（点右侧按钮发送）';
  }
  $('input').addEventListener('keydown', function (e) {
    // 软键盘上 Enter 只换行，发送交给发送按钮；物理键盘仍保留 Enter 直发
    if (e.key === 'Enter' && !e.shiftKey && !e.isComposing && !(MQ_COARSE && MQ_COARSE.matches)) {
      e.preventDefault();
      send();
    }
  });

  $('input').addEventListener('input', function () {
    this.style.height = 'auto';
    this.style.height = Math.min(this.scrollHeight, 200) + 'px';
  });

  /* ============================================ 模型 / 思考等级 / 模式 */

  function loadOptions(force) {
    var q = force ? '?refresh=1' : '';
    return req('/api/options' + q).then(function (r) {
      if (r.status === 401) { showLogin('登录已过期，请重新输入密钥'); return; }
      if (!r.data.ok) { toast(r.data.error || '读取模型目录失败', 'err'); return; }
      state.options = r.data;
      if (r.data.schedule) {
        state.scheduleRuntime = r.data.schedule;
        renderSchedBanner();
      }
      renderModelSelects();
      return r.data;
    }).catch(function () {});
  }

  function renderModelSelects() {
    var o = state.options;
    if (!o) return;
    var models = o.models || [];
    var cur = o.currentModel || {};
    // 模型目录刷新时不能把当前会话的模型重置成全局默认模型。
    var active = (state.sessions || []).filter(function (s) {
      return s.id === state.sessionId;
    })[0];
    if (active && active.model && active.model.modelId) cur = active.model;

    var opts = models.map(function (m) {
      var v = (m.providerId || '') + '|' + (m.modelId || '');
      var sel = (m.providerId === cur.providerId && m.modelId === cur.modelId);
      return '<option value="' + esc(v) + '"' + (sel ? ' selected' : '') + '>' +
        esc(m.label || m.modelId) + (m.providerLabel ? ' · ' + esc(m.providerLabel) : '') +
        '</option>';
    }).join('');

    var sel1 = $('model-select');
    var keep1 = sel1.value;
    sel1.innerHTML = opts || '<option value="">（没有可用模型）</option>';
    if (keep1 && !sel1.value) sel1.value = keep1;
    lastModelValue = sel1.value;

    var sel2 = $('nc-model');
    var keep2 = sel2.value;
    sel2.innerHTML = opts || '<option value="">（没有可用模型）</option>';
    if (keep2 && !sel2.value) sel2.value = keep2;

    var levels = (o.thoughtLevel || {}).available || [];
    var lvOpts = levels.map(function (l) {
      var v = l.value || l;
      return '<option value="' + esc(v) + '">' + esc(l.label || v) + '</option>';
    }).join('');
    if (!lvOpts) lvOpts = '<option value="">默认</option>';

    ['thought-select', 'nc-thought'].forEach(function (id) {
      var el = $(id);
      var keep = el.value;
      el.innerHTML = lvOpts;
      var want = keep || (o.thoughtLevel || {}).current || '';
      if (want) {
        var found = false;
        for (var i = 0; i < el.options.length; i++) {
          if (el.options[i].value === want) { found = true; break; }
        }
        if (found) el.value = want;
      }
    });

    var wm = $('welcome-model');
    if (wm) {
      var cm = models.filter(function (m) {
        return m.providerId === cur.providerId && m.modelId === cur.modelId;
      })[0];
      wm.textContent = cm ? (cm.label || cm.modelId) : '—';
    }

    // 定时生效期间，模型由服务端强制指定，选择器锁住并说明原因，
    // 免得用户改了没用、还以为坏了。
    var locked = !!(state.scheduleRuntime && state.scheduleRuntime.active);
    var lockTip = locked
      ? '模型定时生效中：' + ((state.scheduleRuntime.active.modelLabel) || '') +
        '，需要先「恢复原模型」才能手动改'
      : '';
    ['model-select', 'nc-model'].forEach(function (id) {
      var el = $(id);
      if (!el) return;
      el.disabled = locked;
      el.classList.toggle('locked', locked);
      var wrap = el.closest ? el.closest('.pick') : null;
      if (wrap) wrap.title = locked ? lockTip : (id === 'model-select' ? '本会话使用的模型' : '');
    });
  }

  function renderModeSelects() {
    var modes = state.modes || [];
    var html = modes.map(function (m) {
      return '<option value="' + esc(m.value) + '">' + esc(m.label) +
        (m.short ? '（' + esc(m.short) + '）' : '') + '</option>';
    }).join('');
    if (!html) html = '<option value="build">构建</option>';

    var s1 = $('mode-select');
    var keep = s1.value;
    s1.innerHTML = html;
    if (keep) s1.value = keep;

    var s2 = $('nc-mode');
    s2.innerHTML = html;

    var s3 = $('set-default-mode');
    s3.innerHTML = html;

    updateModeDesc();
  }

  function modeDesc(value) {
    var modes = state.modes || [];
    for (var i = 0; i < modes.length; i++) {
      if (modes[i].value === value) return modes[i].desc || '';
    }
    return '';
  }

  function updateModeDesc() {
    var sel = $('mode-select');
    var v = sel.value;
    // 说明小字挪到 tooltip：右上角现在放今日用量，模式选择器也移到了输入框下方
    sel.title = modeDesc(v) || '本会话的权限模式';
    var w = $('welcome-mode');
    if (w) {
      var modes = state.modes || [];
      var label = v;
      for (var i = 0; i < modes.length; i++) {
        if (modes[i].value === v) { label = modes[i].label; break; }
      }
      w.textContent = label;
    }
  }

  $('mode-select').addEventListener('change', updateModeDesc);
  // 改了模型就只影响当前这个会话（不重启 app-server、不影响别的会话）
  $('model-select').addEventListener('change', function () { applySessionModel(this); });

  // 连续执行：收尾自动续跑 + 出错自动重试；「停止生成」是总闸
  function setAutoRunUI(on) {
    var btn = $('auto-run-btn');
    if (!btn) return;
    btn.classList.toggle('active', !!on);
  }

  $('auto-run-btn').addEventListener('click', function () {
    var sid = state.sessionId;
    if (!sid) { toast('请先选择会话', 'err'); return; }
    var btn = $('auto-run-btn');
    var enable = !btn.classList.contains('active');
    btn.disabled = true;
    req('/api/sessions/' + encodeURIComponent(sid) + '/auto-run', {
      method: 'POST', body: JSON.stringify({ enabled: enable })
    }).then(function (r) {
      btn.disabled = false;
      if (!r.data.ok) { toast(r.data.error || '设置失败', 'err'); return; }
      setAutoRunUI(enable);
      var s = state.sessions.filter(function (x) { return x.id === sid; })[0];
      if (s) s.autoRun = enable;
      renderRunningMarks();
      toast(enable ? '连续执行已开启：收尾后自动续跑，出错自动重试（点「停止生成」可随时关）'
                   : '连续执行已关闭', enable ? 'ok' : '');
    }).catch(function () { btn.disabled = false; toast('设置失败', 'err'); });
  });

  $('nc-mode').addEventListener('change', function () {
    $('nc-mode-desc').textContent = modeDesc(this.value);
  });

  /* ============================================ 会话列表 */

  function startListTimer() {
    stopListTimer();
    state.listTimer = setInterval(function () {
      // 就算当前会话在跑，列表也要继续刷新：后台可能还有别的会话在跑，
      // 得让「执行中」标记和完成状态及时反映出来。
      loadSessions(true);
    }, 20000);
  }

  function stopListTimer() {
    if (state.listTimer) { clearInterval(state.listTimer); state.listTimer = null; }
  }

  function loadSessions(silent) {
    var url = state.tab === 'archived' ? '/api/sessions?archived=1' : '/api/sessions';
    return req(url).then(function (r) {
      if (r.status === 401) { showLogin('登录已过期，请重新输入密钥'); return; }
      if (r.status === 403) { showLogin(r.data.error || '此 IP 已被永久封禁'); return; }
      if (!r.data.ok) {
        if (!silent) toast(r.data.error || '获取会话列表失败', 'err');
        return;
      }
      var list = r.data.sessions || [];
      if (state.tab === 'archived') state.archived = list; else state.sessions = list;
      renderSessions(list);
    }).catch(function () {
      if (!silent) toast('网络异常，无法获取会话列表', 'err');
    });
  }

  function renderSessions(list) {
    var box = $('session-list');
    box.innerHTML = '';

    var groups = groupByWorkspace(list);

    if (!list.length && !groups.length) {
      var empty = document.createElement('div');
      empty.className = 'list-empty';
      empty.innerHTML = state.tab === 'archived'
        ? '没有归档的会话<br>在会话上点「⋯」可以归档'
        : '还没有会话<br>点击「新建会话」开始';
      box.appendChild(empty);
      return;
    }

    groups.forEach(function (g) {
      box.appendChild(wsGroupEl(g));
    });
  }

  // 按工作区把会话分组。已知工作区即使还没有会话也列出来，
  // 这样左侧就是一份完整的工作区清单（和 zcode 桌面端一致）。
  function groupByWorkspace(list) {
    var groups = [];
    var index = {};

    list.forEach(function (s) {
      var key = s.workspacePath || '';
      var g = index[key];
      if (!g) {
        g = index[key] = { key: key, path: key, name: wsNameOf(key), items: [], empty: false };
        groups.push(g);
      }
      g.items.push(s);
    });

    if (state.tab !== 'archived') {
      (state.workspaces || []).forEach(function (w) {
        if (!w || !w.path || index[w.path]) return;
        if (w.purpose === 'task') return;   // 一次性任务的目录不占工作区列表
        var g = { key: w.path, path: w.path, name: w.name || wsNameOf(w.path),
                  items: [], empty: true, ws: w };
        index[w.path] = g;
        groups.push(g);
      });
    }
    return groups;
  }

  function wsNameOf(path) {
    if (!path) return '未指定工作区';
    var w = (state.workspaces || []).filter(function (x) { return x.path === path; })[0];
    if (w && w.name) return w.name;
    var p = String(path).replace(/\/+$/, '').split('/').pop();
    return p || path;
  }

  function wsGroupEl(g) {
    var wrap = document.createElement('div');
    wrap.className = 'ws-group';

    var collapsed = !!state.wsCollapsed[g.key];
    var head = document.createElement('div');
    head.className = 'ws-group-head' + (collapsed ? ' collapsed' : '');
    head.title = g.path || g.name;

    var caret = document.createElement('span');
    caret.className = 'ws-caret';
    caret.innerHTML = '&#9662;';
    head.appendChild(caret);

    var name = document.createElement('span');
    name.className = 'ws-name';
    name.textContent = g.name;
    head.appendChild(name);

    var cnt = document.createElement('span');
    cnt.className = 'ws-count';
    cnt.textContent = g.items.length;
    head.appendChild(cnt);

    // 工作区标题旁的「+」：不用打开弹窗，直接在这个工作区下创建会话。
    if (state.tab !== 'archived' && g.path) {
      var add = document.createElement('button');
      add.type = 'button';
      add.className = 'ws-add-session';
      add.innerHTML = '&#43;';
      add.title = '在「' + g.name + '」下新建会话';
      add.setAttribute('aria-label', add.title);
      add.addEventListener('click', function (e) {
        e.stopPropagation();
        createSessionInWorkspace(g.path, add);
      });
      head.appendChild(add);
    }

    var more = document.createElement('button');
    more.type = 'button';
    more.className = 'ws-more';
    more.innerHTML = '&#8943;';
    more.title = '管理工作区';
    more.addEventListener('click', function (e) {
      e.stopPropagation();
      var w = g.ws || findWorkspace(g.path);
      if (!w && g.path) {
        w = { path: g.path, name: g.name, purpose: 'project', unregistered: true, dir: {} };
      }
      if (!w) { toast('请先刷新工作区列表', 'err'); return; }
      openWsModal('edit', w);
    });
    head.appendChild(more);

    head.addEventListener('click', function () {
      state.wsCollapsed[g.key] = !state.wsCollapsed[g.key];
      saveWsCollapsed();
      head.classList.toggle('collapsed', !!state.wsCollapsed[g.key]);
      body.classList.toggle('hidden', !!state.wsCollapsed[g.key]);
    });

    wrap.appendChild(head);

    var body = document.createElement('div');
    body.className = 'ws-group-body' + (collapsed ? ' hidden' : '');
    if (!g.items.length) {
      var e = document.createElement('div');
      e.className = 'ws-group-empty';
      e.textContent = '还没有会话';
      body.appendChild(e);
    } else {
      g.items.forEach(function (s) { body.appendChild(sessionItemEl(s)); });
    }
    wrap.appendChild(body);
    return wrap;
  }

  function sessionItemEl(s) {
    var item = document.createElement('div');
    item.className = 'session-item' + (s.id === state.sessionId ? ' active' : '');
    item.dataset.sid = s.id;
    item.title = s.title;

    var main = document.createElement('div');
    main.className = 'session-item-main';

    var t = document.createElement('div');
    t.className = 'session-item-title';
    t.textContent = s.title || '(无标题)';
    if (s.archived) {
      var b = document.createElement('span');
      b.className = 'badge-arch';
      b.textContent = '已归档';
      t.appendChild(b);
    }

    var sub = document.createElement('div');
    sub.className = 'session-item-sub';
    var when = s.archived ? relTime(s.archivedAt) : relTime(s.updated || s.created);
    var whenEl = document.createElement('span');
    whenEl.textContent = when;
    sub.appendChild(whenEl);
    if (!s.archived) {
      // 每个会话各用各的模型，侧栏直接标出来，不用逐个点开看。
      // 这个会话没单独设过模型时显示「跟随默认」，语义上才准确。
      var pick = s.pendingModel || s.model;
      var mid = pick && pick.modelId;
      var isDefault = false;
      if (!mid) {
        var cm = (state.options && state.options.currentModel) || {};
        if (cm.modelId) { pick = cm; mid = cm.modelId; isDefault = true; }
      }
      if (mid) {
        var mt = document.createElement('span');
        mt.className = 'model-tag' + (s.pendingModel ? ' pending'
          : (isDefault ? ' default' : ''));
        mt.textContent = shortModel(mid);
        mt.title = (s.pendingModel ? '下一轮起使用：'
          : (isDefault ? '跟随默认模型：' : '本会话模型：')) +
          ((pick.providerId || '') + '/' + mid);
        sub.appendChild(mt);
      }
      // 连续执行中的会话给个小标记，一眼知道它会自己续跑
      if (s.autoRun) {
        var at = document.createElement('span');
        at.className = 'model-tag auto';
        at.textContent = '连续';
        at.title = '连续执行中：收尾后自动续跑，出错自动重试';
        sub.appendChild(at);
      }
      // 后台还在跑的会话要一眼看得出来（不只在打开它的那个标签页里）
      var tag = document.createElement('span');
      tag.className = 'run-tag';
      tag.innerHTML = '<i class="run-pulse"></i>执行中';
      sub.appendChild(tag);
    }
    if (s.status === 'running') item.classList.add('running');

    main.appendChild(t);
    main.appendChild(sub);
    item.appendChild(main);

    var more = document.createElement('button');
    more.className = 'session-more';
    more.type = 'button';
    more.innerHTML = '&#8943;';
    more.addEventListener('click', function (e) {
      e.stopPropagation();
      toggleSessionMenu(item, s);
    });
    item.appendChild(more);

    item.addEventListener('click', function () { openSession(s.id); });
    // 桌面端习惯：在左侧会话上右键直接打开同一套操作菜单。
    // 不触发 openSession，避免用户只是想归档/删除时打断当前对话。
    item.addEventListener('contextmenu', function (e) {
      e.preventDefault();
      e.stopPropagation();
      showSessionMenu(item, s);
    });
    return item;
  }

  function closeSessionMenus() {
    var all = document.querySelectorAll('.session-menu');
    for (var i = 0; i < all.length; i++) {
      if (all[i].parentNode) all[i].parentNode.removeChild(all[i]);
    }
  }

  function toggleSessionMenu(item, s) {
    var exist = item.querySelector('.session-menu');
    closeSessionMenus();
    if (exist) return;
    showSessionMenu(item, s);
  }

  function showSessionMenu(item, s) {
    closeSessionMenus();
    var menu = document.createElement('div');
    menu.className = 'session-menu';
    menu.setAttribute('role', 'menu');
    menu.setAttribute('aria-label', '会话操作');

    if (s.archived) {
      addMenuItem(menu, '取消归档', function () { archiveSession(s.id, false); });
      addMenuItem(menu, '彻底删除', function () { deleteSession(s.id); }, true);
    } else {
      addMenuItem(menu, '归档', function () { archiveSession(s.id, true); });
      addMenuItem(menu, '彻底删除', function () { deleteSession(s.id); }, true);
    }
    item.appendChild(menu);
  }

  function addMenuItem(menu, label, fn, danger) {
    var b = document.createElement('button');
    b.type = 'button';
    b.textContent = label;
    if (danger) b.className = 'danger';
    b.addEventListener('click', function (e) {
      e.stopPropagation();
      if (menu.parentNode) menu.parentNode.removeChild(menu);
      fn();
    });
    menu.appendChild(b);
  }

  function archiveSession(sid, archived) {
    var title = (state.sessions.concat(state.archived).filter(function (x) {
      return x.id === sid;
    })[0] || {}).title || sid;
    var verb = archived ? '归档' : '取消归档';
    var tip = archived
      ? '归档后它会从会话列表移到「归档」里，随时可以取消归档。'
      : '它会回到会话列表。';
    confirmBox(verb + '会话', '确定要' + verb + '「' + title + '」吗？\n' + tip, verb)
      .then(function (ok) {
        if (!ok) return;
        toast(verb + '中…');
        req('/api/sessions/' + encodeURIComponent(sid) + '/archive', {
          method: 'POST', body: JSON.stringify({ archived: archived }),
        }).then(function (r) {
          if (!r.data.ok) { toast(r.data.error || (verb + '失败'), 'err'); return; }
          toast(verb + '成功', 'ok');
          if (state.sessionId === sid && archived) newChat();
          loadSessions(true);
        }).catch(function () { toast('网络异常', 'err'); });
      });
  }

  function deleteSession(sid, force) {
    var title = (state.sessions.concat(state.archived).filter(function (x) {
      return x.id === sid;
    })[0] || {}).title || sid;
    confirmBox('彻底删除会话',
      '确定要彻底删除「' + title + '」吗？\n它的全部消息记录会从数据库里删掉，无法恢复。',
      '彻底删除').then(function (ok) {
      if (!ok) return;
      req('/api/sessions/' + encodeURIComponent(sid) + (force ? '?force=1' : ''),
          { method: 'DELETE' })
        .then(function (r) {
          // 会话正在跑：后端挡了一道（409）。问一次「停止并删除」，确认后带
          // force 重发——面板会先停掉这一轮、摘掉联动的巡检定时任务，再彻底删除。
          if (r.status === 409 && !force) {
            confirmBox('会话正在执行',
              '「' + title + '」正在执行任务。\n先停止它再彻底删除，关联的定时任务也会一起删除。',
              '停止并删除').then(function (ok2) {
              if (ok2) deleteSession(sid, true);
            });
            return;
          }
          if (!r.data.ok) { toast(r.data.error || '删除失败', 'err'); return; }
          toast(force && r.data.stopped ? '已停止并删除' : '已删除', 'ok');
          if (state.sessionId === sid) newChat();
          loadSessions(true);
        }).catch(function () { toast('网络异常', 'err'); });
    });
  }

  $('session-seg').addEventListener('click', function (e) {
    var b = e.target.closest ? e.target.closest('.seg-btn') : null;
    if (!b) return;
    state.tab = b.dataset.tab;
    var btns = this.querySelectorAll('.seg-btn');
    for (var i = 0; i < btns.length; i++) btns[i].classList.toggle('active', btns[i] === b);
    loadSessions(true);
  });

  function openSession(sid) {
    if (sid === state.sessionId) return;

    state.sessionId = sid;
    rememberSession(sid);
    renderActive();
    // 先按已知状态摆好输入区（这个会话可能正在后台跑，那就不该让再发消息）
    syncComposer();

    clearMessages();
    $('chat-title').textContent = '加载中…';
    $('chat-meta').textContent = '';

    req('/api/sessions/' + encodeURIComponent(sid) + '/messages?limit=50').then(function (r) {
      // 期间用户又切到别的会话了：这份结果已经不属于当前视图，直接丢弃，
      // 否则快速来回切会让上一个会话的内容渲染进来。
      if (state.sessionId !== sid) return;
      if (!r.data.ok) {
        toast(r.data.error || '加载对话失败', 'err');
        $('chat-title').textContent = '会话';
        return;
      }
      var msgs = r.data.messages || [];
      msgs.forEach(function (m) { renderHistoryMessage(m); });
      state.msgPage.rendered = msgs.length;
      state.msgPage.total = r.data.total || msgs.length;
      state.msgPage.hasMore = !!r.data.hasMore;
      state.msgPage.oldestSeq = (msgs.length ? msgs[0].seq : null);
      clearCursors();
      scrollToBottom(true);
      updateOlderLoader();

      $('chat-title').textContent = r.data.title || '会话';
      $('chat-meta').textContent = (r.data.workspacePath || '') + ' · 最近 ' +
        msgs.length + ' / 共 ' + state.msgPage.total + ' 条';

      // 把会话自己的模式/模型同步到选择器。老会话在 session/list 里
      // 可能没有模型字段，此时从最近一条助手消息恢复，避免沿用上一个会话。
      var s = state.sessions.filter(function (x) { return x.id === sid; })[0];
      if (s) {
        var sm = s.model;
        if (!(sm && sm.modelId)) {
          for (var mi = msgs.length - 1; mi >= 0; mi--) {
            if (msgs[mi].role !== 'assistant' || !msgs[mi].model) continue;
            sm = ((state.options || {}).models || []).filter(function (m) {
              return m.modelId === msgs[mi].model;
            })[0];
            if (sm) break;
          }
        }
        applySessionSettings(s, sm);
      }

      // 实时性：这个会话不管此刻跑没跑，都把「增量尾随」挂上。
      // 外部进程（禅道自动执行器）自己拉起 app-server，它的事件永远进不了面板
      // 的事件流，只有定时读库才能做到「不用切换/刷新就看到新消息」。
      startTail(sid, r.data.cursor || 0);

      // 用服务端状态校准「它还在不在跑」：网页关着的时候它可能已经跑完了。
      if (r.data.wsLive) {
        // 面板自己驱动的轮次：走事件流，能拿到逐 token 的增量
        markRunning(sid, true, { external: false });
        showThinking();
        wsSend({ type: 'watch', sessionId: sid });
        setStatus('已接回后台执行，正在恢复输出…');
      } else if (r.data.dbLive && r.data.active) {
        markRunning(sid, true, { external: true });
        showThinking();
        setStatus('这个会话正在外部执行，输出自动刷新…');
      } else if (r.data.dbLive) {
        // 未收尾但已停住：残留轮次，不锁输入框（尾随会持续纠正状态）
        markRunning(sid, false);
        setStatus('这个会话上一轮没有正常收尾（外部进程已停止）');
      } else {
        markRunning(sid, false);
        setStatus('');
      }
      if (isMobileLayout()) setSidebarOpen(false);
    });
  }

  function applySessionSettings(s, fallbackModel) {
    if (s.mode) {
      $('mode-select').value = s.mode;
      updateModeDesc();
    }
    setAutoRunUI(!!s.autoRun);
    var m = fallbackModel || s.model;
    if (!(m && m.providerId && m.modelId)) {
      m = (state.options && state.options.currentModel) || {};
    }
    if (m.providerId && m.modelId) {
      var v = m.providerId + '|' + m.modelId;
      var sel = $('model-select');
      var found = false;
      for (var i = 0; i < sel.options.length; i++) {
        if (sel.options[i].value === v) { sel.value = v; found = true; break; }
      }
      if (!found) {
        var fallback = document.createElement('option');
        fallback.value = v;
        fallback.textContent = shortModel(m.modelId) + '（当前使用，目录中不可用）';
        sel.appendChild(fallback);
        sel.value = v;
      }
    }
    // 切进来的会话可能带着「下一轮才生效」的模型，也提示一下
    var pm = s.pendingModel;
    if (pm && pm.modelId) {
      setStatus('本会话模型：' + shortModel((s.model || {}).modelId || '—') +
                '，下一轮起用 ' + shortModel(pm.modelId));
    }
    lastModelValue = $('model-select').value;
  }

  // 一条助手消息 -> 一个完整气泡。历史加载、增量尾随、断线对齐都走这里，
  // 保证同一条消息无论从哪条路径来，渲染结果都一致。
  function buildAssistantRow(m) {
    var row = document.createElement('div');
    row.className = 'row';
    row.innerHTML = '<div class="assistant"><div class="avatar">Z</div>' +
      '<div class="assistant-body"></div></div>';
    var entry = { mid: m.id, role: 'assistant', row: row,
                  body: row.querySelector('.assistant-body'), mdEl: null, raw: '' };
    state.messageMap.set(m.id, entry);   // 先登记：part 里的 handleTool 要按 id 查它
    fillAssistantParts(entry, m);
    if (m.completed === false) appendLiveCursor(entry);
    else addMsgTimeEl(entry, m.time);
    return entry;
  }

  function fillAssistantParts(entry, m) {
    (m.parts || []).forEach(function (p) {
      if (p.type === 'text' && p.text) {
        var el = mdElOf(entry);
        entry.raw += p.text;
        el.dataset.raw = entry.raw;
        el.innerHTML = mdParse(entry.raw);
      } else if (p.type === 'reasoning' && p.text) {
        var last = entry.body.lastElementChild;
        if (!last || !last.classList.contains('fold') || last.dataset.kind !== 'reason') {
          var fold = document.createElement('details');
          fold.className = 'fold';
          fold.dataset.kind = 'reason';
          var sum = document.createElement('summary');
          sum.textContent = '思考过程';
          var fb = document.createElement('div');
          fb.className = 'fold-body';
          fold.appendChild(sum);
          fold.appendChild(fb);
          entry.body.appendChild(fold);
          entry.mdEl = null;
          last = fold;
        }
        var body = last.querySelector('.fold-body');
        body.textContent = (body.textContent || '') + p.text;
      } else if (p.type === 'tool') {
        var st = p.state || {};
        handleTool({
          messageId: m.id, toolCallId: p.callID || p.toolCallId,
          toolName: p.tool, state: st.status || 'completed',
          input: st.input, payload: { output: st.output, error: st.error },
        });
      }
    });
    if (m.error) {
      var ed = document.createElement('div');
      ed.className = 'tool';
      ed.innerHTML = '<div class="tool-head"><span class="tool-name">错误</span>' +
        '<span class="tool-status error">失败</span></div>' +
        '<div class="tool-body out">' + esc(typeof m.error === 'string'
          ? m.error : JSON.stringify(m.error)) + '</div>';
      entry.body.appendChild(ed);
    }
  }

  // 还没收尾的消息给个光标，表示这一轮还在往下写
  function appendLiveCursor(entry) {
    if (!entry || !entry.body) return;
    var el = document.createElement('span');
    el.className = 'cursor-blink';
    entry.body.appendChild(el);
  }

  function shouldRenderUserMessage(m) {
    // 后端根据协议来源和可见性标记过滤；没有标记的旧消息按不显示处理，
    // 避免把自动续跑、禅道执行、系统提醒误当成用户输入。
    return !!(m && m.role === 'user' && m.visibleToUser === true);
  }

  function renderHistoryMessage(m, prepend, beforeEl) {
    if (m.role === 'user') {
      if (!shouldRenderUserMessage(m)) return;
      var texts = (m.parts || []).filter(function (p) { return p.type === 'text'; })
        .map(function (p) { return p.text || ''; }).join('');
      if (texts) appendUserMessage(texts, prepend, m.time, beforeEl);
      return;
    }
    if (m.role !== 'assistant') return;

    removeThinking();
    hideWelcome();
    var entry = buildAssistantRow(m);
    var box = $('messages');
    if (prepend) box.insertBefore(entry.row, box.firstChild);
    // 底部可能有「正在处理」占位行，新内容要插在它前面
    else box.insertBefore(entry.row, beforeEl || $('thinking-row') || null);
  }

  /* ---------- 实时尾随：外部进程跑的会话只能靠读库刷新 ---------- */

  function messageSignature(m) {
    try { return JSON.stringify([m.completed, m.error, m.parts || []]); }
    catch (e) { return ''; }
  }

  // 把一条库里的消息同步进当前视图（按 id 幂等：内容没变就什么都不做）
  function upsertTailMessage(m) {
    if (!m || !m.id) return;
    if (m.role === 'user') {
      if (!shouldRenderUserMessage(m)) return;
      if (state.messageMap.get(m.id)) return;
      var texts = (m.parts || []).filter(function (p) { return p.type === 'text'; })
        .map(function (p) { return p.text || ''; }).join('');
      if (!texts) return;
      // 自己刚发出去、本地已经渲染过的那条：认领它，别出现两条一样的
      var locals = state.localUserMsgs || [];
      for (var i = 0; i < locals.length; i++) {
        if (!locals[i].bound && locals[i].text === texts) {
          locals[i].bound = true;
          locals[i].row.dataset.mid = m.id;
          state.messageMap.set(m.id, { mid: m.id, role: 'user', row: locals[i].row });
          return;
        }
      }
      var row = appendUserMessage(texts, false, m.time);
      if (row) {
        row.dataset.mid = m.id;
        state.messageMap.set(m.id, { mid: m.id, role: 'user', row: row });
        state.msgPage.total += 1;
        state.msgPage.rendered += 1;
      }
      return;
    }
    if (m.role !== 'assistant') return;

    var sig = messageSignature(m);
    var old = state.messageMap.get(m.id);
    if (old && old.sig === sig) return;      // 内容没变就别动 DOM，免得闪
    removeThinking();
    hideWelcome();
    state.cur = null;                        // 重建期间别让 part 渲染串到别的气泡
    var entry = buildAssistantRow(m);
    entry.sig = sig;
    var box = $('messages');
    if (old && old.row && old.row.parentNode) {
      box.insertBefore(entry.row, old.row);  // 原地替换，保住消息顺序
      old.row.parentNode.removeChild(old.row);
    } else {
      box.insertBefore(entry.row, $('thinking-row') || null);
      state.msgPage.total += 1;
      state.msgPage.rendered += 1;
    }
    if (m.completed === false) state.cur = entry;
    scrollToBottom();
  }

  function startTail(sid, cursor) {
    if (!sid) return;
    var t = state.tail;
    if (t.sid === sid && t.timer) {
      if (cursor) t.cursor = Math.max(t.cursor || 0, cursor);
      return;
    }
    stopTail();
    state.tail = { sid: sid, timer: null, cursor: cursor || 0,
                   busy: false, external: false, quiet: 0 };
    scheduleTail(0);
  }

  function stopTail() {
    var t = state.tail;
    if (t && t.timer) clearTimeout(t.timer);
    state.tail = { sid: null, timer: null, cursor: 0,
                   busy: false, external: false, quiet: 0 };
  }

  function scheduleTail(delay) {
    var t = state.tail;
    if (!t || !t.sid) return;
    if (t.timer) clearTimeout(t.timer);
    t.timer = setTimeout(pollTail, delay);
  }

  function pollTail() {
    var t = state.tail;
    if (!t || !t.sid || t.busy) return;
    if (t.sid !== state.sessionId) { stopTail(); return; }
    if (document.hidden) { scheduleTail(TAIL_INTERVAL); return; }
    var sid = t.sid;
    t.busy = true;
    req('/api/sessions/' + encodeURIComponent(sid) + '/tail?after=' + (t.cursor || 0))
      .then(function (r) {
        t.busy = false;
        if (!state.tail || state.tail !== t || state.sessionId !== sid) return;
        if (r.status === 401) { stopTail(); showLogin('登录已过期，请重新输入密钥'); return; }
        if (r.status === 403) { stopTail(); showLogin(r.data.error || '此 IP 已被永久封禁'); return; }
        var d = r.data || {};
        if (!d.ok) { scheduleTail(TAIL_QUIET_INTERVAL); return; }
        if (d.cursor) t.cursor = Math.max(t.cursor || 0, d.cursor);
        if (d.wsLive) {
          // 面板自己驱动的轮次有逐 token 的事件流，别用库里的整段内容去盖它
          t.external = false;
          t.quiet = 0;
          scheduleTail(TAIL_INTERVAL);
          return;
        }
        var changed = d.messages || [];
        changed.forEach(upsertTailMessage);
        if (d.live && d.active) {
          t.external = true;
          t.quiet = 0;
          markRunning(sid, true, { external: true });
          if (!state.cur) showThinking();
          setStatus('外部任务执行中，输出自动刷新…');
        } else if (d.live) {
          // 未收尾、但早就没动静了：多半是崩在半路的残留，别锁住输入框，
          // 也别把它当「正在跑」；隔几秒再探一次，它重新动起来会立刻接上。
          if (isRunning(sid)) markRunning(sid, false);
          removeThinking();
          t.quiet = Math.max(t.quiet || 0, 11);
          setStatus('这个会话上一轮没有正常收尾（外部进程已停止）');
        } else {
          if (t.external || isRunning(sid)) { finishExternalTurn(sid); return; }
          t.quiet = changed.length ? 0 : (t.quiet || 0) + 1;
        }
        scheduleTail(t.quiet > 10 ? TAIL_QUIET_INTERVAL : TAIL_INTERVAL);
      })
      .catch(function () {
        t.busy = false;
        if (state.tail === t) scheduleTail(TAIL_QUIET_INTERVAL);
      });
  }

  function finishExternalTurn(sid) {
    var wasExternal = !!(state.tail && state.tail.external);
    var wasRunning = isRunning(sid);
    stopTail();
    markRunning(sid, false);
    if (sid !== state.sessionId) return;
    clearCursors();
    removeThinking();
    setStatus('');
    if (state.cur && state.cur.raw) {
      var el = state.cur.mdEl;
      if (el) el.innerHTML = mdParse(state.cur.raw);
    }
    state.cur = null;
    if (wasExternal && wasRunning) toast('「' + titleOf(sid) + '」已完成', 'ok');
    loadSessions(true);
    loadUsage(null, true);
  }

  function renderActive() {
    var items = document.querySelectorAll('.session-item');
    for (var i = 0; i < items.length; i++) {
      items[i].classList.toggle('active', items[i].dataset.sid === state.sessionId);
    }
  }

  function welcomeHtml() {
    return '<div id="welcome" class="welcome">' +
      '<div class="welcome-mark">Z</div>' +
      '<h2>开始一个新对话</h2>' +
      '<p>点「新建会话」选择工作区与用途，或者在左侧挑一个已有会话接着聊。</p>' +
      '<div class="welcome-hint">' +
      '<span class="chip">模式 <b id="welcome-mode">构建</b></span>' +
      '<span class="chip">模型 <b id="welcome-model">—</b></span>' +
      '<span class="chip">工作区 <b id="welcome-ws">—</b></span>' +
      '</div></div>';
  }

  function newChat() {
    // 别的会话在后台跑着，也不影响你开一个新会话
    state.sessionId = null;
    state.cur = null;
    stopTail();
    rememberSession(null);
    clearMessages();
    $('messages').innerHTML = welcomeHtml();
    $('chat-title').textContent = '新会话';
    $('chat-meta').textContent = '';
    renderActive();
    updateModeDesc();
    renderModelSelects();
    var ww = $('welcome-ws');
    if (ww) ww.textContent = '/root';
    syncComposer();
    $('input').focus();
  }

  // 创建成功后切到新会话。新建会话弹窗和工作区「+」共用这段收尾逻辑。
  function activateCreatedSession(sessionId, wsPath, mode, model, thoughtLevel) {
    newChat();
    state.sessionId = sessionId;
    rememberSession(sessionId);
    if (model && model.providerId && model.modelId) {
      var v = model.providerId + '|' + model.modelId;
      var sel = $('model-select');
      for (var i = 0; i < sel.options.length; i++) {
        if (sel.options[i].value === v) { sel.value = v; break; }
      }
    }
    if (mode) $('mode-select').value = mode;
    updateModeDesc();
    if (thoughtLevel) $('thought-select').value = thoughtLevel;
    $('chat-title').textContent = '新会话';
    $('chat-meta').textContent = wsPath || '';
    var ww = $('welcome-ws');
    if (ww) ww.textContent = wsPath || '';
    loadSessions(true).then(renderActive);
    $('input').focus();
  }

  // 工作区标题旁「+」的直建逻辑：沿用当前会话的模式/模型/思考等级。
  function createSessionInWorkspace(wsPath, btn) {
    if (!wsPath) { toast('工作区路径无效', 'err'); return; }
    var model = currentModelValue();
    var mode = $('mode-select').value || 'build';
    var thoughtLevel = $('thought-select').value || null;
    var w = findWorkspace(wsPath);
    var purpose = (w && w.purpose === 'task') ? 'task' : 'project';
    var oldHtml = btn ? btn.innerHTML : '';
    if (btn) { btn.disabled = true; btn.innerHTML = '&#8230;'; }

    req('/api/sessions', {
      method: 'POST',
      body: JSON.stringify({
        workspacePath: wsPath,
        purpose: purpose,
        mode: mode,
        model: model,
        thoughtLevel: thoughtLevel,
      }),
    }).then(function (r) {
      if (btn) { btn.disabled = false; btn.innerHTML = oldHtml; }
      if (!r.data.ok) { toast(r.data.error || '新建会话失败', 'err'); return; }
      state.wsCollapsed[wsPath] = false;
      saveWsCollapsed();
      activateCreatedSession(r.data.sessionId, wsPath, mode, model, thoughtLevel);
      toast('已在「' + (w ? w.name : wsNameOf(wsPath)) + '」下创建会话', 'ok');
    }).catch(function () {
      if (btn) { btn.disabled = false; btn.innerHTML = oldHtml; }
      toast('网络异常', 'err');
    });
  }

  /* ============================================ 新建会话 */

  function openNewChat() {
    $('nc-mode').value = $('mode-select').value || 'build';
    $('nc-mode-desc').textContent = modeDesc($('nc-mode').value);
    loadWorkspaces().then(function () { openModal('newchat-modal'); });
  }

  function loadWorkspaces() {
    return req('/api/workspaces').then(function (r) {
      if (!r.data.ok) return;
      state.workspaces = r.data.workspaces || [];
      if (r.data.root) state.wsRoot = r.data.root;
      var sel = $('nc-workspace');
      var keep = sel.value;
      sel.innerHTML = state.workspaces.map(function (w) {
        return '<option value="' + esc(w.path) + '">' + esc(w.name) +
          '　' + esc(w.path) + (w.sessions ? '（' + w.sessions + '）' : '') + '</option>';
      }).join('') || '<option value="' + esc(state.wsRoot) + '">' + esc(state.wsRoot) + '</option>';
      if (keep) {
        for (var i = 0; i < sel.options.length; i++) {
          if (sel.options[i].value === keep) { sel.value = keep; break; }
        }
      }
      updateWsHint();
      return state.workspaces;
    });
  }

  function findWorkspace(path) {
    return (state.workspaces || []).filter(function (x) { return x.path === path; })[0] || null;
  }

  function updateWsHint() {
    var p = $('nc-workspace').value;
    var w = findWorkspace(p);
    $('nc-ws-hint').textContent = w
      ? '用途：' + (w.purpose === 'task' ? '任务' : '项目') +
        '　已有 ' + (w.sessions || 0) + ' 个会话　路径：' + p
      : p;
    if (w && w.purpose) setPurpose(w.purpose);
  }

  function setPurpose(p) {
    var cards = document.querySelectorAll('.purpose-card');
    for (var i = 0; i < cards.length; i++) {
      cards[i].classList.toggle('active', cards[i].dataset.purpose === p);
    }
  }

  function currentPurpose() {
    var a = document.querySelector('.purpose-card.active');
    return a ? a.dataset.purpose : 'project';
  }

  $('nc-workspace').addEventListener('change', updateWsHint);

  document.querySelectorAll('.purpose-card').forEach(function (c) {
    c.addEventListener('click', function () { setPurpose(this.dataset.purpose); });
  });

  /* ============================================ 工作区：新建 / 改名 / 删除 */

  // 与后端 slug 规则保持一致，仅用于「目录」留空时给用户看结果预览
  function wsSlug(name) {
    var s = String(name || '');
    s = s.replace(/[^0-9A-Za-z_\-\u4e00-\u9fff\u3400-\u4dbf\u3040-\u30ff\uac00-\ud7af]+/g, '-');
    return s.replace(/^-+|-+$/g, '').toLowerCase();
  }

  function wsResolveDir(name, path) {
    var root = (state.wsRoot || '/root').replace(/\/+$/, '');
    var p = String(path || '').trim();
    if (p) return p.charAt(0) === '/' ? p : root + '/' + p.replace(/^\/+/, '');
    var slug = wsSlug(name);
    return slug ? root + '/' + slug : root;
  }

  function updateWsPreview() {
    var name = $('wsm-name').value.trim();
    var raw = $('wsm-path').value.trim();
    var el = $('wsm-preview');
    if (!name && !raw) {
      el.textContent = '填名称或目录后，这里会显示最终使用的目录';
      return;
    }
    var dir = wsResolveDir(name, raw);
    var known = findWorkspace(dir);
    el.textContent = '将使用目录：' + dir + (known ? '（已存在的工作区，会更新它的名称）' : '');
  }

  function updatePurgeHint() {
    var ws = state.wsEditing;
    if (!ws) return;
    var st = ws.dir || {};
    if ($('wsm-purge').checked) {
      $('wsm-danger-hint').textContent = st.exists
        ? '将连同磁盘目录一起删除，目录内的文件无法恢复。'
        : '目录不存在，勾选与不勾选结果一样。';
    } else {
      $('wsm-danger-hint').textContent = '不勾选时只从左侧列表移除，磁盘上的文件原样保留。';
    }
  }

  function openWsModal(mode, ws, onDone) {
    var isEdit = mode === 'edit' && !!ws;
    state.wsEditing = isEdit ? ws : null;
    state.wsOnDone = onDone || null;

    $('wsm-title').textContent = isEdit ? '工作区设置' : '新建工作区';
    $('wsm-name').value = isEdit ? (ws.name || '') : '';
    $('wsm-path').value = isEdit ? (ws.path || '') : '';
    $('wsm-purpose').value = (ws && ws.purpose) || 'project';
    $('wsm-path').readOnly = isEdit;
    $('wsm-purpose-row').classList.toggle('hidden', isEdit);
    $('wsm-save').textContent = isEdit ? '保存名称' : '创建';
    // 未登记的目录（别的途径建的会话带出来的）只能「保存名称」把它登记进来；
    // 工作区根目录不允许移除，避免把 /root 从列表里弄掉。
    var canDelete = isEdit && !ws.unregistered && !ws.isRoot;
    $('wsm-delete').classList.toggle('hidden', !canDelete);
    $('wsm-danger-row').classList.toggle('hidden', !(isEdit && !ws.unregistered));
    $('wsm-danger-row').querySelector('.check-row').classList.toggle('hidden', !canDelete);
    $('wsm-purge').checked = false;

    if (canDelete) {
      var st = ws.dir || {};
      $('wsm-dir').textContent = ws.path;
      $('wsm-entries').textContent = st.exists
        ? (st.entries + (st.truncated ? '+' : '')) : '0';
      updatePurgeHint();
    } else if (isEdit && ws.isRoot) {
      $('wsm-danger-hint').textContent = '这是工作区根目录，只能改名，不能移除。';
    } else if (isEdit) {
      $('wsm-danger-hint').textContent =
        '这个目录还没登记进工作区列表（是别的途径建的会话带出来的），保存名称即可登记。';
    }
    updateWsPreview();
    openModal('ws-modal');
    setTimeout(function () { $('wsm-name').focus(); }, 30);
  }

  $('wsm-name').addEventListener('input', updateWsPreview);
  $('wsm-path').addEventListener('input', updateWsPreview);
  $('wsm-purge').addEventListener('change', updatePurgeHint);

  $('wsm-save').addEventListener('click', function () {
    var name = $('wsm-name').value.trim();
    if (!name) { toast('请填写工作区名称', 'err'); return; }
    var btn = this;
    var editing = state.wsEditing;
    btn.disabled = true;

    var p;
    if (editing && !editing.unregistered) {
      p = req('/api/workspaces/' + encodeURIComponent(editing.id), {
        method: 'POST', body: JSON.stringify({ name: name }),
      });
    } else {
      // 新建，或把「未登记的目录」按路径登记进来
      p = req('/api/workspaces', {
        method: 'POST',
        body: JSON.stringify({
          name: name,
          purpose: editing ? (editing.purpose || 'project') : $('wsm-purpose').value,
          path: editing ? editing.path : $('wsm-path').value.trim(),
        }),
      });
    }

    p.then(function (r) {
      btn.disabled = false;
      if (!r.data.ok) { toast(r.data.error || '操作失败', 'err'); return; }
      var w = r.data.workspace || {};
      if (editing && !editing.unregistered) {
        toast('已重命名为「' + w.name + '」', 'ok');
      } else if (r.data.created) {
        toast('已创建工作区「' + w.name + '」', 'ok');
      } else {
        toast('工作区已存在，已更新登记信息', 'ok');
      }
      closeModal('ws-modal');
      loadWorkspaces().then(function () {
        renderSessions(state.tab === 'archived' ? state.archived : state.sessions);
        if (state.wsOnDone) state.wsOnDone(w);
        state.wsOnDone = null;
      });
    }).catch(function () {
      btn.disabled = false;
      toast('网络异常', 'err');
    });
  });

  $('wsm-delete').addEventListener('click', function () {
    var ws = state.wsEditing;
    if (!ws) return;
    var purge = $('wsm-purge').checked;
    var st = ws.dir || {};
    var text = '确定要移除工作区「' + (ws.name || ws.path) + '」吗？\n';
    if (purge) {
      text += '磁盘目录 ' + ws.path + '（' + st.entries + (st.truncated ? '+' : '') +
        ' 项）会被彻底删除，无法恢复。\n';
    } else {
      text += '只会从左侧列表移除，磁盘上的 ' + ws.path + ' 原样保留。\n';
    }
    if (ws.sessions) {
      text += '该工作区下的 ' + ws.sessions + ' 个会话记录不会被一起删除。';
    }
    confirmBox(purge ? '删除工作区目录' : '移除工作区', text, purge ? '删除' : '移除')
      .then(function (ok) {
        if (!ok) return;
        var url = '/api/workspaces/' + encodeURIComponent(ws.id) +
          (purge ? '?purge=1&confirm=' + encodeURIComponent(ws.path) : '');
        req(url, { method: 'DELETE' }).then(function (r) {
          if (!r.data.ok) { toast(r.data.error || '删除失败', 'err'); return; }
          toast(purge ? '工作区与磁盘目录已删除' : '已移出工作区列表', 'ok');
          closeModal('ws-modal');
          loadWorkspaces().then(function () {
            loadSessions(true);
          });
        }).catch(function () { toast('网络异常', 'err'); });
      });
  });

  $('ws-add-btn').addEventListener('click', function () { openWsModal('create'); });

  $('nc-new-ws').addEventListener('click', function () {
    openWsModal('create', null, function (w) {
      var sel = $('nc-workspace');
      if (w && w.path) {
        for (var i = 0; i < sel.options.length; i++) {
          if (sel.options[i].value === w.path) { sel.value = w.path; break; }
        }
      }
      updateWsHint();
    });
  });

  $('nc-edit-ws').addEventListener('click', function () {
    var p = $('nc-workspace').value;
    if (!p) return;
    openWsModal('edit', findWorkspace(p));
  });

  $('nc-create').addEventListener('click', function () {
    var btn = this;
    var wsPath = $('nc-workspace').value;
    var mv = $('nc-model').value;
    var model = mv ? { providerId: mv.split('|')[0], modelId: mv.split('|').slice(1).join('|') } : null;
    var mode = $('nc-mode').value;
    var thoughtLevel = $('nc-thought').value || null;
    btn.disabled = true;
    btn.textContent = '创建中…';
    req('/api/sessions', {
      method: 'POST',
      body: JSON.stringify({
        workspacePath: wsPath,
        purpose: currentPurpose(),
        mode: mode,
        model: model,
        thoughtLevel: thoughtLevel,
      }),
    }).then(function (r) {
      btn.disabled = false;
      btn.textContent = '创建并开始';
      if (!r.data.ok) { toast(r.data.error || '新建会话失败', 'err'); return; }
      closeModal('newchat-modal');
      activateCreatedSession(r.data.sessionId, wsPath, mode, model, thoughtLevel);
      toast('会话已创建，可以开始对话了', 'ok');
    }).catch(function () {
      btn.disabled = false;
      btn.textContent = '创建并开始';
      toast('网络异常', 'err');
    });
  });

  /* ============================================ 设置：框架 */

  var SETTINGS_PANES = ['general', 'security', 'models', 'schedule', 'usage'];

  function showSettingsPane(name) {
    if (SETTINGS_PANES.indexOf(name) < 0) name = 'general';
    document.querySelectorAll('#settings-modal .nav-item').forEach(function (x) {
      x.classList.toggle('active', x.dataset.pane === name);
    });
    SETTINGS_PANES.forEach(function (p) {
      var el = $('pane-' + p);
      if (el) el.classList.toggle('hidden', p !== name);
    });
    if (name === 'usage') loadUsage(state.usageRange, true);
    if (name === 'schedule') loadSchedules(true);
  }

  $('settings-btn').addEventListener('click', function () {
    openModal('settings-modal');
    // 每次打开都回到「官方配置」栏，免得上次停在自定义栏造成误解
    state.provScope = 'official';
    document.querySelectorAll('#prov-seg .seg-btn').forEach(function (b) {
      b.classList.toggle('active', b.dataset.scope === 'official');
    });
    loadSecurity();
    loadProviders();
    loadUsage(state.usageRange);
    loadSchedules();
  });

  document.querySelectorAll('#settings-modal .nav-item').forEach(function (b) {
    b.addEventListener('click', function () { showSettingsPane(b.dataset.pane); });
  });

  /* ============================================ 设置：通用 */

  function loadSecurity() {
    return req('/api/settings/security').then(function (r) {
      if (!r.data.ok) return;
      var d = r.data;
      $('set-key-input').value = d.key || '';
      $('set-key-input').type = 'password';
      $('set-auto-approve').checked = !!d.autoApprove;
      $('set-max-attempts').value = d.maxAttempts;
      $('set-ws-root').textContent = d.workspaceRoot || '/root';
      if (d.defaultMode) $('set-default-mode').value = d.defaultMode;
      $('settings-note').textContent = '当前来源 IP：' + (d.ip || '—') +
        (d.isDefault ? '（正在使用默认密钥）' : '');
      renderPermissions(d.permissions || []);
    }).catch(function () {});
  }

  function renderPermissions(list) {
    var box = $('perm-list');
    if (!list.length) {
      box.innerHTML = '<div class="perm-empty">还没有工具放行记录。</div>';
      return;
    }
    box.innerHTML = list.slice().reverse().map(function (p) {
      var allow = p.decision === 'allow';
      return '<div class="perm-row"><code>' + esc(p.toolName || '—') + '</code>' +
        '<span class="' + (allow ? 'ok' : 'no') + '">' + (allow ? '已放行' : '已拒绝') + '</span>' +
        '<span>' + esc(p.riskLevel || '') + '</span></div>';
    }).join('');
  }

  $('set-general-save').addEventListener('click', function () {
    var btn = this;
    btn.disabled = true;
    req('/api/settings/misc', {
      method: 'POST',
      body: JSON.stringify({
        autoApprove: $('set-auto-approve').checked,
        maxAttempts: parseInt($('set-max-attempts').value, 10) || 5,
        defaultMode: $('set-default-mode').value,
      }),
    }).then(function (r) {
      btn.disabled = false;
      if (!r.data.ok) { toast(r.data.error || '保存失败', 'err'); return; }
      toast('已保存', 'ok');
      syncMe();
    }).catch(function () { btn.disabled = false; toast('网络异常', 'err'); });
  });

  /* ============================================ 设置：登录安全 */

  $('set-key-eye').addEventListener('click', function () {
    var el = $('set-key-input');
    el.type = el.type === 'password' ? 'text' : 'password';
  });

  $('set-key-save').addEventListener('click', function () {
    var key = $('set-key-input').value.trim();
    if (key.length < 6) { $('set-key-msg').textContent = '密钥至少 6 位'; return; }
    var btn = this;
    btn.disabled = true;
    req('/api/settings/key', { method: 'POST', body: JSON.stringify({ key: key }) })
      .then(function (r) {
        btn.disabled = false;
        if (!r.data.ok) { $('set-key-msg').textContent = r.data.error || '更新失败'; return; }
        $('set-key-msg').textContent = '已更新。下次登录请用新密钥。';
        toast('登录密钥已更新', 'ok');
        loadSecurity();
      }).catch(function () { btn.disabled = false; $('set-key-msg').textContent = '网络异常'; });
  });

  $('set-key-default').addEventListener('click', function () {
    $('set-key-input').value = 'admin123';
    $('set-key-input').type = 'text';
    $('set-key-msg').textContent = '已填入默认密钥 admin123，点「更新密钥」生效。';
  });

  /* ============================================ 设置：模型配置 */

  function loadProviders(reveal) {
    return req('/api/settings/providers' + (reveal ? '?reveal=1' : ''))
      .then(function (r) {
        if (!r.data.ok) return;
        state.providers = r.data.providers || [];
        state.provSecret = r.data;
        renderProviders();
        $('prov-note').textContent = '配置文件：' + (r.data.configPath || '');
        return r.data;
      }).catch(function () {});
  }

  function renderProviders() {
    var scope = state.provScope;
    var list = state.providers.filter(function (p) { return p.scope === scope; });
    var box = $('prov-list');
    box.innerHTML = '';

    if (!list.length) {
      var e = document.createElement('div');
      e.className = 'prov-empty';
      e.innerHTML = scope === 'official'
        ? '官方配置里还没有厂商。<br>用 zcode 官方账号登录后，z.ai / 智谱 等官方入口会出现在这里。'
        : '还没有添加第三方厂商。<br>点右上角「+ 新增第三方厂商」开始。';
      box.appendChild(e);
      return;
    }

    list.forEach(function (p) {
      var card = document.createElement('div');
      card.className = 'prov-card';

      var head = document.createElement('div');
      head.className = 'prov-card-head';
      head.innerHTML = '<span class="prov-card-name">' + esc(p.name) + '</span>' +
        '<span class="prov-card-id">' + esc(p.providerId) + '</span>' +
        '<span class="prov-tag">' + esc(p.kind === 'anthropic' ? 'Anthropic' : 'OpenAI 兼容') + '</span>';
      if ((p.models || []).some(function (m) { return m.isMain; })) {
        var mt = document.createElement('span');
        mt.className = 'prov-tag main';
        mt.textContent = '默认模型';
        head.appendChild(mt);
      }
      card.appendChild(head);

      var sub = document.createElement('div');
      sub.className = 'prov-card-sub';
      sub.textContent = 'Base URL：' + (p.baseURL || '（未填）') +
        '\nAPI Key：' + (p.hasApiKey ? (p.apiKeyMasked || '已配置') : '（未配置）');
      sub.style.whiteSpace = 'pre-line';
      card.appendChild(sub);

      var ms = document.createElement('div');
      ms.className = 'prov-models';
      (p.models || []).forEach(function (m) {
        var c = document.createElement('span');
        c.className = 'prov-model-chip';
        c.textContent = (m.name || m.modelId) + (m.isMain ? ' ★' : '');
        ms.appendChild(c);
      });
      card.appendChild(ms);

      card.addEventListener('click', function () { openProvider(p.providerId); });
      box.appendChild(card);
    });
  }

  $('prov-seg').addEventListener('click', function (e) {
    var b = e.target.closest ? e.target.closest('.seg-btn') : null;
    if (!b) return;
    state.provScope = b.dataset.scope;
    var btns = this.querySelectorAll('.seg-btn');
    for (var i = 0; i < btns.length; i++) btns[i].classList.toggle('active', btns[i] === b);
    renderProviders();
  });

  $('prov-add').addEventListener('click', function () { openProvider(null); });

  function modelRow(m) {
    var box = document.createElement('div');
    box.className = 'model-item';
    var row = document.createElement('div');
    row.className = 'model-row';
    row.innerHTML =
      '<input class="w-id" placeholder="模型 ID，如 z-ai/glm-5.3-flash">' +
      '<input class="w-name" placeholder="显示名称">' +
      '<input class="w-num" placeholder="上下文" type="number" min="1">' +
      '<input class="w-num" placeholder="最大输出" type="number" min="1">' +
      '<button class="mini-btn probe" type="button" ' +
      'title="用这个模型发一条最小请求，验证厂商能不能通">测试</button>' +
      '<button class="icon-btn del" type="button" title="移除">&#10005;</button>';
    row.querySelector('.w-id').value = m.modelId || '';
    row.querySelector('.w-name').value = m.name || '';
    row.querySelector('.w-num').value = m.context || '';
    var nums = row.querySelectorAll('.w-num');
    if (nums[1]) nums[1].value = m.output || '';

    var msg = document.createElement('div');
    msg.className = 'probe-msg hidden';

    row.querySelector('.probe').addEventListener('click', function () {
      probeModel(box, row.querySelector('.w-id').value.trim(), this);
    });
    row.querySelector('.del').addEventListener('click', function () {
      if (box.parentNode) box.parentNode.removeChild(box);
    });

    box.appendChild(row);
    box.appendChild(msg);
    return box;
  }

  /* ============================================ 设置：厂商连通性（测试 / 拉模型） */

  function pvCtx(extra) {
    var out = {
      kind: $('pv-kind').value,
      baseURL: $('pv-base').value.trim(),
      providerId: state.editingProvider || undefined,
    };
    var key = $('pv-key').value;
    if (key) out.apiKey = key;          // 没填就交给服务端沿用已保存的密钥
    if (extra) for (var k in extra) out[k] = extra[k];
    return out;
  }

  function applyBaseFix(url, kind) {
    if (!url) return;
    $('pv-base').value = url;
    if (kind) $('pv-kind').value = kind;
  }

  function renderProbeResult(msg, res, base) {
    var html = '';
    if (res.ok) {
      msg.className = 'probe-msg good';
      html = '✅ 通了' + (res.latencyMs != null ? ' · ' + res.latencyMs + 'ms' : '') +
        '<br><span class="probe-url">POST ' + esc(res.url || '') + '</span>';
      if (res.reply) html += '<br>模型回复：<b>' + esc(res.reply) + '</b>';
      if (res.suggestBaseURL) {
        html += '<br>⚠ ' + esc(res.hint || '') +
          ' <button class="mini-btn fix" type="button" data-fix="' +
          esc(res.suggestBaseURL) + '" data-fixkind="' + esc(res.suggestKind || '') +
          '">一键填入</button>';
      }
    } else {
      msg.className = 'probe-msg bad';
      html = '❌ ' + esc(res.error || '失败') +
        (res.status ? ' <span class="probe-url">(HTTP ' + esc(res.status) + ')</span>' : '');
      if (res.hint) html += '<br>👉 ' + esc(res.hint);
      if (res.suggestBaseURL) {
        html += '<br><button class="mini-btn fix" type="button" data-fix="' +
          esc(res.suggestBaseURL) + '" data-fixkind="' + esc(res.suggestKind || '') +
          '">把 Base URL 改成 ' + esc(res.suggestBaseURL) + '</button>';
      }
      if ((res.tried || []).length > 1) {
        html += '<details class="probe-detail"><summary>试过的地址（' +
          res.tried.length + ' 个）</summary><div>' +
          res.tried.map(function (t) {
            // 光看状态码会被骗：智谱那种「200 里塞 404」的情况必须把响应体也显示出来
            var tag = t.status ? ('HTTP ' + t.status) : '';
            var d = t.detail || t.error || (t.ok ? '成功' : '无响应');
            return esc(t.url) + ' → ' + esc(tag + (tag && d ? ' · ' : '') + d);
          }).join('<br>') + '</div></details>';
      }
    }
    msg.innerHTML = html;
    var b = msg.querySelector('[data-fix]');
    if (b) b.addEventListener('click', function () {
      applyBaseFix(this.getAttribute('data-fix'), this.getAttribute('data-fixkind'));
      toast('已填入建议的 Base URL，再点一次「测试」确认', 'ok');
    });
  }

  function probeModel(box, modelId, btn) {
    var msg = box.querySelector('.probe-msg');
    var base = $('pv-base').value.trim();
    if (!base) { toast('先填 Base URL', 'err'); return; }
    if (!modelId) { toast('先填模型 ID，或点「获取模型列表」挑一个', 'err'); return; }
    // 模型没保存进配置前，服务端只能靠这里传的 kind/base/key 去探
    var old = btn.textContent;
    btn.disabled = true;
    btn.textContent = '测试中…';
    msg.className = 'probe-msg';
    msg.textContent = '正在请求 ' + base + ' …（跨境链路可能要十几秒）';
    req('/api/settings/provider-test', {
      method: 'POST',
      body: JSON.stringify(pvCtx({ modelId: modelId })),
    }).then(function (r) {
      btn.disabled = false;
      btn.textContent = old;
      if (!r.data.ok) {
        msg.className = 'probe-msg bad';
        msg.textContent = r.data.error || '测试请求失败';
        return;
      }
      renderProbeResult(msg, r.data.result || {}, base);
    }).catch(function () {
      btn.disabled = false;
      btn.textContent = old;
      msg.className = 'probe-msg bad';
      msg.textContent = '网络异常（面板到浏览器这一跳断了？）';
    });
  }

  function renderFetched(res) {
    var box = $('pv-fetched');
    var wrap = $('pv-fetched-wrap');
    box.innerHTML = '';
    (res.models || []).forEach(function (m) {
      var lab = document.createElement('label');
      lab.className = 'fetched-item';
      lab.innerHTML = '<input type="checkbox" value="' + esc(m.id) + '"' +
        ' data-name="' + esc(m.name || m.id) + '">' +
        '<span class="fetched-id">' + esc(m.id) + '</span>' +
        (m.name && m.name !== m.id
          ? '<span class="fetched-name">' + esc(m.name) + '</span>' : '');
      box.appendChild(lab);
    });
    $('pv-fetched-note').textContent = '共 ' + (res.count || 0) + ' 个 · 来自 ' +
      (res.url || '') + (res.truncated ? '（已截断显示）' : '');
    wrap.classList.remove('hidden');
    // 拉到的地址跟填的不一样：直接提示要不要改
    if (res.suggestBaseURL) {
      toast('模型列表是在 ' + res.suggestBaseURL + ' 上拿到的，建议把 Base URL 改成它', 'ok');
    }
  }

  function fetchModels(btn) {
    var base = $('pv-base').value.trim();
    if (!base) { toast('先填 Base URL', 'err'); return; }
    var old = btn.textContent;
    btn.disabled = true;
    btn.textContent = '拉取中…';
    req('/api/settings/provider-models', {
      method: 'POST',
      body: JSON.stringify(pvCtx()),
    }).then(function (r) {
      btn.disabled = false;
      btn.textContent = old;
      var res = (r.data || {}).result || {};
      if (!res.ok) {
        var t = '拉取失败：' + (res.error || '未知错误');
        if (res.hint) t += ' —— ' + res.hint;
        toast(t, 'err');
        if (res.suggestBaseURL) {
          confirmBox('换个 Base URL 试试？',
                     '拉取失败，但探测发现这个地址可用：\n' + res.suggestBaseURL +
                     '\n\n要不要填进去再拉一次？', '填入并重试')
            .then(function (ok) {
              if (!ok) return;
              applyBaseFix(res.suggestBaseURL);
              fetchModels(btn);
            });
        }
        return;
      }
      renderFetched(res);
      toast('拉到 ' + (res.count || 0) + ' 个模型，勾选后点「加入选中的模型」', 'ok');
    }).catch(function () {
      btn.disabled = false;
      btn.textContent = old;
      toast('网络异常', 'err');
    });
  }

  function existingModelIds() {
    var out = {};
    var rows = $('pv-models').querySelectorAll('.model-row');
    for (var i = 0; i < rows.length; i++) {
      var v = rows[i].querySelector('.w-id').value.trim();
      if (v) out[v] = true;
    }
    return out;
  }

  $('pv-fetch').addEventListener('click', function () { fetchModels(this); });

  $('pv-fetched-all').addEventListener('click', function () {
    var boxes = $('pv-fetched').querySelectorAll('input[type=checkbox]');
    var anyOff = false;
    for (var i = 0; i < boxes.length; i++) if (!boxes[i].checked) anyOff = true;
    for (var j = 0; j < boxes.length; j++) boxes[j].checked = anyOff;
  });

  $('pv-fetched-close').addEventListener('click', function () {
    $('pv-fetched-wrap').classList.add('hidden');
  });

  $('pv-fetched-add').addEventListener('click', function () {
    var boxes = $('pv-fetched').querySelectorAll('input[type=checkbox]');
    var mine = existingModelIds();
    var added = 0;
    for (var i = 0; i < boxes.length; i++) {
      if (!boxes[i].checked) continue;
      var id = boxes[i].value;
      if (!id || mine[id]) continue;
      mine[id] = true;
      $('pv-models').appendChild(modelRow({
        modelId: id,
        name: boxes[i].getAttribute('data-name') || id,
      }));
      added++;
    }
    toast(added ? ('已加入 ' + added + ' 个模型，记得点「保存」') : '没有新增（都已存在或未勾选）',
          added ? 'ok' : 'err');
  });

  // 厂商 ID 自动生成：名称里的字母数字直接拿来当 slug，中文等取不到就退化成 vendor-xxxx。
  function hashText(s) {
    var h = 5381;
    for (var i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) >>> 0;
    return h.toString(36);
  }

  function providerSlug(name) {
    var s = String(name || '').toLowerCase()
      .replace(/[^a-z0-9_\-]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 40)
      .replace(/^-+|-+$/g, '');
    return s || ('vendor-' + hashText(String(name || '') + Date.now()));
  }

  function nextProviderId(name) {
    var base = providerSlug(name);
    var taken = {};
    (state.providers || []).forEach(function (p) { taken[p.providerId] = true; });
    var pid = base, n = 2;
    while (taken[pid]) {
      var sfx = '-' + n;
      pid = base.slice(0, 40 - sfx.length) + sfx;
      n++;
    }
    return pid;
  }

  function setProviderIdHint(pid) {
    var el = $('pv-id-hint');
    if (!el) return;
    el.innerHTML = pid
      ? ('厂商 ID：<code>' + esc(pid) + '</code>（自动生成，不可修改）')
      : '厂商 ID 会自动生成，不用手填。';
  }

  function openProvider(pid) {
    state.editingProvider = pid;
    $('pv-title').textContent = pid ? '编辑厂商' : '新增第三方厂商';
    $('pv-delete').classList.toggle('hidden', !pid);
    $('pv-models').innerHTML = '';
    $('pv-key').type = 'password';
    // 每次开弹窗都收起上一次拉到的模型列表，避免串场
    $('pv-fetched').innerHTML = '';
    $('pv-fetched-note').textContent = '';
    $('pv-fetched-wrap').classList.add('hidden');

    if (!pid) {
      state.editingModels = [];
      setProviderIdHint(null);
      $('pv-name').value = '';
      $('pv-kind').value = 'openai-compatible';
      $('pv-base').value = '';
      $('pv-key').value = '';
      $('pv-models').appendChild(modelRow({}));
      openModal('provider-modal');
      return;
    }

    // 编辑：先拉一次带明文密钥的列表
    req('/api/settings/providers?reveal=1').then(function (r) {
      var p = (r.data.providers || []).filter(function (x) {
        return x.providerId === pid;
      })[0];
      if (!p) { toast('厂商不存在', 'err'); return; }
      setProviderIdHint(p.providerId);
      $('pv-name').value = p.name || '';
      $('pv-kind').value = p.kind || 'openai-compatible';
      $('pv-base').value = p.baseURL || '';
      $('pv-key').value = p.apiKey || '';
      state.editingModels = (p.models || []).map(function (m) {
        return m.modelId;
      }).filter(Boolean);
      (p.models || []).forEach(function (m) {
        $('pv-models').appendChild(modelRow(m));
      });
      if (!(p.models || []).length) $('pv-models').appendChild(modelRow({}));
      openModal('provider-modal');
    });
  }

  $('pv-key-eye').addEventListener('click', function () {
    var el = $('pv-key');
    el.type = el.type === 'password' ? 'text' : 'password';
  });

  $('pv-add-model').addEventListener('click', function () {
    $('pv-models').appendChild(modelRow({}));
  });

  $('pv-save').addEventListener('click', function () {
    var name = $('pv-name').value.trim();
    var base = $('pv-base').value.trim();
    if (!name) { toast('请填写显示名称', 'err'); return; }
    // 厂商 ID 由面板生成：编辑时沿用原 ID，新建时按显示名称推导一个没被占用的
    var pid = state.editingProvider || nextProviderId(name);
    if (!/^[A-Za-z0-9_\-]{1,40}$/.test(pid)) {
      toast('厂商 ID 生成异常，请改一下显示名称再试', 'err');
      return;
    }
    if (!base) { toast('请填写 Base URL', 'err'); return; }

    var models = [];
    var rows = $('pv-models').querySelectorAll('.model-row');
    for (var i = 0; i < rows.length; i++) {
      var id = rows[i].querySelector('.w-id').value.trim();
      if (!id) continue;
      var nums = rows[i].querySelectorAll('.w-num');
      models.push({
        modelId: id,
        name: rows[i].querySelector('.w-name').value.trim() || id,
        context: nums[0] ? nums[0].value : '',
        output: nums[1] ? nums[1].value : '',
      });
    }
    if (!models.length) { toast('至少填一个模型', 'err'); return; }

    // 后端是「合并式」写入：只有显式告诉它删哪些，模型才会真从配置里消失。
    // 原来的 ID 集合减去现在还留着的，就是被删掉 / 被改名的那些。
    var keep = {};
    models.forEach(function (m) { keep[m.modelId] = true; });
    var removeModels = (state.editingModels || []).filter(function (id) {
      return !keep[id];
    });

    var payload = {
      providerId: pid,
      name: name,
      kind: $('pv-kind').value,
      baseURL: base,
      models: models,
      removeModels: removeModels,
    };
    var key = $('pv-key').value;
    if (key) payload.apiKey = key;

    var btn = this;
    btn.disabled = true;
    btn.textContent = '保存中…';
    req('/api/settings/providers', { method: 'POST', body: JSON.stringify(payload) })
      .then(function (r) {
        btn.disabled = false;
        btn.textContent = '保存';
        if (!r.data.ok) { toast(r.data.error || '保存失败', 'err'); return; }
        toast('已保存，正在重载 zcode 配置…', 'ok');
        closeModal('provider-modal');
        setTimeout(function () {
          loadProviders();
          loadOptions(true);
        }, 1200);
      }).catch(function () {
        btn.disabled = false;
        btn.textContent = '保存';
        toast('网络异常', 'err');
      });
  });

  $('pv-delete').addEventListener('click', function () {
    var pid = state.editingProvider;
    if (!pid) return;
    confirmBox('删除厂商', '确定要删除厂商「' + pid + '」吗？它下面的模型也会一起移除。', '删除')
      .then(function (ok) {
        if (!ok) return;
        req('/api/settings/providers/' + encodeURIComponent(pid), { method: 'DELETE' })
          .then(function (r) {
            if (!r.data.ok) { toast(r.data.error || '删除失败', 'err'); return; }
            toast('已删除', 'ok');
            closeModal('provider-modal');
            setTimeout(function () {
              loadProviders();
              loadOptions(true);
            }, 1200);
          }).catch(function () { toast('网络异常', 'err'); });
      });
  });

  /* ============================================ 模型定时 */

  var SCHED_KIND_HINT = {
    once: '只在指定日期的这个时间点执行一次。',
    daily: '每天到这个时间点切换一次。',
    weekly: '只在选中的星期几到这个时间点切换。',
  };
  var DAY_LABELS = ['日', '一', '二', '三', '四', '五', '六'];

  function fmtDT(ms) {
    if (!ms) return '—';
    var d = new Date(ms);
    return (d.getMonth() + 1) + '-' + pad2(d.getDate()) + ' ' +
      pad2(d.getHours()) + ':' + pad2(d.getMinutes());
  }

  function fmtDTFull(ms) {
    if (!ms) return '—';
    var d = new Date(ms);
    return d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate()) + ' ' +
      pad2(d.getHours()) + ':' + pad2(d.getMinutes());
  }

  function schedDurText(min) {
    min = Number(min) || 0;
    if (!min) return '持续生效';
    if (min % 1440 === 0) return '持续 ' + (min / 1440) + ' 天';
    if (min % 60 === 0) return '持续 ' + (min / 60) + ' 小时';
    return '持续 ' + min + ' 分钟';
  }

  // 「生效前模型」怎么描述：优先说清「逐会话各回各的」，
  // 没有快照时才退回全局默认模型。
  function schedPrevText(rt) {
    rt = rt || {};
    var n = Number(rt.prevSessions) || 0;
    if (n > 0) return '逐会话各回原模型（' + n + ' 个会话有快照）';
    var pm = (rt.prev && rt.prev.model) || null;
    if (pm && pm.providerId) return pm.providerId + '/' + pm.modelId;
    var g = rt.prevGlobal || {};
    if (g.main) return g.main + '（全局默认）';
    return '未知';
  }

  function schedEndText(startMs, min) {
    if (!startMs || !min) return '';
    var d = new Date(startMs + min * 60000);
    return pad2(d.getHours()) + ':' + pad2(d.getMinutes()) +
      '（' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate()) + '）';
  }

  function loadSchedules(force) {
    return req('/api/schedules' + (force ? '?_=' + Date.now() : '')).then(function (r) {
      if (r.status === 401) { showLogin('登录已过期，请重新输入密钥'); return; }
      if (!r.data.ok) return;
      state.schedules = r.data.schedules || [];
      state.scheduleRuntime = r.data.runtime || { active: null };
      renderSchedules();
      renderSchedBanner();
      renderModelSelects();
      return r.data;
    }).catch(function () {});
  }

  function renderSchedules() {
    var rt = state.scheduleRuntime || {};
    var box = $('sched-active');
    if (rt.active) {
      var a = rt.active;
      var untilTxt = a.until ? ('到 ' + fmtDTFull(a.until)) : '持续生效（直到恢复或下一条规则）';
      var prevTxt = schedPrevText(rt);
      box.classList.remove('hidden');
      box.innerHTML =
        '<div class="sched-active-head">' +
        '<span class="sched-dot on"></span>' +
        '<b>生效中：' + esc(a.name || '定时规则') + '</b>' +
        '<span class="sched-tag">' + (a.manual ? '手动' : '定时') + '</span>' +
        '</div>' +
        '<div class="sched-active-body">所有会话当前使用 <b>' +
        esc(a.modelLabel || (a.model ? a.model.modelId : '—')) + '</b>' +
        '（' + esc(a.model ? a.model.providerId : '') + '/' + esc(a.model ? a.model.modelId : '') + '）' +
        '，' + esc(untilTxt) + '。' +
        '<br>生效前模型：<code>' + esc(prevTxt) + '</code>' +
        '　开始于 ' + fmtDTFull(a.since) + '</div>' +
        '<button class="ghost-btn" id="sched-revert">恢复原模型</button>';
      var rb = $('sched-revert');
      if (rb) rb.addEventListener('click', revertSched);
    } else {
      box.classList.add('hidden');
      box.innerHTML = '';
    }

    var list = $('sched-list');
    if (!state.schedules.length) {
      list.innerHTML = '<div class="usage-empty">还没有定时规则。点右上角「新建规则」添加一条。</div>';
    } else {
      list.innerHTML = state.schedules.map(function (s) {
        var on = !!s.enabled;
        var dur = Number(s.durationMinutes) || 0;
        var last = '';
        if (s.lastRunAt && s.lastResult) {
          last = '上次 ' + fmtDT(s.lastRunAt) + '：命中 ' + (s.lastResult.total || 0) +
            ' 个会话，成功 ' + (s.lastResult.switched || 0) +
            (s.lastResult.interrupted ? '，中断 ' + s.lastResult.interrupted + ' 个并续跑 ' +
              (s.lastResult.resumed || 0) + ' 个' : '') +
            (s.lastResult.external ? '，含外部会话 ' + s.lastResult.external + ' 个' : '') +
            (s.lastResult.pending ? '，' + s.lastResult.pending + ' 个待空闲续跑' : '') +
            (s.lastResult.inactive ? '，' + s.lastResult.inactive + ' 个已闲置' : '');
        }
        return '<div class="sched-row' + (on ? '' : ' off') + (s.active ? ' running' : '') +
          '" data-id="' + esc(s.id) + '">' +
          '<div class="sched-row-head">' +
          '<span class="sched-dot' + (s.active ? ' on' : '') + '"></span>' +
          '<b class="sched-name">' + esc(s.name) + '</b>' +
          '<span class="sched-tag">' + esc(s.kindLabel) + '</span>' +
          (s.active ? '<span class="sched-tag live">生效中</span>' : '') +
          (on ? '' : '<span class="sched-tag off">已停用</span>') +
          '</div>' +
          '<div class="sched-row-body">' +
          '<span class="sched-when">' + esc(s.when) + '</span>' +
          '<span class="sched-arrow">→</span>' +
          '<b class="sched-model">' + esc(s.modelLabel) + '</b>' +
          '<span class="sched-dur">' + esc(schedDurText(dur)) + '</span>' +
          '</div>' +
          '<div class="sched-row-sub">' +
          (on ? '下次 ' + fmtDT(s.nextRunAt) : '已停用') +
          (last ? '　·　' + esc(last) : '') +
          (s.resumeRunning ? '　·　自动续跑' : '　·　不续跑') +
          '</div>' +
          '<div class="sched-row-ops">' +
          '<button class="ghost-btn xs" data-act="run">立即执行</button>' +
          '<button class="ghost-btn xs" data-act="toggle">' + (on ? '停用' : '启用') + '</button>' +
          '<button class="ghost-btn xs" data-act="edit">编辑</button>' +
          '<button class="text-btn xs danger" data-act="del">删除</button>' +
          '</div>' +
          '</div>';
      }).join('');
    }

    var enabled = state.schedules.filter(function (s) { return s.enabled; }).length;
    $('sched-note').textContent = '共 ' + state.schedules.length + ' 条规则，启用 ' + enabled +
      ' 条。调度器每 ' + ((state.scheduleRuntime.tickSeconds) || 10) + ' 秒核对一次；' +
      '规则改动、服务重启都会自动重新对齐，不会漏执行。';
  }

  function refreshSchedViews() {
    loadSchedules(true);
    loadOptions(true);
  }

  function renderSchedBanner() {
    var bar = $('sched-banner');
    if (!bar) return;
    var rt = state.scheduleRuntime || {};
    var a = rt.active;
    if (!a) { bar.classList.add('hidden'); return; }
    var untilTxt = a.until ? ('，到 ' + fmtDT(a.until) + ' 结束') : '';
    // 有会话因为长任务没停下来时会登记成「待续跑」，把这件事说出来，
    // 免得用户看着一个已经结束的轮次以为定时没生效。
    var pendTxt = rt.pending ? ('　<span class="sched-pend">' + rt.pending +
      ' 个会话等空闲后自动续跑</span>') : '';
    $('sched-banner-text').innerHTML =
      '模型定时生效中：所有会话使用 <b>' + esc(a.modelLabel || '') + '</b>' +
      esc(untilTxt) + '　<code>' + esc(a.model ? a.model.providerId : '') + '</code>' + pendTxt;
    bar.classList.remove('hidden');
  }

  // ---- 规则弹窗 ----
  function schedKind() {
    var b = document.querySelector('#sm-kind .seg-btn.active');
    return b ? b.dataset.kind : 'daily';
  }

  function setSchedKind(kind) {
    document.querySelectorAll('#sm-kind .seg-btn').forEach(function (b) {
      b.classList.toggle('active', b.dataset.kind === kind);
    });
    $('sm-date-row').classList.toggle('hidden', kind !== 'once');
    $('sm-days-row').classList.toggle('hidden', kind !== 'weekly');
    $('sm-kind-hint').textContent = SCHED_KIND_HINT[kind] || '';
    updateSchedForm();
  }

  function fillSchedModels(selectedPid, selectedMid) {
    var models = (state.options && state.options.models) || [];
    var sel = $('sm-model');
    if (!models.length) {
      sel.innerHTML = '<option value="">（模型目录还没加载好）</option>';
      return;
    }
    sel.innerHTML = models.map(function (m) {
      var v = (m.providerId || '') + '|' + (m.modelId || '');
      var on = m.providerId === selectedPid && m.modelId === selectedMid;
      return '<option value="' + esc(v) + '"' + (on ? ' selected' : '') + '>' +
        esc(m.label || m.modelId) + (m.providerLabel ? ' · ' + esc(m.providerLabel) : '') +
        '</option>';
    }).join('');
  }

  function updateSchedForm() {
    var kind = schedKind();
    var dur = Number($('sm-duration').value) || 0;
    var hh = $('sm-at').value || '09:00';
    $('sm-duration-hint').textContent = dur > 0
      ? ('到点切换，' + schedDurText(dur) + '后自动切回原模型。')
      : '0 = 一直生效，直到你点「恢复原模型」或下一条规则到点接管。';

    var when;
    if (kind === 'once') {
      when = ($('sm-date').value || '（未选日期）') + ' ' + hh;
    } else if (kind === 'weekly') {
      var days = [];
      document.querySelectorAll('#sm-days input:checked').forEach(function (c) {
        days.push(DAY_LABELS[Number(c.value)]);
      });
      when = '每周' + (days.length ? days.join('、') : '（未选星期）') + ' ' + hh;
    } else {
      when = '每天 ' + hh;
    }
    var pid = ($('sm-model').value || '').split('|');
    var label = $('sm-model').selectedIndex >= 0
      ? $('sm-model').options[$('sm-model').selectedIndex].text : '—';
    var txt = when + ' 切换到 ' + label;
    if (dur > 0) txt += '，' + schedDurText(dur) + '后切回';
    $('sm-preview').textContent = '预览：' + txt +
      ($('sm-resume').checked ? '；被打断的会话会收到「' +
        ($('sm-prompt').value || '请继续执行任务。') + '」' : '；不打断正在跑的会话');
  }

  function openSchedModal(rule) {
    state.schedEditing = rule || null;
    var isEdit = !!rule;
    $('sm-title').textContent = isEdit ? '编辑定时规则' : '新建定时规则';
    $('sm-delete').classList.toggle('hidden', !isEdit);
    $('sm-run').classList.toggle('hidden', !isEdit);
    $('sm-save').textContent = isEdit ? '保存' : '创建';

    var d = new Date(Date.now() + 3600000);
    $('sm-name').value = isEdit ? (rule.name || '') : '';
    $('sm-at').value = isEdit ? (rule.at || '09:00') : '09:00';
    $('sm-date').value = isEdit && rule.date
      ? rule.date
      : (d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate()));
    $('sm-duration').value = isEdit ? (rule.durationMinutes || 0) : 0;
    $('sm-resume').checked = isEdit ? !!rule.resumeRunning : true;
    $('sm-prompt').value = isEdit ? (rule.resumePrompt || '') : '';
    $('sm-enabled').checked = isEdit ? !!rule.enabled : true;

    var days = (isEdit && rule.days) || [];
    document.querySelectorAll('#sm-days input').forEach(function (c) {
      c.checked = days.indexOf(Number(c.value)) >= 0;
    });

    var cur = (state.options && state.options.currentModel) || {};
    fillSchedModels(isEdit ? rule.providerId : cur.providerId,
                    isEdit ? rule.modelId : cur.modelId);
    setSchedKind(isEdit ? (rule.kind || 'daily') : 'daily');
    openModal('sched-modal');
  }

  function schedFormPayload() {
    var mv = ($('sm-model').value || '').split('|');
    var days = [];
    document.querySelectorAll('#sm-days input:checked').forEach(function (c) {
      days.push(Number(c.value));
    });
    return {
      id: state.schedEditing ? state.schedEditing.id : '',
      name: $('sm-name').value.trim(),
      kind: schedKind(),
      at: $('sm-at').value,
      date: $('sm-date').value,
      days: days,
      durationMinutes: Number($('sm-duration').value) || 0,
      providerId: mv[0] || '',
      modelId: mv.slice(1).join('|') || '',
      resumeRunning: $('sm-resume').checked,
      resumePrompt: $('sm-prompt').value.trim(),
      enabled: $('sm-enabled').checked,
    };
  }

  function saveSchedule() {
    var payload = schedFormPayload();
    if (!payload.providerId || !payload.modelId) {
      toast('请选择要切换到的模型', 'err');
      return;
    }
    $('sm-save').disabled = true;
    req('/api/schedules', { method: 'POST', body: JSON.stringify(payload) }).then(function (r) {
      $('sm-save').disabled = false;
      if (r.status === 401) { showLogin('登录已过期，请重新输入密钥'); return; }
      if (!r.data.ok) { toast(r.data.error || '保存失败', 'err'); return; }
      closeModal('sched-modal');
      toast('规则已保存');
      loadSchedules(true);
    }).catch(function () { $('sm-save').disabled = false; });
  }

  function deleteSchedule(id, name) {
    confirmBox('删除定时规则', '确定要删除「' + name + '」吗？\n如果它正在生效，会立刻把模型切回原来的。', '删除')
      .then(function (ok) {
        if (!ok) return;
        req('/api/schedules/' + encodeURIComponent(id), { method: 'DELETE' }).then(function (r) {
          if (!r.data.ok) { toast(r.data.error || '删除失败', 'err'); return; }
          closeModal('sched-modal');
          toast(r.data.wasActive ? '规则已删除，模型已恢复' : '规则已删除');
          loadSchedules(true);
          loadOptions(true);
        });
      });
  }

  function runScheduleNow(id) {
    req('/api/schedules/' + encodeURIComponent(id) + '/run', { method: 'POST' }).then(function (r) {
      if (!r.data.ok) { toast(r.data.error || '执行失败', 'err'); return; }
      var rep = r.data.report || {};
      var msg = '已切换：命中 ' + (rep.total || 0) + ' 个会话，成功 ' + (rep.switched || 0) +
            (rep.interrupted ? '，中断并续跑 ' + (rep.resumed || 0) + ' 个' : '') +
            (rep.external ? '，含外部会话 ' + rep.external + ' 个' : '') +
            (rep.pending ? '，' + rep.pending + ' 个待空闲续跑' : '');
      // 手动执行同样遵守规则时长：到点会自动切回，把这件事说清楚
      if (r.data.expiresAt) msg += '；' + fmtDT(r.data.expiresAt) + ' 自动恢复';
      toast(msg, rep.failed ? 'err' : 'ok');
      loadSchedules(true);
      loadOptions(true);
    });
  }

  function revertSched() {
    confirmBox('恢复原模型', '把每个会话切回它各自在定时生效前用的模型？\n正在跑的会话会按规则设置决定是否打断后继续。', '恢复')
      .then(function (ok) {
        if (!ok) return;
        req('/api/schedules/revert', { method: 'POST' }).then(function (r) {
          if (!r.data.ok) { toast(r.data.error || '恢复失败', 'err'); return; }
          var m = r.data.model || {};
          toast('已恢复为 ' + (m.providerId ? m.providerId + '/' + m.modelId : '默认模型'));
          loadSchedules(true);
          loadOptions(true);
        });
      });
  }

  function toggleSchedule(id, enabled) {
    var s = state.schedules.filter(function (x) { return x.id === id; })[0];
    if (!s) return;
    req('/api/schedules', {
      method: 'POST',
      body: JSON.stringify({ id: id, enabled: enabled }),
    }).then(function (r) {
      if (!r.data.ok) { toast(r.data.error || '操作失败', 'err'); return; }
      toast(enabled ? '规则已启用' : '规则已停用');
      loadSchedules(true);
    });
  }

  $('sched-add').addEventListener('click', function () { openSchedModal(null); });

  document.querySelectorAll('#sm-kind .seg-btn').forEach(function (b) {
    b.addEventListener('click', function () { setSchedKind(b.dataset.kind); });
  });
  ['sm-at', 'sm-date', 'sm-duration', 'sm-model', 'sm-resume', 'sm-prompt'].forEach(function (id) {
    var el = $(id);
    if (el) el.addEventListener('input', updateSchedForm);
    if (el) el.addEventListener('change', updateSchedForm);
  });
  $('sm-days').addEventListener('change', updateSchedForm);
  $('sm-save').addEventListener('click', saveSchedule);
  $('sm-run').addEventListener('click', function () {
    if (state.schedEditing) runScheduleNow(state.schedEditing.id);
  });
  $('sm-delete').addEventListener('click', function () {
    if (state.schedEditing) deleteSchedule(state.schedEditing.id, state.schedEditing.name);
  });

  $('sched-list').addEventListener('click', function (e) {
    var btn = e.target.closest ? e.target.closest('button[data-act]') : null;
    if (!btn) return;
    var row = btn.closest('.sched-row');
    var id = row && row.dataset.id;
    var rule = state.schedules.filter(function (x) { return x.id === id; })[0];
    if (!rule) return;
    var act = btn.dataset.act;
    if (act === 'edit') openSchedModal(rule);
    else if (act === 'del') deleteSchedule(rule.id, rule.name);
    else if (act === 'run') runScheduleNow(rule.id);
    else if (act === 'toggle') toggleSchedule(rule.id, !rule.enabled);
  });

  $('sched-banner-revert').addEventListener('click', revertSched);
  $('sched-banner-detail').addEventListener('click', function () {
    openModal('settings-modal');
    showSettingsPane('schedule');
  });

  /* ============================================ 使用统计 */

  var USAGE_RANGE_LABEL = { today: '今日', '7d': '近 7 天', '30d': '近 30 天', all: '全部' };

  function loadUsage(range, force) {
    if (range) state.usageRange = range;
    var url = '/api/usage?range=' + encodeURIComponent(state.usageRange) +
      (force ? '&fresh=1&_=' + Date.now() : '');
    return req(url).then(function (r) {
      if (!r.data.ok) {
        if (force) toast(r.data.error || '读取用量失败', 'err');
        return null;
      }
      state.usage = r.data;
      renderUsageChip(r.data);
      renderUsagePane(r.data);
      return r.data;
    }).catch(function () { return null; });
  }

  function renderUsageChip(d) {
    var t = d.today || {};
    var val = $('usage-chip-value');
    if (val) val.textContent = fmtTokens(t.tokens || 0);
    var chip = $('usage-chip');
    if (chip) {
      chip.title = '今日 token 用量 ' + fmtNum(t.tokens || 0) +
        '\n输入 ' + fmtNum(t.inputTokens || 0) +
        ' · 输出 ' + fmtNum(t.outputTokens || 0) +
        (t.reasoningTokens ? ' · 思考 ' + fmtNum(t.reasoningTokens) : '') +
        '\n模型请求 ' + (t.requests || 0) + ' 次 · 涉及 ' + (t.sessions || 0) + ' 个会话' +
        '\n点击查看使用统计';
    }
  }

  function renderUsagePane(d) {
    var sum = d.summary || {};
    var label = USAGE_RANGE_LABEL[d.range] || d.range;

    var cards = [
      { k: '总 token', v: fmtTokens(sum.tokens), sub: fmtNum(sum.tokens) },
      { k: '输入', v: fmtTokens(sum.inputTokens), sub: fmtNum(sum.inputTokens) },
      { k: '输出', v: fmtTokens(sum.outputTokens), sub: fmtNum(sum.outputTokens) },
      { k: '思考', v: fmtTokens(sum.reasoningTokens), sub: fmtNum(sum.reasoningTokens) },
      { k: '缓存读取', v: fmtTokens(sum.cacheReadTokens), sub: fmtNum(sum.cacheReadTokens) },
      { k: '模型请求', v: fmtNum(sum.requests) + ' 次', sub: '平均 ' + fmtDuration(sum.avgDurationMs) },
      { k: '会话数', v: fmtNum(sum.sessions) + ' 个', sub: '工具调用 ' + fmtNum(sum.toolExecutions || 0) + ' 次' },
      { k: '失败/取消', v: fmtNum(sum.errors) + ' 次', sub: '总计耗时 ' + fmtDuration(sum.durationMs) },
    ];
    $('usage-cards').innerHTML = cards.map(function (c) {
      return '<div class="usage-card"><span class="usage-card-k">' + esc(c.k) + '</span>' +
        '<b class="usage-card-v">' + esc(c.v) + '</b>' +
        '<span class="usage-card-sub">' + esc(c.sub) + '</span></div>';
    }).join('');

    renderUsageChart(d.series || []);
    renderUsageModels(d.byModel || []);

    var last = sum.lastAt ? new Date(sum.lastAt).toLocaleString() : '—';
    var first = sum.firstAt ? new Date(sum.firstAt).toLocaleString() : '—';
    $('usage-note').textContent = '统计区间：' + label +
      '　累计（全部时间）：' + fmtTokens((d.all || {}).tokens) +
      '　首次记录：' + first + '　最近一次：' + last;
  }

  function renderUsageChart(series) {
    var box = $('usage-chart');
    if (!series.length) { box.innerHTML = '<div class="usage-empty">暂无数据</div>'; return; }
    var max = 0;
    series.forEach(function (p) { if (p.tokens > max) max = p.tokens; });
    var today = new Date();
    var todayKey = today.getFullYear() + '-' + pad2(today.getMonth() + 1) + '-' + pad2(today.getDate());
    var bars = series.map(function (p) {
      var h = max > 0 ? Math.max(2, Math.round(p.tokens / max * 100)) : 2;
      var dim = todayKey === p.date;
      return '<div class="usage-bar' + (dim ? ' today' : '') + '" title="' +
        esc(p.date) + '　' + fmtNum(p.tokens) + ' tokens（输入 ' + fmtNum(p.input) +
        ' / 输出 ' + fmtNum(p.output) + '，' + p.requests + ' 次请求）">' +
        '<span class="usage-bar-fill" style="height:' + h + '%"></span></div>';
    }).join('');
    var labels = series.map(function (p, i) {
      // 只标首、尾和每隔一天，免得挤在一起
      var show = i === series.length - 1 || i === 0 || i % 3 === 0;
      return '<span>' + (show ? esc(p.date.slice(5)) : '') + '</span>';
    }).join('');
    box.innerHTML = '<div class="usage-bars">' + bars + '</div>' +
      '<div class="usage-bar-labels">' + labels + '</div>' +
      '<div class="usage-bar-max">峰值 ' + fmtTokens(max) + ' tokens</div>';
  }

  function renderUsageModels(list) {
    var box = $('usage-models');
    if (!list.length) { box.innerHTML = '<div class="usage-empty">这个区间还没有调用记录</div>'; return; }
    var rows = list.map(function (m) {
      return '<tr><td><code>' + esc(m.providerId) + '</code></td>' +
        '<td>' + esc(m.modelId) + '</td>' +
        '<td class="num"><b>' + fmtTokens(m.tokens) + '</b></td>' +
        '<td class="num">' + fmtTokens(m.inputTokens) + '</td>' +
        '<td class="num">' + fmtTokens(m.outputTokens) + '</td>' +
        '<td class="num">' + (m.requests || 0) + '</td>' +
        '<td class="num">' + (m.sessions || 0) + '</td></tr>';
    }).join('');
    box.innerHTML = '<table class="usage-tbl"><thead><tr>' +
      '<th>厂商</th><th>模型</th><th class="num">总 token</th><th class="num">输入</th>' +
      '<th class="num">输出</th><th class="num">请求</th><th class="num">会话</th>' +
      '</tr></thead><tbody>' + rows + '</tbody></table>';
  }

  $('usage-seg').addEventListener('click', function (e) {
    var b = e.target.closest ? e.target.closest('.seg-btn') : null;
    if (!b) return;
    var btns = this.querySelectorAll('.seg-btn');
    for (var i = 0; i < btns.length; i++) btns[i].classList.toggle('active', btns[i] === b);
    loadUsage(b.dataset.range, true);
  });

  $('usage-refresh').addEventListener('click', function () { loadUsage(null, true); });

  $('usage-chip').addEventListener('click', function () {
    openModal('settings-modal');
    showSettingsPane('usage');
    loadSecurity();
    loadProviders();
    loadUsage(state.usageRange, true);
  });

  /* ============================================ 侧栏 / 顶栏交互 */

  $('new-chat-btn').addEventListener('click', openNewChat);
  $('refresh-btn').addEventListener('click', function () { loadSessions(false); });

  // ---- 移动端抽屉：≤720px 时侧栏是浮层，遮罩用来点空白处收起 ----
  var MQ_MOBILE = window.matchMedia ? window.matchMedia('(max-width: 720px)') : null;

  function isMobileLayout() { return MQ_MOBILE ? MQ_MOBILE.matches : window.innerWidth <= 720; }

  function setSidebarOpen(open) {
    $('sidebar').classList.toggle('collapsed', !open);
    $('expand-btn').classList.toggle('hidden', open);
    var mask = $('sidebar-mask');
    if (mask) mask.classList.toggle('hidden', !(open && isMobileLayout()));
  }

  // 软键盘弹出时（iOS 不会缩放布局视口）把 visualViewport 的实际高度写进
  // --vvh，让 .app-view 跟着收缩，输入框始终留在键盘上方而不是被盖住。
  if (window.visualViewport) {
    var vvSync = function () {
      document.documentElement.style.setProperty('--vvh',
        window.visualViewport.height + 'px');
    };
    window.visualViewport.addEventListener('resize', vvSync);
    window.visualViewport.addEventListener('scroll', vvSync);
    vvSync();
  }

  // 窗口跨过 720px 断点（旋转屏幕 / 拉伸窗口）时同步两侧的布局状态：
  // 到窄屏收起抽屉，回宽屏恢复常驻侧栏并清掉遮罩和展开按钮。
  function bindMobileQuery(mq) {
    if (!mq) return;
    var onChange = function (e) {
      if (e.matches) setSidebarOpen(false);
      else setSidebarOpen(true);
    };
    if (mq.addEventListener) mq.addEventListener('change', onChange);
    else if (mq.addListener) mq.addListener(onChange);
  }
  bindMobileQuery(MQ_MOBILE);

  $('collapse-btn').addEventListener('click', function () { setSidebarOpen(false); });

  $('expand-btn').addEventListener('click', function () { setSidebarOpen(true); });

  var sidebarMask = $('sidebar-mask');
  if (sidebarMask) {
    sidebarMask.addEventListener('click', function () { setSidebarOpen(false); });
  }

  $('set-logout-btn').addEventListener('click', function () {
    req('/api/logout', { method: 'POST' }).then(function () {
      state.wsManualClose = true;
      state.wsEnabled = false;
      stopWsKeepalive();
      clearTimeout(state.wsResumeTimer);
      state.wsResumeTimer = null;
      if (state.wsTimer) { clearTimeout(state.wsTimer); state.wsTimer = null; }
      if (state.ws) { try { state.ws.close(); } catch (e) {} state.ws = null; }
      state.sessionId = null;
      clearMessages();
      // 退出登录时把还开着的弹窗一并关掉，否则会浮在登录页上面
      closeModal('settings-modal');
      closeModal('newchat-modal');
      closeModal('ws-modal');
      showLogin();
      $('key-input').value = '';
      $('key-input').focus();
    });
  });

  /* ============================================ 启动 */

  // 滚到接近顶部时自动加载更早的历史消息（配合顶部的手动点击入口）
  $('messages').addEventListener('scroll', function () {
    var p = state.msgPage;
    if (!p.hasMore || p.loading) return;
    var box = $('messages');
    if (box.scrollTop <= 60) loadOlderMessages();
  });

  function applyMe(d) {
    if (!d) return;
    if (d.ip) $('foot-info').textContent = d.ip;
    if (d.modes && d.modes.length) state.modes = d.modes;
    renderModeSelects();
    if (d.defaultMode) {
      $('mode-select').value = d.defaultMode;
      $('set-default-mode').value = d.defaultMode;
      updateModeDesc();
    }
  }

  function syncMe() {
    return req('/api/me').then(function (r) {
      if (r.data && r.data.ok) applyMe(r.data);
      return r.data;
    });
  }

  req('/api/me').then(function (r) {
    if (r.data && r.data.ok) {
      applyMe(r.data);
      showApp();
    } else if (r.data && r.data.banned) {
      showLogin(r.data.error || '此 IP 已被永久封禁');
      $('login-btn').disabled = true;
      $('login-btn').textContent = '已封禁';
    } else {
      $('key-input').focus();
    }
  }).catch(function () {
    $('key-input').focus();
  });

})();
