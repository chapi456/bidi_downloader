/*
  File: app.js
  Path: bidi/templates/static/app.js

  Version: 1.0.0
  Date: 2026-04-23

  Changelog:
  - 1.0.0 (2026-04-23): Extraction depuis index.html inline <script>
*/
'use strict';

// ── État ─────────────────────────────────────────────────────────────────────
const STATE = {
  filter: '',
  platform: '',
  offset: 0,
  search: '',
  sort: 'newest',
  loading: false,
  sidebarOpen: true,
  currentEmail: null,
  currentMediaIdx: 0,
  searchTimer: null,
};

const PAGE_SIZE = 48;
const STEPS_DB  = ['new','parsed','meta_done','download_sent','download_done','thumb_done','llm_done','done'];
const STEPS_CLI = ['fetch','parse','meta','send','check','thumb','llm'];

// ── Helpers ──────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);

function esc(s){
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function fmtSize(b){
  if(!b) return '';
  if(b>1e9) return (b/1e9).toFixed(1)+' GB';
  if(b>1e6) return (b/1e6).toFixed(1)+' MB';
  if(b>1e3) return Math.round(b/1e3)+' KB';
  return b+' B';
}

function platIcon(p=''){
  const m={youtube:'▶',reddit:'⬤',twitter:'✖','x.com':'✖',redgifs:'⏵',pornhub:'◉',xvideos:'▷',xhamster:'◈'};
  return m[p.toLowerCase()] || '◉';
}

function starsHtml(rating, emailId){
  return Array.from({length:5},(_,i)=>
    `<button class="star-edit ${(i+1)<=(rating||0)?'on':''}"
       onclick="rate(${emailId},${i+1})"
       aria-label="${i+1} étoile${i>0?'s':''}"
       data-idx="${i+1}">★</button>`
  ).join('');
}

function badgeHtml(status){
  const m={ok:'badge-ok',failed:'badge-failed',running:'badge-running'};
  const lbl={ok:'✓ ok',failed:'✗ failed',running:'… running'};
  return `<span class="badge ${m[status]||'badge-other'}">${esc(lbl[status]||status)}</span>`;
}

// ── Logs ─────────────────────────────────────────────────────────────────────
const LOG_MAX = 60;

function log(msg, cls='l-info'){
  const t = new Date().toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
  ['logbar','pipeline-log'].forEach(id => {
    const el = $(id); if(!el) return;
    const line = document.createElement('div');
    line.className = `log-line ${cls}`;
    line.textContent = `${t}  ${msg}`;
    el.appendChild(line);
    while(el.children.length > LOG_MAX) el.firstChild.remove();
    el.scrollTop = el.scrollHeight;
  });
}

// ── Vues ─────────────────────────────────────────────────────────────────────
function setView(v){
  $('view-gallery').classList.toggle('hidden', v !== 'gallery');
  $('view-pipeline').classList.toggle('active', v === 'pipeline');
  $('tab-gallery').classList.toggle('active', v === 'gallery');
  $('tab-pipeline').classList.toggle('active', v === 'pipeline');
  if(v === 'pipeline') loadStats();
}

function toggleSidebar(){
  STATE.sidebarOpen = !STATE.sidebarOpen;
  $('sidebar').classList.toggle('collapsed', !STATE.sidebarOpen);
  $('btn-sidebar').classList.toggle('active', STATE.sidebarOpen);
}

// ── API ───────────────────────────────────────────────────────────────────────
async function api(path, opts={}){
  try{
    const r = await fetch(path, opts);
    if(!r.ok){ log(`HTTP ${r.status} ${path}`, 'l-err'); return null; }
    return await r.json();
  } catch(e){
    log(`Serveur inaccessible (${path})`, 'l-err');
    return null;
  }
}

// ── Stats ─────────────────────────────────────────────────────────────────────
async function loadStats(){
  const d = await api('/api/stats');
  if(!d?.ok) return;
  const s = d.data || {};
  $('st-emails').textContent = s.total_emails ?? '–';
  $('st-files').textContent  = s.total_media_files ?? '–';
  $('st-tasks').textContent  = s.pending_download_tasks ?? '–';

  const steps = s.steps || {};
  let total = 0;
  STEPS_DB.forEach(step => {
    const counts = steps[step] || {};
    const n = Object.values(counts).reduce((a,b)=>a+b,0);
    const el = $('n-'+step); if(el) el.textContent = n || '–';
    total += n;
  });
  const allEl = $('n-all');
  if(allEl) allEl.textContent = s.total_emails || total || '–';

  renderPipelineViz(s);
}

function renderPipelineViz(s){
  const wrap = $('pipeline-viz'); if(!wrap) return;
  const steps = s.steps || {};
  const total = Math.max(s.total_emails || 1, 1);
  wrap.innerHTML = STEPS_DB.map(step => {
    const counts = steps[step] || {};
    const n   = Object.values(counts).reduce((a,b)=>a+b,0);
    const pct = Math.round((n/total)*100);
    const hasFail = (counts.failed||0) > 0;
    return `<div class="ps-row">
      <span class="ps-name">${esc(step)}</span>
      <div class="ps-bar"><div class="ps-fill ${hasFail?'fail':''}" style="width:${pct}%"></div></div>
      <span class="ps-count">${n}</span>
      <span class="ps-warn">${hasFail?'⚠':''}</span>
    </div>`;
  }).join('');
}

// ── Galerie ───────────────────────────────────────────────────────────────────
function buildCard(e){
  const files   = e.media_files || [];
  const thumb   = files.find(f => f.file_type === 'thumbnail')
               || files.find(f => f.file_type === 'image') || null;
  const videos  = files.filter(f => f.file_type === 'video');
  const images  = files.filter(f => f.file_type === 'image');
  const mediaF  = files.filter(f => f.file_type !== 'thumbnail');
  const nMedia  = mediaF.length;

  const isFailed  = e.step_status === 'failed';
  const isRunning = e.step_status === 'running';
  const isPending = !e.step || e.step === 'new';

  let thumbContent = '';
  if(thumb?.url){
    thumbContent = `<img src="${esc(thumb.url)}" alt="" loading="lazy" decoding="async"
      onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">
      <div class="no-thumb" style="display:none">
        <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.2" aria-hidden="true">
          <rect x="3" y="3" width="18" height="18" rx="2"/>
          <circle cx="8.5" cy="8.5" r="1.5"/>
          <polyline points="21 15 16 10 5 21"/>
        </svg>
      </div>`;
  } else {
    thumbContent = `<div class="no-thumb">
      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.2" aria-hidden="true">
        <rect x="3" y="3" width="18" height="18" rx="2"/>
        <circle cx="8.5" cy="8.5" r="1.5"/>
        <polyline points="21 15 16 10 5 21"/>
      </svg>
    </div>`;
  }

  let typeBadge = '';
  if(videos.length > 0)
    typeBadge = `<div class="thumb-badge-type">▶ ${videos.length > 1 ? videos.length+' vidéos' : 'vidéo'}</div>`;
  else if(images.length > 0)
    typeBadge = `<div class="thumb-badge-type">🖼 ${images.length > 1 ? images.length+' images' : 'image'}</div>`;

  let countBadge = '';
  let dotsHtml   = '';
  if(nMedia > 1){
    countBadge = `<div class="thumb-badge-count">${nMedia}</div>`;
    dotsHtml = `<div class="thumb-dots">${
      Array.from({length:Math.min(nMedia,5)},(_,i)=>
        `<div class="thumb-dot ${i===0?'on':''}"></div>`).join('')
    }</div>`;
  }

  let overlay = '';
  if(isFailed)       overlay = `<div class="thumb-overlay"><span class="thumb-overlay-icon">⚠</span></div>`;
  else if(isRunning) overlay = `<div class="thumb-overlay"><span class="thumb-overlay-icon" style="font-size:1rem">⏳</span></div>`;
  else if(isPending && !thumb) overlay = `<div class="thumb-overlay"><span class="thumb-overlay-icon" style="font-size:1rem;opacity:.5">⏳</span></div>`;

  const title = e.title || e.subject || '';
  const plat  = (e.platform || '').toLowerCase();
  const step  = e.step || 'new';

  return `<article class="media-card${isFailed?' is-failed':''}" onclick="showDetail(${e.id})"
    role="button" tabindex="0" aria-label="${esc(title||'Email #'+e.id)}"
    onkeydown="if(event.key==='Enter'||event.key===' ')showDetail(${e.id})">
    <div class="card-thumb">
      ${thumbContent}
      ${typeBadge}
      ${countBadge}
      ${dotsHtml}
      ${overlay}
    </div>
    <div class="card-body">
      <div class="card-title${title?'':' empty'}">${esc(title || '(sans titre)')}</div>
      <div class="card-meta">
        ${plat ? `<span class="card-plat">${platIcon(plat)} ${esc(plat)}</span>` : ''}
        <span class="card-step${isFailed?' fail':''}">${esc(step)}${isFailed?' ✗':''}</span>
        <div class="card-stars" aria-label="Note ${e.rating||0}/5">
          ${Array.from({length:5},(_,i)=>
            `<span class="card-star${(i+1)<=(e.rating||0)?' on':''}">★</span>`).join('')}
        </div>
      </div>
    </div>
  </article>`;
}

async function loadGallery(reset=true){
  if(STATE.loading) return;
  STATE.loading = true;
  if(reset) STATE.offset = 0;

  const gallery = $('gallery');

  if(reset){
    gallery.innerHTML = Array.from({length:10},()=>`
      <div class="skeleton-card">
        <div class="skeleton-thumb skeleton"></div>
        <div class="skeleton-body">
          <div class="skeleton-line skeleton" style="width:80%"></div>
          <div class="skeleton-line skeleton" style="width:50%"></div>
        </div>
      </div>`).join('');
    $('load-more-wrap').style.display = 'none';
  }

  const params = new URLSearchParams({limit: PAGE_SIZE, offset: STATE.offset});
  if(STATE.filter)   params.set('step',   STATE.filter);
  if(STATE.search)   params.set('search', STATE.search);

  const d = await api(`/api/emails?${params}`);
  STATE.loading = false;

  if(!d?.ok){
    gallery.innerHTML = `<div class="empty-state">
      <svg width="44" height="44" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.2" aria-hidden="true">
        <circle cx="12" cy="12" r="10"/><path d="M12 8v4M12 16h.01"/>
      </svg>
      <h3>Serveur inaccessible</h3>
      <p>Vérifiez que app_web.py est lancé sur le bon port.</p>
    </div>`;
    return;
  }

  let emails = d.emails || [];

  if(STATE.platform)
    emails = emails.filter(e => (e.platform||'').toLowerCase().includes(STATE.platform));

  if(STATE.sort === 'rating')
    emails = [...emails].sort((a,b) => (b.rating||0)-(a.rating||0));
  else if(STATE.sort === 'oldest')
    emails = [...emails].reverse();

  $('count-lbl').textContent = `${d.count ?? emails.length} résultat${(d.count??emails.length)!==1?'s':''}`;

  if(reset) gallery.innerHTML = '';

  if(!emails.length && STATE.offset === 0){
    gallery.innerHTML = `<div class="empty-state">
      <svg width="44" height="44" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.2" aria-hidden="true">
        <rect x="3" y="3" width="18" height="18" rx="2"/>
        <circle cx="8.5" cy="8.5" r="1.5"/>
        <polyline points="21 15 16 10 5 21"/>
      </svg>
      <h3>Aucun média${STATE.filter ? ' pour ce step' : ''}</h3>
      <p>${STATE.filter ? 'Essayez un autre filtre.' : 'Lancez le pipeline pour commencer.'}</p>
    </div>`;
    $('load-more-wrap').style.display = 'none';
    return;
  }

  gallery.insertAdjacentHTML('beforeend', emails.map(buildCard).join(''));

  const lmw = $('load-more-wrap');
  if(emails.length >= PAGE_SIZE){
    STATE.offset += PAGE_SIZE;
    lmw.style.display = 'flex';
  } else {
    lmw.style.display = 'none';
  }
}

async function loadMore(){ await loadGallery(false); }

function debounceSearch(){
  STATE.search = $('search-input').value.trim();
  clearTimeout(STATE.searchTimer);
  STATE.searchTimer = setTimeout(() => loadGallery(true), 280);
}

function setFilter(step){
  STATE.filter = step;
  document.querySelectorAll('.step-btn').forEach(b => b.classList.remove('active'));
  $('sb-' + (step||'all'))?.classList.add('active');
  loadGallery(true);
}

function setPlatform(btn, plat){
  STATE.platform = plat;
  document.querySelectorAll('[data-plat]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  loadGallery(true);
}

// ── Run / Reset ───────────────────────────────────────────────────────────────
async function runStep(step){
  log(`→ run ${step}…`, 'l-info');
  const d = await api(`/api/run/${step}`, {method:'POST'});
  if(!d) return;
  if(!d.ok){ log(`✗ ${step}: ${d.error||'erreur'}`, 'l-err'); return; }
  log(`✓ ${step} démarré`, 'l-ok');
  const rbtn = $('rbtn-'+step);
  if(rbtn){ rbtn.classList.add('running'); setTimeout(()=>rbtn.classList.remove('running'), 8000); }
  scheduleRefresh(4000);
}

async function doResetStep(step, emailId=null){
  const path = `/api/reset/step/${step}${emailId!=null?`?email_id=${emailId}`:''}`;
  log(`→ reset step ${step}${emailId!=null?` #${emailId}`:''}`, 'l-warn');
  const d = await api(path, {method:'POST'});
  if(!d) return;
  if(!d.ok){ log(`✗ reset: ${d.error}`, 'l-err'); return; }
  log(`✓ reset ${step}: ${d.reset??'?'} email(s) → ${d.target??'?'}`, 'l-ok');
  scheduleRefresh(800);
}

async function doResetFailed(emailId=null){
  const path = `/api/reset/failed${emailId!=null?`?email_id=${emailId}`:''}`;
  log(`→ reset failed${emailId!=null?` #${emailId}`:''}`, 'l-warn');
  const d = await api(path, {method:'POST'});
  if(!d) return;
  log(`✓ reset failed: ${d.reset??'?'} email(s)`, 'l-ok');
  scheduleRefresh(800);
}

function promptResetStep(emailId=null){
  const step = prompt(`Reset quel step ?\n(${STEPS_CLI.join(', ')})`);
  if(!step) return;
  if(!STEPS_CLI.includes(step)){ log('Step invalide : '+step,'l-err'); return; }
  doResetStep(step, emailId);
}

// ── Rating ────────────────────────────────────────────────────────────────────
async function rate(emailId, rating){
  const d = await api(`/api/emails/${emailId}/rating`, {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({rating})
  });
  if(!d?.ok) return;
  document.querySelectorAll('#stars-edit .star-edit').forEach((s,i) =>
    s.classList.toggle('on', (i+1) <= rating));
  document.querySelectorAll('.media-card').forEach(c => {
    if(c.getAttribute('onclick')?.includes(`showDetail(${emailId})`)){
      c.querySelectorAll('.card-star').forEach((s,i) => s.classList.toggle('on',(i+1)<=rating));
    }
  });
  log(`★ note ${rating}/5 enregistrée`, 'l-ok');
}

// ── Modal détail ──────────────────────────────────────────────────────────────
async function showDetail(id){
  $('player-wrap').innerHTML = `<div class="player-no-preview">
    <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" class="spin" aria-hidden="true">
      <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"/>
    </svg>
  </div>`;
  $('modal').classList.add('open');
  document.body.style.overflow = 'hidden';

  const d = await api(`/api/emails/${id}`);
  if(!d?.ok){ log('Détail introuvable','l-err'); return; }

  STATE.currentEmail    = d.email;
  STATE.currentMediaIdx = 0;
  renderModal(d.email);
}

function closeModal(){
  $('modal').classList.remove('open');
  document.body.style.overflow = '';
  const vid = $('player-wrap')?.querySelector('video');
  if(vid) vid.pause();
  STATE.currentEmail    = null;
  STATE.currentMediaIdx = 0;
}

function renderModal(e){
  $('modal-title').textContent = e.title || e.subject || `Email #${e.id}`;

  const allFiles   = e.media_files || [];
  const mediaFiles = allFiles.filter(f => f.file_type !== 'thumbnail');

  renderPlayer(e, mediaFiles, 0);

  // Carousel
  const carSec = $('carousel-section');
  if(mediaFiles.length > 1){
    carSec.style.display = 'block';
    $('carousel-label').textContent = `${mediaFiles.length} médias`;
    $('carousel-track').innerHTML = mediaFiles.map((f,i) => {
      const isVid = f.file_type === 'video';
      if(!isVid && f.url)
        return `<div class="carousel-item ${i===0?'active':''}" onclick="setMediaIdx(${i})" title="Fichier ${i+1}">
          <img src="${esc(f.url)}" alt="" loading="lazy" onerror="this.outerHTML='<div class=ci-icon>🖼</div>'">
        </div>`;
      return `<div class="carousel-item ${i===0?'active':''}" onclick="setMediaIdx(${i})" title="${isVid?'Vidéo':'Fichier'} ${i+1}">
        <div class="ci-icon">${isVid?'▶':'📄'}</div>
      </div>`;
    }).join('');
  } else { carSec.style.display = 'none'; }

  // LLM
  const llmSec = $('llm-section');
  if(e.llm_summary){ llmSec.style.display='block'; $('llm-box').textContent = e.llm_summary; }
  else              { llmSec.style.display='none'; }

  // Fichiers
  const fileSec = $('files-section');
  if(allFiles.length > 0){
    fileSec.style.display = 'block';
    $('file-list').innerHTML = allFiles.map((f,i) => {
      const midx = mediaFiles.indexOf(f);
      return `<div class="file-item ${midx===STATE.currentMediaIdx&&midx>=0?'active':''}"
        onclick="${midx>=0?`setMediaIdx(${midx})`:''}">
        <span class="file-type">${esc(f.file_type||'?')}</span>
        <span class="file-name" title="${esc(f.file_path||'')}">${esc((f.file_path||'').split(/[/\\]/).pop()||f.file_path||'–')}</span>
        <span class="file-size">${esc(fmtSize(f.file_size))}</span>
        ${f.is_primary?'<span style="color:var(--warn);font-size:.6rem">★</span>':''}
      </div>`;
    }).join('');
  } else { fileSec.style.display='none'; }

  // Infos
  const rows = [];
  if(e.platform)   rows.push(['Plateforme',`<span class="card-plat">${platIcon(e.platform)} ${esc(e.platform)}</span>`]);
  if(e.author)     rows.push(['Auteur',    esc(e.author)]);
  if(e.channel)    rows.push(['Chaîne',    esc(e.channel)]);
  if(e.post_date)  rows.push(['Date',      esc(e.post_date)]);
  if(e.duration)   rows.push(['Durée',     esc(e.duration)]);
  if(e.source_url) rows.push(['URL',`<a href="${esc(e.source_url)}" target="_blank" rel="noopener noreferrer">${esc(e.source_url.substring(0,55))}…</a>`]);
  rows.push(['Step', badgeHtml(e.step_status)+'&nbsp;<span class="ig-val mono">'+esc(e.step)+'</span>']);
  if(e.step_error) rows.push(['Erreur',`<span style="color:var(--fail);font-size:.62rem;font-family:var(--font-mono)">${esc(e.step_error)}</span>`]);
  $('info-grid').innerHTML = rows.map(([l,v])=>
    `<span class="ig-label">${esc(l)}</span><div class="ig-val">${v}</div>`).join('');

  // Keywords
  const kwSec = $('kw-section');
  const kws = [
    ...(e.known_keywords||[]).map(k=>`<span class="kw-tag kw-known">${esc(k)}</span>`),
    ...(e.unknown_keywords||[]).map(k=>`<span class="kw-tag kw-unknown">${esc(k)}</span>`),
  ];
  if(kws.length){ kwSec.style.display='block'; $('kw-list').innerHTML=kws.join(''); }
  else           { kwSec.style.display='none'; }

  // Stars
  $('stars-edit').innerHTML = starsHtml(e.rating, e.id);

  // Post
  const postSec = $('post-section');
  if(e.post_body){ postSec.style.display='block'; $('post-box').textContent=e.post_body; }
  else            { postSec.style.display='none'; }

  // Commentaires
  const cmtSec = $('comments-section');
  const cmts   = e.post_comments || [];
  if(cmts.length){
    cmtSec.style.display='block';
    $('comment-list').innerHTML = cmts.slice(0,12).map(c=>`
      <div class="comment">
        <div class="comment-meta">
          <span class="c-author">u/${esc(c.author||'?')}</span>
          <span class="c-score">▲ ${c.score||0}</span>
        </div>
        <div class="c-body">${esc((c.body||'').substring(0,280))}</div>
      </div>`).join('');
  } else { cmtSec.style.display='none'; }

  // Actions
  $('modal-actions').innerHTML = `
    <button class="maction warn"   onclick="promptResetStep(${e.id})">↺ Reset step</button>
    <button class="maction danger" onclick="doResetFailed(${e.id})">↺ Reset failed</button>
    <button class="maction"        onclick="runStep('reparse')">reparse</button>
    <button class="maction"        onclick="runStep('remeta')">remeta</button>`;
}

function renderPlayer(e, mediaFiles, idx){
  const wrap = $('player-wrap');
  if(!mediaFiles.length){
    wrap.innerHTML = `<div class="player-no-preview">
      <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.2" aria-hidden="true">
        <rect x="3" y="3" width="18" height="18" rx="2"/>
        <circle cx="8.5" cy="8.5" r="1.5"/>
        <polyline points="21 15 16 10 5 21"/>
      </svg>
      <span>Aucun fichier média</span>
    </div>`;
    return;
  }
  const f = mediaFiles[idx] || mediaFiles[0];
  if(f.file_type === 'video' && f.url){
    wrap.innerHTML = `<video controls preload="metadata" style="width:100%;max-height:400px;display:block;background:#000">
      <source src="${esc(f.url)}">
      Votre navigateur ne supporte pas la lecture vidéo.
    </video>`;
  } else if((f.file_type==='image'||f.file_type==='thumbnail') && f.url){
    wrap.innerHTML = `<img src="${esc(f.url)}" alt="${esc(e?.title||'')}"
      style="width:100%;max-height:400px;object-fit:contain;display:block;background:#000">`;
  } else {
    const name = (f.file_path||'').split(/[/\\]/).pop() || f.file_type || 'fichier';
    wrap.innerHTML = `<div class="player-no-preview">
      <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.2" aria-hidden="true">
        <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/>
        <polyline points="13 2 13 9 20 9"/>
      </svg>
      <span>${esc(name)} — aperçu indisponible</span>
    </div>`;
  }
}

function setMediaIdx(idx){
  STATE.currentMediaIdx = idx;
  if(!STATE.currentEmail) return;
  const allFiles   = STATE.currentEmail.media_files || [];
  const mediaFiles = allFiles.filter(f => f.file_type !== 'thumbnail');
  renderPlayer(STATE.currentEmail, mediaFiles, idx);
  document.querySelectorAll('.carousel-item').forEach((el,i) => el.classList.toggle('active',i===idx));
  document.querySelectorAll('.file-item').forEach((el,i) => {
    const midx = mediaFiles.indexOf(allFiles[i]);
    el.classList.toggle('active', midx === idx && midx >= 0);
  });
}

function shiftCarousel(dir){
  if(!STATE.currentEmail) return;
  const n = (STATE.currentEmail.media_files||[]).filter(f=>f.file_type!=='thumbnail').length;
  setMediaIdx((STATE.currentMediaIdx + dir + n) % n);
  const track = $('carousel-track');
  const items = track?.querySelectorAll('.carousel-item');
  items?.[STATE.currentMediaIdx]?.scrollIntoView({behavior:'smooth',block:'nearest',inline:'center'});
}

// ── SSE ───────────────────────────────────────────────────────────────────────
function connectSSE(){
  const es = new EventSource('/api/status/stream');
  es.onmessage = ev => {
    try{
      const p = JSON.parse(ev.data);
      (p.logs||[]).forEach(l => {
        const cls = /ERREUR|Exception|error/i.test(l) ? 'l-err'
                  : /Terminé|OK|✓/i.test(l)           ? 'l-ok'
                  : /warning|attention/i.test(l)       ? 'l-warn'
                  : 'l-info';
        log(l, cls);
      });
      (p.running||[]).forEach(s => {
        const btn = $('rbtn-'+s);
        if(btn) btn.classList.add('running');
      });
      STEPS_CLI.forEach(s => {
        if(!(p.running||[]).includes(s)){
          $('rbtn-'+s)?.classList.remove('running');
        }
      });
    } catch(_){}
  };
  es.onerror = () => {};
}

// ── Refresh ───────────────────────────────────────────────────────────────────
let _refreshTimeout = null;
function scheduleRefresh(ms=2000){
  clearTimeout(_refreshTimeout);
  _refreshTimeout = setTimeout(refresh, ms);
}

async function refresh(){
  const icon = $('refresh-icon');
  icon?.classList.add('spin');
  await loadStats();
  await loadGallery(true);
  icon?.classList.remove('spin');
}

// ── Clavier ───────────────────────────────────────────────────────────────────
document.addEventListener('keydown', e => {
  if(e.key === 'Escape'    && $('modal').classList.contains('open')) closeModal();
  if(e.key === 'ArrowLeft' && STATE.currentEmail) shiftCarousel(-1);
  if(e.key === 'ArrowRight'&& STATE.currentEmail) shiftCarousel(1);
});
$('modal').addEventListener('click', e => { if(e.target === $('modal')) closeModal(); });
$('sort-select').addEventListener('change', function(){ STATE.sort = this.value; loadGallery(true); });

// ── Init ──────────────────────────────────────────────────────────────────────
log('⬡ BiDi chargé', 'l-info');
loadStats();
loadGallery(true);
connectSSE();
setInterval(refresh, 20000);
