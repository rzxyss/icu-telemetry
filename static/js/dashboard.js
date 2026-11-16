/**
 * ICU Telemetry Command Center Dashboard Controller
 * Real-time SSE subscriber, Bed renderer, Audio Telemetry, and Interactive Controls.
 */

document.addEventListener('DOMContentLoaded', () => {
  // Application State
  const state = {
    beds: {},
    featuredBedId: 1,
    activeFilter: 'ALL',
    audioEnabled: false,
    streamActive: true,
    alerts: [],
    trendChart: null,
    modalActiveBedId: null
  };

  // Audio Context for Medical Beep Synthesis
  let audioCtx = null;
  function initAudio() {
    if (!audioCtx) {
      const AudioContext = window.AudioContext || window.webkitAudioContext;
      if (AudioContext) audioCtx = new AudioContext();
    }
    if (audioCtx && audioCtx.state === 'suspended') {
      audioCtx.resume();
    }
  }

  function playMedicalBeep(severity = 'CRITICAL') {
    if (!state.audioEnabled || !audioCtx) return;
    try {
      const osc = audioCtx.createOscillator();
      const gain = audioCtx.createGain();
      osc.type = 'sine';
      
      const freq = severity === 'CRITICAL' ? 880 : 587.33; // A5 or D5
      osc.frequency.setValueAtTime(freq, audioCtx.currentTime);
      
      gain.gain.setValueAtTime(0.08, audioCtx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.001, audioCtx.currentTime + 0.18);
      
      osc.connect(gain);
      gain.connect(audioCtx.destination);
      
      osc.start();
      osc.stop(audioCtx.currentTime + 0.19);
    } catch (e) {
      // Audio autoplay policy fallback
    }
  }

  // DOM Elements Cache
  const el = {
    bedsGrid: document.getElementById('beds-grid'),
    alertsContainer: document.getElementById('alerts-container'),
    alertCounterBadge: document.getElementById('alert-counter-badge'),
    
    // Spark Metrics
    sparkStatusText: document.getElementById('spark-status-text'),
    sparkIndicator: document.getElementById('spark-indicator'),
    statBatchId: document.getElementById('stat-batch-id'),
    statTotalRecords: document.getElementById('stat-total-records'),
    statLatency: document.getElementById('stat-latency'),
    
    // Summary Counts
    summaryCritCount: document.getElementById('summary-crit-count'),
    summaryWarnCount: document.getElementById('summary-warn-count'),
    summaryStableCount: document.getElementById('summary-stable-count'),
    
    // Featured Bed Elements
    featuredBedLabel: document.getElementById('featured-bed-label'),
    featuredPatientInfo: document.getElementById('featured-patient-info'),
    featHr: document.getElementById('feat-hr'),
    featSpo2: document.getElementById('feat-spo2'),
    featBp: document.getElementById('feat-bp'),
    featMap: document.getElementById('feat-map'),
    featResp: document.getElementById('feat-resp'),
    featTemp: document.getElementById('feat-temp'),
    featHrv: document.getElementById('feat-hrv'),
    featBmi: document.getElementById('feat-bmi'),
    
    // Controls
    btnToggleStream: document.getElementById('btn-toggle-stream'),
    btnStreamText: document.getElementById('btn-stream-text'),
    btnSound: document.getElementById('btn-sound'),
    iconSoundOn: document.getElementById('icon-sound-on'),
    iconSoundOff: document.getElementById('icon-sound-off'),
    btnAnomalyMenu: document.getElementById('btn-anomaly-menu'),
    anomalyDropdown: document.getElementById('anomaly-dropdown'),
    
    // Modal
    patientModal: document.getElementById('patient-modal'),
    modalClose: document.getElementById('modal-close'),
    modalCloseBtn: document.getElementById('modal-close-btn'),
    modalBedIcon: document.getElementById('modal-bed-icon'),
    modalTitle: document.getElementById('modal-title'),
    modalStatusBadge: document.getElementById('modal-status-badge'),
    modalPatientMeta: document.getElementById('modal-patient-meta'),
    modalVHr: document.getElementById('modal-v-hr'),
    modalVSpo2: document.getElementById('modal-v-spo2'),
    modalVBp: document.getElementById('modal-v-bp'),
    modalVMap: document.getElementById('modal-v-map'),
    modalVResp: document.getElementById('modal-v-resp'),
    modalVTemp: document.getElementById('modal-v-temp'),
    modalAlertsList: document.getElementById('modal-alerts-list')
  };

  // 1. Establish SSE Connection with Flask Backend
  function connectSSE() {
    console.log('[SSE] Connecting to ICU Telemetry stream...');
    const eventSource = new EventSource('/api/stream');

    eventSource.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        handleIncomingEvent(payload);
      } catch (err) {
        console.error('[SSE Error parsing data]', err);
      }
    };

    eventSource.onerror = (err) => {
      console.warn('[SSE Connection interrupted, reconnecting in 2s...]', err);
      el.sparkIndicator.className = 'w-2.5 h-2.5 rounded-full bg-rose-500 animate-ping';
      el.sparkStatusText.textContent = 'RECONNECTING';
      eventSource.close();
      setTimeout(connectSSE, 2000);
    };
  }

  // 2. Process incoming stream events
  function handleIncomingEvent(payload) {
    if (payload.event_type === 'initial_state') {
      // Populate state
      payload.beds.forEach(bed => {
        state.beds[bed.bed_id] = bed;
      });
      renderAllBeds();
      updateFeaturedBedDisplay();
      updateSparkMetrics(payload.metrics);
      if (payload.alerts) {
        state.alerts = payload.alerts;
        renderAlertFeed();
      }
    } else if (payload.event_type === 'bed_update') {
      const bed = payload.bed;
      state.beds[bed.bed_id] = bed;
      
      updateSingleBedCard(bed);
      
      if (bed.bed_id === state.featuredBedId) {
        updateFeaturedBedDisplay();
      }
      
      if (state.modalActiveBedId === bed.bed_id) {
        updateModalData(bed);
      }

      if (payload.latest_alert) {
        appendAlertItem(payload.latest_alert);
        if (payload.latest_alert.level === 'CRITICAL') {
          playMedicalBeep('CRITICAL');
        }
      }

      if (payload.metrics) {
        updateSparkMetrics(payload.metrics);
      }
    } else if (payload.event_type === 'spark_batch_completed') {
      updateSparkMetrics(payload.metrics);
    }
  }

  // 3. Update Spark Streaming Metrics in Header
  function updateSparkMetrics(metrics) {
    if (!metrics) return;
    el.statBatchId.textContent = `#${metrics.batch_id || 0}`;
    el.statTotalRecords.textContent = (metrics.total_records_processed || 0).toLocaleString();
    el.statLatency.textContent = `${(metrics.processing_latency_ms || 0).toFixed(1)} ms`;
    
    el.sparkIndicator.className = 'w-2.5 h-2.5 rounded-full bg-emerald-400 animate-pulse';
    el.sparkStatusText.textContent = metrics.spark_active ? 'SPARK STREAMING' : 'MICRO-BATCHING';

    el.summaryCritCount.textContent = metrics.critical_alerts_count || 0;
    el.summaryWarnCount.textContent = metrics.warning_alerts_count || 0;
    el.summaryStableCount.textContent = metrics.normal_count || 0;
  }

  // 4. Render Bed Cards
  function renderAllBeds() {
    el.bedsGrid.innerHTML = '';
    const sortedBedIds = Object.keys(state.beds).map(Number).sort((a,b) => a - b);
    
    sortedBedIds.forEach(id => {
      const bed = state.beds[id];
      const card = createBedCardElement(bed);
      el.bedsGrid.appendChild(card);
    });
    applyFilter();
  }

  function createBedCardElement(bed) {
    const card = document.createElement('div');
    card.id = `bed-card-${bed.bed_id}`;
    card.className = `bed-card bg-[#0b101c] border rounded-2xl p-3.5 transition-all duration-200 cursor-pointer hover:border-slate-600 ${getBedBorderClass(bed.risk_category)}`;
    card.dataset.risk = bed.risk_category;
    card.onclick = () => openPatientModal(bed.bed_id);
    
    card.innerHTML = getBedCardHTML(bed);
    return card;
  }

  function updateSingleBedCard(bed) {
    let card = document.getElementById(`bed-card-${bed.bed_id}`);
    if (!card) {
      card = createBedCardElement(bed);
      el.bedsGrid.appendChild(card);
      applyFilter();
      return;
    }
    
    card.className = `bed-card bg-[#0b101c] border rounded-2xl p-3.5 transition-all duration-200 cursor-pointer hover:border-slate-600 ${getBedBorderClass(bed.risk_category)}`;
    card.dataset.risk = bed.risk_category;
    card.innerHTML = getBedCardHTML(bed);
    
    // Quick flash animation on update
    card.style.transition = 'none';
    card.style.backgroundColor = '#121b2d';
    setTimeout(() => {
      card.style.transition = 'background-color 0.4s ease';
      card.style.backgroundColor = '#0b101c';
    }, 80);
    
    applyFilter();
  }

  function getBedBorderClass(category) {
    if (category === 'CRITICAL') return 'bed-critical border-rose-500';
    if (category === 'WARNING') return 'bed-warning border-amber-500';
    return 'bed-stable border-slate-800';
  }

  function getBedStatusBadge(category, status) {
    if (category === 'CRITICAL') {
      return `<span class="px-2 py-0.5 text-[10px] font-mono rounded-full bg-rose-950 text-rose-300 border border-rose-700/60 font-semibold animate-pulse">CRITICAL</span>`;
    }
    if (category === 'WARNING') {
      return `<span class="px-2 py-0.5 text-[10px] font-mono rounded-full bg-amber-950 text-amber-300 border border-amber-700/60 font-semibold">WARNING</span>`;
    }
    return `<span class="px-2 py-0.5 text-[10px] font-mono rounded-full bg-emerald-950 text-emerald-300 border border-emerald-700/60 font-semibold">STABLE</span>`;
  }

  function getBedCardHTML(bed) {
    const v = bed.vitals || {};
    const hrColor = v.heart_rate > 100 ? 'text-rose-400 font-bold' : (v.heart_rate < 60 ? 'text-amber-400' : 'text-emerald-400');
    const spo2Color = v.oxygen_saturation < 92 ? 'text-rose-400 font-bold' : (v.oxygen_saturation < 95 ? 'text-amber-400' : 'text-cyan-400');
    const isSelected = bed.bed_id === state.featuredBedId;

    return `
      <div class="flex items-center justify-between pb-2 border-b border-slate-800/80">
        <div class="flex items-center gap-2">
          <span class="w-6 h-6 rounded flex items-center justify-center font-mono text-xs font-bold ${isSelected ? 'bg-cyan-500 text-black' : 'bg-slate-800 text-slate-300'}">
            ${bed.bed_id < 10 ? '0' + bed.bed_id : bed.bed_id}
          </span>
          <div>
            <h4 class="text-xs font-bold text-white tracking-wide">${bed.bed_label}</h4>
            <span class="text-[10px] font-mono text-slate-400">Pt #${bed.patient_id || '-'} • ${bed.age || '-'}Th • ${bed.gender ? bed.gender[0] : '-'}</span>
          </div>
        </div>
        <div>
          ${getBedStatusBadge(bed.risk_category, bed.status)}
        </div>
      </div>

      <!-- Vitals 2x2 Grid -->
      <div class="grid grid-cols-3 gap-1.5 mt-2.5 text-center font-mono">
        <!-- HR -->
        <div class="bg-[#070b13] p-1.5 rounded-lg border border-slate-800/60">
          <span class="text-[9px] uppercase text-slate-400 block">HR</span>
          <span class="text-base font-bold ${hrColor}">${v.heart_rate || '--'}</span>
          <span class="text-[9px] text-slate-500 block">bpm</span>
        </div>
        <!-- SpO2 -->
        <div class="bg-[#070b13] p-1.5 rounded-lg border border-slate-800/60">
          <span class="text-[9px] uppercase text-slate-400 block">SpO2</span>
          <span class="text-base font-bold ${spo2Color}">${v.oxygen_saturation || '--'}</span>
          <span class="text-[9px] text-slate-500 block">%</span>
        </div>
        <!-- BP -->
        <div class="bg-[#070b13] p-1.5 rounded-lg border border-slate-800/60">
          <span class="text-[9px] uppercase text-slate-400 block">BP</span>
          <span class="text-sm font-bold text-indigo-300">${v.systolic_bp || '-'}/${v.diastolic_bp || '-'}</span>
          <span class="text-[9px] text-slate-500 block">mmHg</span>
        </div>
      </div>

      <!-- Secondary Telemetry Line -->
      <div class="flex items-center justify-between mt-2 pt-2 border-t border-slate-800/60 text-[10px] font-mono text-slate-400">
        <span>Resp: <b class="text-slate-200">${v.respiratory_rate || '-'}</b></span>
        <span>Temp: <b class="text-slate-200">${v.temperature || '-'}°C</b></span>
        <span>MAP: <b class="text-slate-200">${v.map || '-'}</b></span>
      </div>

      <!-- Card Action Footer -->
      <div class="mt-2.5 flex items-center justify-between">
        <button onclick="event.stopPropagation(); window.setFeaturedBed(${bed.bed_id})" class="text-[10px] font-mono px-2 py-0.5 rounded ${isSelected ? 'bg-cyan-950 text-cyan-400 border border-cyan-800' : 'text-slate-400 hover:text-white hover:bg-slate-800'} transition">
          ${isSelected ? '● OSC SYNCED' : 'Sync ECG'}
        </button>
        <span class="text-[10px] text-slate-500 font-mono">${bed.last_timestamp || '--:--:--'}</span>
      </div>
    `;
  }

  // 5. Featured Bed Synchronization
  window.setFeaturedBed = function(bedId) {
    state.featuredBedId = bedId;
    updateFeaturedBedDisplay();
    renderAllBeds();
  };

  function updateFeaturedBedDisplay() {
    const bed = state.beds[state.featuredBedId];
    if (!bed) return;

    el.featuredBedLabel.textContent = bed.bed_label;
    el.featuredPatientInfo.textContent = `(Pasien #${bed.patient_id}, ${bed.gender}, ${bed.age} Th, BMI: ${bed.vitals.bmi})`;

    const v = bed.vitals || {};
    el.featHr.textContent = v.heart_rate || '--';
    el.featSpo2.textContent = v.oxygen_saturation || '--';
    el.featBp.textContent = `${v.systolic_bp || '-'}/${v.diastolic_bp || '-'}`;
    el.featMap.textContent = v.map || '--';
    el.featResp.textContent = v.respiratory_rate || '--';
    el.featTemp.textContent = v.temperature || '--';
    el.featHrv.textContent = v.hrv || '--';
    el.featBmi.textContent = v.bmi || '--';

    // Synchronize ECG Oscilloscope BPM
    if (window.ecgMonitor) {
      window.ecgMonitor.setBPM(v.heart_rate || 75);
    }
  }

  // 6. Alert Triage Feed Rendering
  function appendAlertItem(alert) {
    if (!alert) return;
    state.alerts.unshift(alert);
    if (state.alerts.length > 50) state.alerts.pop();
    renderAlertFeed();
  }

  function renderAlertFeed() {
    el.alertCounterBadge.textContent = `${state.alerts.length} Alerts`;
    if (state.alerts.length === 0) {
      el.alertsContainer.innerHTML = `
        <div class="text-center text-slate-500 py-12 text-xs font-mono">
          Menunggu aliran event tanda vital...
        </div>
      `;
      return;
    }

    el.alertsContainer.innerHTML = state.alerts.map(a => {
      const isCritical = a.level === 'CRITICAL';
      const borderClass = isCritical ? 'border-rose-500/50 bg-rose-950/20' : 'border-amber-500/50 bg-amber-950/20';
      const badgeClass = isCritical ? 'bg-rose-950 text-rose-300 border-rose-700' : 'bg-amber-950 text-amber-300 border-amber-700';

      return `
        <div class="alert-item-enter border ${borderClass} p-2.5 rounded-xl transition hover:border-slate-500 cursor-pointer" onclick="window.openPatientModal(${a.bed_id})">
          <div class="flex items-center justify-between">
            <span class="px-1.5 py-0.5 rounded text-[10px] font-mono border ${badgeClass} font-bold">${a.level}</span>
            <span class="font-mono text-[10px] text-slate-400">${a.timestamp}</span>
          </div>
          <div class="mt-1">
            <span class="font-bold text-white font-mono">${a.bed_label}:</span>
            <span class="text-slate-200">${a.message}</span>
          </div>
          <div class="text-[10px] font-mono text-slate-400 mt-1">
            ${a.vitals_snapshot || ''}
          </div>
        </div>
      `;
    }).join('');
  }

  // 7. Patient Detail Modal & Chart.js Integration
  window.openPatientModal = function(bedId) {
    initAudio(); // User interaction unlocks Web Audio API
    state.modalActiveBedId = bedId;
    const bed = state.beds[bedId];
    if (!bed) return;

    el.modalBedIcon.textContent = bed.bed_id < 10 ? '0' + bed.bed_id : bed.bed_id;
    el.modalTitle.textContent = `${bed.bed_label} Telemetry Detail`;
    el.modalPatientMeta.textContent = `Pasien ID: ${bed.patient_id} • Usia: ${bed.age} Th • Gender: ${bed.gender} • BMI: ${bed.vitals.bmi}`;
    
    updateModalData(bed);
    initModalTrendChart(bed);

    el.patientModal.classList.remove('hidden');
  };

  function updateModalData(bed) {
    const v = bed.vitals || {};
    el.modalVHr.textContent = v.heart_rate || '--';
    el.modalVSpo2.textContent = v.oxygen_saturation || '--';
    el.modalVBp.textContent = `${v.systolic_bp || '-'}/${v.diastolic_bp || '-'}`;
    el.modalVMap.textContent = v.map || '--';
    el.modalVResp.textContent = v.respiratory_rate || '--';
    el.modalVTemp.textContent = v.temperature || '--';

    el.modalStatusBadge.innerHTML = getBedStatusBadge(bed.risk_category, bed.status);

    if (bed.active_alerts && bed.active_alerts.length > 0) {
      el.modalAlertsList.innerHTML = bed.active_alerts.map(al => {
        const color = al.level === 'CRITICAL' ? 'text-rose-400 font-bold' : 'text-amber-400';
        return `<li class="${color}">⚠ [${al.type}] ${al.message}</li>`;
      }).join('');
    } else {
      el.modalAlertsList.innerHTML = `<li class="text-emerald-400">✓ Semua parameter tanda vital berada dalam batas normal.</li>`;
    }

    // Update chart if active
    if (state.trendChart && bed.history) {
      state.trendChart.data.labels = bed.history.timestamps || [];
      state.trendChart.data.datasets[0].data = bed.history.heart_rate || [];
      state.trendChart.data.datasets[1].data = bed.history.oxygen_saturation || [];
      state.trendChart.data.datasets[2].data = bed.history.systolic_bp || [];
      state.trendChart.update('none');
    }
  }

  function initModalTrendChart(bed) {
    const ctx = document.getElementById('patientTrendChart');
    if (!ctx) return;

    if (state.trendChart) {
      state.trendChart.destroy();
    }

    const hist = bed.history || { timestamps: [], heart_rate: [], oxygen_saturation: [], systolic_bp: [] };

    state.trendChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: hist.timestamps,
        datasets: [
          {
            label: 'Heart Rate (BPM)',
            data: hist.heart_rate,
            borderColor: '#10b981',
            backgroundColor: 'rgba(16, 185, 129, 0.1)',
            tension: 0.3,
            borderWidth: 2,
            pointRadius: 2,
            yAxisID: 'y'
          },
          {
            label: 'SpO2 (%)',
            data: hist.oxygen_saturation,
            borderColor: '#06b6d4',
            backgroundColor: 'rgba(6, 182, 212, 0.1)',
            tension: 0.3,
            borderWidth: 2,
            pointRadius: 2,
            yAxisID: 'y1'
          },
          {
            label: 'Systolic BP (mmHg)',
            data: hist.systolic_bp,
            borderColor: '#a855f7',
            backgroundColor: 'transparent',
            tension: 0.3,
            borderWidth: 1.5,
            borderDash: [4, 4],
            pointRadius: 0,
            yAxisID: 'y'
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: {
          x: {
            grid: { color: 'rgba(255, 255, 255, 0.05)' },
            ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 10 } }
          },
          y: {
            position: 'left',
            min: 40,
            max: 180,
            grid: { color: 'rgba(255, 255, 255, 0.05)' },
            ticks: { color: '#10b981', font: { family: 'JetBrains Mono', size: 10 } }
          },
          y1: {
            position: 'right',
            min: 80,
            max: 100,
            grid: { drawOnChartArea: false },
            ticks: { color: '#06b6d4', font: { family: 'JetBrains Mono', size: 10 } }
          }
        },
        plugins: {
          legend: {
            labels: { color: '#94a3b8', font: { family: 'Inter', size: 11 } }
          }
        }
      }
    });
  }

  function closeModal() {
    el.patientModal.classList.add('hidden');
    state.modalActiveBedId = null;
    if (state.trendChart) {
      state.trendChart.destroy();
      state.trendChart = null;
    }
  }

  el.modalClose.onclick = closeModal;
  el.modalCloseBtn.onclick = closeModal;
  el.patientModal.onclick = (e) => {
    if (e.target === el.patientModal) closeModal();
  };

  // 8. Filter Tabs Handling
  document.querySelectorAll('.filter-tab').forEach(tab => {
    tab.onclick = () => {
      document.querySelectorAll('.filter-tab').forEach(t => {
        t.classList.remove('active', 'bg-slate-800', 'text-white', 'font-semibold');
      });
      tab.classList.add('active', 'bg-slate-800', 'text-white', 'font-semibold');
      state.activeFilter = tab.dataset.filter;
      applyFilter();
    };
  });

  function applyFilter() {
    document.querySelectorAll('.bed-card').forEach(card => {
      const risk = card.dataset.risk;
      if (state.activeFilter === 'ALL' || risk === state.activeFilter) {
        card.style.display = 'block';
      } else {
        card.style.display = 'none';
      }
    });
  }

  // 9. Interactive Controls
  // Stream Toggle
  el.btnToggleStream.onclick = async () => {
    initAudio();
    try {
      const res = await fetch('/api/control/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'toggle' })
      });
      const data = await res.json();
      state.streamActive = data.stream_active;
      
      if (state.streamActive) {
        el.btnStreamText.textContent = 'Pause Stream';
        el.btnToggleStream.className = 'flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-medium text-xs transition border border-emerald-400/40 shadow-sm';
      } else {
        el.btnStreamText.textContent = 'Resume Stream';
        el.btnToggleStream.className = 'flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-amber-600 hover:bg-amber-500 text-white font-medium text-xs transition border border-amber-400/40 shadow-sm';
      }
    } catch (err) {
      console.error('Failed to toggle stream', err);
    }
  };

  // Speed Selector
  document.querySelectorAll('.speed-btn').forEach(btn => {
    btn.onclick = async () => {
      const speed = parseFloat(btn.dataset.speed);
      document.querySelectorAll('.speed-btn').forEach(b => {
        b.classList.remove('bg-slate-800', 'text-cyan-300', 'font-bold');
        b.classList.add('text-slate-400');
      });
      btn.classList.add('bg-slate-800', 'text-cyan-300', 'font-bold');
      btn.classList.remove('text-slate-400');
      
      await fetch('/api/control/speed', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ multiplier: speed })
      });
    };
  });

  // Sound Toggle
  el.btnSound.onclick = () => {
    initAudio();
    state.audioEnabled = !state.audioEnabled;
    if (state.audioEnabled) {
      el.iconSoundOn.classList.remove('hidden');
      el.iconSoundOff.classList.add('hidden');
      el.btnSound.classList.add('bg-cyan-950', 'text-cyan-400', 'border-cyan-800');
      playMedicalBeep('CRITICAL');
    } else {
      el.iconSoundOn.classList.add('hidden');
      el.iconSoundOff.classList.remove('hidden');
      el.btnSound.classList.remove('bg-cyan-950', 'text-cyan-400', 'border-cyan-800');
    }
  };

  // Anomaly Injection Menu
  el.btnAnomalyMenu.onclick = (e) => {
    e.stopPropagation();
    el.anomalyDropdown.classList.toggle('hidden');
  };

  document.addEventListener('click', () => {
    el.anomalyDropdown.classList.add('hidden');
  });

  document.querySelectorAll('.anomaly-opt').forEach(opt => {
    opt.onclick = async () => {
      initAudio();
      const anomalyType = opt.dataset.type;
      el.anomalyDropdown.classList.add('hidden');
      
      try {
        const res = await fetch('/api/control/inject_anomaly', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ type: anomalyType, bed_id: state.featuredBedId })
        });
        const data = await res.json();
        console.log('[Anomaly Injected]', data);
      } catch (err) {
        console.error('Failed to inject anomaly', err);
      }
    };
  });

  // Start connection
  connectSSE();
});
