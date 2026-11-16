/**
 * Realistic Clinical ECG Lead II Oscilloscope Simulation
 * Synthesizes standard P-Q-R-S-T cardiac waveform synchronized with Heart Rate (BPM).
 */

class ECGMonitor {
  constructor(canvasId) {
    this.canvas = document.getElementById(canvasId);
    if (!this.canvas) return;
    this.ctx = this.canvas.getContext('2d');
    
    this.bpm = 75;
    this.isRunning = true;
    this.sweepX = 0;
    this.prevX = 0;
    this.prevY = 0;
    
    // Waveform phase accumulator
    this.phase = 0; // 0 to 1 per cardiac cycle
    
    this.initCanvasSize();
    window.addEventListener('resize', () => this.initCanvasSize());
    
    // Start rendering loop
    this.lastTime = performance.now();
    this.animate = this.animate.bind(this);
    requestAnimationFrame(this.animate);
  }

  initCanvasSize() {
    const rect = this.canvas.getBoundingClientRect();
    this.width = rect.width;
    this.height = rect.height;
    
    // Set actual canvas pixel buffer matching display resolution (high-DPI)
    const dpr = window.devicePixelRatio || 1;
    this.canvas.width = this.width * dpr;
    this.canvas.height = this.height * dpr;
    this.ctx.scale(dpr, dpr);
    
    this.baselineY = this.height * 0.55;
    this.amplitude = this.height * 0.38;
    
    // Clear initial canvas
    this.ctx.fillStyle = '#03070d';
    this.ctx.fillRect(0, 0, this.width, this.height);
    this.sweepX = 0;
    this.prevX = 0;
    this.prevY = this.baselineY;
  }

  setBPM(bpm) {
    this.bpm = Math.max(35, Math.min(200, bpm || 75));
  }

  /**
   * Evaluates P-Q-R-S-T wave function at cycle phase t in [0, 1].
   */
  calculateECGVoltage(t) {
    // Standard normal cardiac cycle phase allocations
    // 0.00 - 0.12: P Wave (atrial depolarization)
    // 0.12 - 0.22: PR Segment (isoelectric)
    // 0.22 - 0.25: Q Wave (septal depolarization)
    // 0.25 - 0.30: R Wave (ventricular depolarization spike)
    // 0.30 - 0.34: S Wave (downward deflection)
    // 0.34 - 0.44: ST Segment (isoelectric)
    // 0.44 - 0.65: T Wave (ventricular repolarization)
    // 0.65 - 1.00: TP Interval (isoelectric resting baseline)
    
    if (t >= 0.02 && t < 0.14) {
      // P Wave: Gaussian bump
      const pMid = 0.08;
      const pWidth = 0.035;
      return 0.15 * Math.exp(-Math.pow((t - pMid) / pWidth, 2));
    } else if (t >= 0.22 && t < 0.25) {
      // Q Wave: Small dip
      const qMid = 0.235;
      const qWidth = 0.01;
      return -0.15 * Math.exp(-Math.pow((t - qMid) / qWidth, 2));
    } else if (t >= 0.25 && t < 0.31) {
      // R Wave: Sharp tall spike
      const rMid = 0.28;
      const rWidth = 0.018;
      return 1.0 * Math.exp(-Math.pow((t - rMid) / rWidth, 2));
    } else if (t >= 0.31 && t < 0.36) {
      // S Wave: Negative dip
      const sMid = 0.335;
      const sWidth = 0.015;
      return -0.28 * Math.exp(-Math.pow((t - sMid) / sWidth, 2));
    } else if (t >= 0.46 && t < 0.66) {
      // T Wave: Broader rounded wave
      const tMid = 0.56;
      const tWidth = 0.06;
      return 0.32 * Math.exp(-Math.pow((t - tMid) / tWidth, 2));
    }
    
    // Isoelectric baseline with subtle physiological micro-noise
    return (Math.random() - 0.5) * 0.015;
  }

  animate(currentTime) {
    if (!this.isRunning) {
      requestAnimationFrame(this.animate);
      return;
    }

    const deltaTime = Math.min(0.1, (currentTime - this.lastTime) / 1000);
    this.lastTime = currentTime;

    // Cardiac cycle frequency in Hz: BPM / 60
    const freq = this.bpm / 60.0;
    this.phase = (this.phase + deltaTime * freq) % 1.0;

    // Sweep speed across monitor (pixels per second)
    const sweepSpeed = this.width / 4.0; // 4 seconds full sweep across screen
    const advanceX = sweepSpeed * deltaTime;
    
    const nextX = (this.sweepX + advanceX);
    
    // Calculate new voltage and Y position (invert for canvas Y-axis)
    const voltage = this.calculateECGVoltage(this.phase);
    const nextY = this.baselineY - (voltage * this.amplitude);

    // Eraser bar sweeping ahead of the beam (erases ~25px ahead)
    const eraseWidth = 28;
    this.ctx.fillStyle = '#03070d';
    
    if (nextX < this.width) {
      this.ctx.fillRect(nextX, 0, eraseWidth, this.height);
    } else {
      // Wrap around screen edge
      this.ctx.fillRect(0, 0, eraseWidth, this.height);
    }

    // Draw ECG Lead beam
    this.ctx.save();
    this.ctx.strokeStyle = '#00ff88'; // Medical phosphor green
    this.ctx.lineWidth = 2.0;
    this.ctx.lineCap = 'round';
    this.ctx.lineJoin = 'round';
    this.ctx.shadowBlur = 6;
    this.ctx.shadowColor = '#00ff88';

    this.ctx.beginPath();
    if (nextX >= this.prevX) {
      this.ctx.moveTo(this.prevX, this.prevY);
      this.ctx.lineTo(nextX, nextY);
    } else {
      // Wrap-around reset
      this.ctx.moveTo(0, nextY);
      this.ctx.lineTo(nextX, nextY);
    }
    this.ctx.stroke();
    this.ctx.restore();

    // Advance sweep
    if (nextX >= this.width) {
      this.sweepX = 0;
      this.prevX = 0;
    } else {
      this.sweepX = nextX;
      this.prevX = nextX;
    }
    this.prevY = nextY;

    requestAnimationFrame(this.animate);
  }
}

// Global instance exposed for dashboard synchronization
window.ecgMonitor = null;
document.addEventListener('DOMContentLoaded', () => {
  window.ecgMonitor = new ECGMonitor('ecgCanvas');
});
