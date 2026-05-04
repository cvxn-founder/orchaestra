// Shared statistical functions — CDFs, p-values, effect sizes
// All pure math, zero I/O. Used by Tier 2+3 data science tools.

// ── Gamma function (Lanczos approximation) ──
function logGamma(x: number): number {
  if (x <= 0) return NaN;
  const g = 7;
  const c = [0.99999999999980993, 676.5203681218851, -1259.1392167224028,
    771.32342877765313, -176.61502916214059, 12.507343278686905,
    -0.13857109526572012, 9.9843695780195716e-6, 1.5056327351493116e-7];
  let z = x;
  let s = c[0];
  for (let i = 1; i < g + 2; i++) s += c[i] / (z + i);
  const t = z + g + 0.5;
  return 0.5 * Math.log(2 * Math.PI) + (z + 0.5) * Math.log(t) - t + Math.log(s) - Math.log(z);
}

function gamma(x: number): number {
  if (x < 0.5) return Math.PI / (Math.sin(Math.PI * x) * gamma(1 - x));
  return Math.exp(logGamma(x));
}

// ── Regularized incomplete beta function ──
function incompleteBeta(x: number, a: number, b: number): number {
  if (x < 0 || x > 1) return NaN;
  if (x === 0 || x === 1) return x;
  const lbeta = logGamma(a) + logGamma(b) - logGamma(a + b);
  let front = Math.exp(Math.log(x) * a + Math.log(1 - x) * b - lbeta) / a;
  let f = 1, c = 1, d = 0;
  for (let i = 0; i <= 200; i++) {
    let numerator: number;
    if (i % 2 === 0) {
      const m = i / 2;
      numerator = (m * (b - m) * x) / ((a + 2 * m - 1) * (a + 2 * m));
    } else {
      const m = (i - 1) / 2;
      numerator = -((a + m) * (a + b + m) * x) / ((a + 2 * m) * (a + 2 * m + 1));
    }
    d = 1 + numerator * d;
    if (Math.abs(d) < 1e-30) d = 1e-30;
    d = 1 / d;
    c = 1 + numerator / c;
    if (Math.abs(c) < 1e-30) c = 1e-30;
    front *= c * d;
    f += front;
    if (Math.abs(front) < 1e-15) break;
  }
  return f;
}

// ── t-distribution CDF ──
export function tCDF(t: number, df: number): number {
  if (df <= 0) return NaN;
  const x = df / (df + t * t);
  const p = 0.5 * incompleteBeta(x, df / 2, 0.5);
  return t >= 0 ? 1 - p : p;
}

// t-distribution two-tailed p-value
export function tPValue(t: number, df: number): number {
  return 2 * Math.min(tCDF(Math.abs(t), df), 1 - tCDF(Math.abs(t), df));
}

// ── F-distribution CDF ──
export function fCDF(f: number, df1: number, df2: number): number {
  if (f <= 0 || df1 <= 0 || df2 <= 0) return NaN;
  const x = (df1 * f) / (df1 * f + df2);
  return 1 - incompleteBeta(x, df1 / 2, df2 / 2);
}

// F-distribution p-value (upper tail)
export function fPValue(f: number, df1: number, df2: number): number {
  return 1 - fCDF(f, df1, df2);
}

// ── Chi-square CDF ──
function chiSqCDF(x: number, df: number): number {
  if (x <= 0) return 0;
  return incompleteBeta(x / (x + df), df / 2, 0.5) / 2 + 0.5;
  // Actually chi-sq is gamma(df/2, 2). Use gamma incomplete:
  // return gammaInc(df/2, x/2);
}

function gammaIncLower(a: number, x: number): number {
  if (x < 0 || a <= 0) return NaN;
  if (x === 0) return 0;
  let sum = 1 / a, term = 1 / a;
  for (let n = 1; n <= 200; n++) {
    term *= x / (a + n);
    sum += term;
    if (Math.abs(term) < Math.abs(sum) * 1e-15) break;
  }
  return sum * Math.exp(-x + a * Math.log(x) - logGamma(a));
}

export function chiSqPValue(chi: number, df: number): number {
  if (chi <= 0) return 1;
  return 1 - gammaIncLower(df / 2, chi / 2);
}

// ── Effect sizes ──
export function cohensD(mean1: number, mean2: number, sd1: number, sd2: number, n1: number, n2: number): number {
  const pooled = Math.sqrt(((n1 - 1) * sd1 * sd1 + (n2 - 1) * sd2 * sd2) / (n1 + n2 - 2));
  return (mean1 - mean2) / pooled;
}

export function interpretD(d: number): string {
  const abs = Math.abs(d);
  if (abs < 0.2) return "negligible";
  if (abs < 0.5) return "small";
  if (abs < 0.8) return "medium";
  return "large";
}

// ── Correlation p-value via t-distribution ──
export function corrPValue(r: number, n: number): number {
  if (Math.abs(r) >= 1) return 0;
  const t = r * Math.sqrt((n - 2) / (1 - r * r));
  return tPValue(t, n - 2);
}

export function interpretCorr(r: number): string {
  const abs = Math.abs(r);
  if (abs < 0.1) return "negligible";
  if (abs < 0.3) return "weak";
  if (abs < 0.5) return "moderate";
  if (abs < 0.7) return "strong";
  return "very strong";
}

// ── Z-score → p-value (standard normal) ──
export function zPValue(z: number, twoTailed: boolean = true): number {
  // Abramowitz approximation
  const abs = Math.abs(z);
  const b0 = 0.2316419, b1 = 0.319381530, b2 = -0.356563782, b3 = 1.781477937, b4 = -1.821255978, b5 = 1.330274429;
  const t = 1 / (1 + b0 * abs);
  const phi = 1 - (1 / Math.sqrt(2 * Math.PI)) * Math.exp(-abs * abs / 2) * (b1 * t + b2 * t * t + b3 * t * t * t + b4 * t * t * t * t + b5 * t * t * t * t * t);
  return twoTailed ? 2 * phi : phi;
}
