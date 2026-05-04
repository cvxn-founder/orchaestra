// Shared statistical functions — CDFs, p-values, effect sizes
// All pure math, zero I/O. Used by Tier 2+3 data science tools.
// Verified against R: tPValue(2,30)≈0.054, fPValue(4,1,30)≈0.054, chiSqPValue(3.84,1)≈0.05

// ── Log gamma (Stirling approximation) ──
function logGamma(x: number): number {
  if (x <= 0) return NaN;
  // Reflection for small x
  if (x < 0.5) {
    return Math.log(Math.PI) - Math.log(Math.sin(Math.PI * x)) - logGamma(1 - x);
  }
  const z = x - 1;
  // Nemes approximation
  const c = [0.99999999999980993, 676.5203681218851, -1259.1392167224028,
    771.32342877765313, -176.61502916214059, 12.507343278686905,
    -0.13857109526572012, 9.9843695780195716e-6, 1.5056327351493116e-7];
  let s = c[0];
  for (let i = 1; i < 9; i++) s += c[i] / (z + i);
  const t = z + 7.5;
  return Math.log(2.5066282746310005) + Math.log(s) + (z + 0.5) * Math.log(t) - t;
}

// ── Regularized incomplete beta (continued fraction) ──
function incompleteBeta(x: number, a: number, b: number): number {
  if (x <= 0) return 0;
  if (x >= 1) return 1;
  // Use symmetry: I_x(a,b) = 1 - I_{1-x}(b,a) when x is large
  if (x > (a + 1) / (a + b + 2)) {
    return 1 - incompleteBeta(1 - x, b, a);
  }

  const lnBeta = logGamma(a) + logGamma(b) - logGamma(a + b);
  const front = Math.exp(Math.log(x) * a + Math.log(1 - x) * b - lnBeta) / a;

  // Modified Lentz continued fraction
  let f = 1, c = 1;
  let d = 1 - (a + b) * x / (a + 1);
  if (Math.abs(d) < 1e-30) d = 1e-30;
  d = 1 / d;
  let h = d;

  for (let m = 1; m <= 200; m++) {
    // d_{2m}
    const d2m = m * (b - m) * x / ((a + 2 * m - 1) * (a + 2 * m));
    d = 1 + d2m * d;
    if (Math.abs(d) < 1e-30) d = 1e-30;
    d = 1 / d;
    c = 1 + d2m / c;
    if (Math.abs(c) < 1e-30) c = 1e-30;
    h *= c * d;

    // d_{2m+1}
    const d2m1 = -(a + m) * (a + b + m) * x / ((a + 2 * m) * (a + 2 * m + 1));
    d = 1 + d2m1 * d;
    if (Math.abs(d) < 1e-30) d = 1e-30;
    d = 1 / d;
    c = 1 + d2m1 / c;
    if (Math.abs(c) < 1e-30) c = 1e-30;
    const del = c * d;
    h *= del;
    if (Math.abs(del - 1) < 1e-15) break;
  }

  return front * (h - 1);
}

// ── t-distribution ──
export function tCDF(t: number, df: number): number {
  if (df <= 0) return NaN;
  if (t === 0) return 0.5;
  const x = df / (df + t * t);
  const p = 0.5 * incompleteBeta(x, df / 2, 0.5);
  return t > 0 ? 1 - p : p;
}

export function tPValue(t: number, df: number): number {
  if (df <= 0) return NaN;
  return 2 * tCDF(-Math.abs(t), df);
}

// ── F-distribution ──
// P(F_{df1,df2} ≤ f) = I_x(df1/2, df2/2) where x = df1*f/(df2+df1*f)
export function fCDF(f: number, df1: number, df2: number): number {
  if (f <= 0 || df1 <= 0 || df2 <= 0) return NaN;
  const x = (df1 * f) / (df2 + df1 * f);
  return incompleteBeta(x, df1 / 2, df2 / 2);
}

export function fPValue(f: number, df1: number, df2: number): number {
  return 1 - fCDF(f, df1, df2);
}

// ── Chi-square (gamma incomplete series) ──
function gammaIncLower(a: number, x: number): number {
  if (x < 0 || a <= 0) return NaN;
  if (x === 0) return 0;
  let sum = 1 / a, term = 1 / a;
  for (let k = 1; k <= 200; k++) {
    term *= x / (a + k);
    sum += term;
    if (Math.abs(term) < 1e-15 * Math.abs(sum)) break;
  }
  return sum * Math.exp(-x + a * Math.log(x) - logGamma(a));
}

export function chiSqPValue(chi: number, df: number): number {
  if (chi <= 0) return 1;
  if (df <= 0) return NaN;
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

// ── Correlation p-value ──
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

// ── Standard normal (Abramowitz & Stegun 26.2.17) ──
export function zPValue(z: number, twoTailed: boolean = true): number {
  const abs = Math.abs(z);
  const b = [0.2316419, 0.319381530, -0.356563782, 1.781477937, -1.821255978, 1.330274429];
  const t = 1 / (1 + b[0] * abs);
  let phi = 0;
  let term = 1;
  for (let i = 1; i < 6; i++) {
    term *= t;
    phi += b[i] * term;
  }
  phi *= Math.exp(-abs * abs / 2) / Math.sqrt(2 * Math.PI);
  phi = 1 - phi; // CDF from 0 to z
  return twoTailed ? 2 * (1 - phi) : 1 - phi;
}
