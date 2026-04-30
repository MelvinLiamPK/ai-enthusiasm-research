# Sentiment Measurement Diagnostics

Sanity checks on the LinkedIn AI sentiment measure prior to using it in
downstream causal work. Each diagnostic answers a different question about
whether the measure tracks something real.

## Diagnostic 1 — Face validity

See `outputs/sanity_checks/face_validity_*` for the latest run. Headline:
post-ChatGPT (Nov 2022) AI-post share rose ~5.7× vs. the pre-ChatGPT
baseline; firm rankings within SIC 73 (software/business services)
recover the recognizable AI-forward names.

## Diagnostic 2 — Tobin's Q on AI sentiment (staggered build-up)

Question: does firm-level AI sentiment predict next-year firm value? We
build up the specification one layer at a time so we can see what each
layer absorbs, instead of reporting only the FE-saturated version.

Spec ladder (all on the same sample, SEs clustered by firm):

1. Bivariate pooled OLS: `ln(Q)_{t+1} ~ AI sentiment`
2. + Year FE
3. + Standard controls (`ln_at`, `leverage`, `rnd_int`, `profit_marg`)
4. + Firm FE (saturated)

Run as a three-way bracket to gauge measurement-error attenuation:
- **Full sample** — every LinkedIn URL we have a post for, including
  unvalidated ones
- **Strong-match (strict)** — Revelio confirms name *and* exact/legal-suffix
  company match
- **Strong-match (fuzzy)** — same, but the company match also accepts
  hyphen/space/abbreviation variants (SequenceMatcher ratio ≥ 0.80)

If the AI-sentiment coefficient grows as we tighten validation, the full
sample was attenuated by wrong-person noise — the IV's first-stage signal
is real.

### Full sample

<!-- DIAG2_FULL_START -->
_Run: 20260427_162018 · panel: `company_sentiment_annual_20260427_160543.csv` · funda: `funda_20260425_135322.csv` · min_ai_posts=3 · lead=1_

| Regressor | Layer | β | SE | t | p | N | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| `ai_mean_net_sentiment` | (1) Pooled OLS | 0.0013 | 0.0013 | 0.99 | 0.324 | 1,888 | 0.0008 |
| `ai_mean_net_sentiment` | (2) + Year FE | 0.0021 | 0.0013 | 1.61 | 0.107 | 1,888 | 0.0022 |
| `ai_mean_net_sentiment` | (3) + Controls | 0.0015 | 0.0011 | 1.42 | 0.155 | 1,888 | 0.3005 |
| `ai_mean_net_sentiment` | (4) + Firm FE (saturated) | -0.0006 | 0.0008 | -0.70 | 0.485 | 1,888 | 0.1398 |
| `ai_post_share` | (1) Pooled OLS | 0.5994** | 0.2489 | 2.41 | 0.016 | 1,888 | 0.0066 |
| `ai_post_share` | (2) + Year FE | 0.8391*** | 0.2831 | 2.96 | 0.003 | 1,888 | 0.0121 |
| `ai_post_share` | (3) + Controls | 0.2519 | 0.2201 | 1.14 | 0.253 | 1,888 | 0.3005 |
| `ai_post_share` | (4) + Firm FE (saturated) | -0.0554 | 0.1805 | -0.31 | 0.759 | 1,888 | 0.1388 |

Significance: * p<0.10, ** p<0.05, *** p<0.01.
<!-- DIAG2_FULL_END -->

### Strong-match (strict)

<!-- DIAG2_STRONG_START -->
_Run: 20260427_162019 · panel: `company_sentiment_annual_20260427_160543.csv` · funda: `funda_20260425_135322.csv` · min_ai_posts=3 · lead=1_

| Regressor | Layer | β | SE | t | p | N | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| `ai_mean_net_sentiment` | (1) Pooled OLS | -0.0000 | 0.0017 | -0.01 | 0.993 | 1,280 | 0.0000 |
| `ai_mean_net_sentiment` | (2) + Year FE | 0.0009 | 0.0017 | 0.53 | 0.597 | 1,280 | 0.0004 |
| `ai_mean_net_sentiment` | (3) + Controls | 0.0007 | 0.0014 | 0.47 | 0.635 | 1,280 | 0.2993 |
| `ai_mean_net_sentiment` | (4) + Firm FE (saturated) | -0.0011 | 0.0011 | -0.99 | 0.322 | 1,280 | 0.1142 |
| `ai_post_share` | (1) Pooled OLS | 0.6876** | 0.3195 | 2.15 | 0.032 | 1,280 | 0.0076 |
| `ai_post_share` | (2) + Year FE | 1.0124*** | 0.3677 | 2.75 | 0.006 | 1,280 | 0.0151 |
| `ai_post_share` | (3) + Controls | 0.4198 | 0.2714 | 1.55 | 0.122 | 1,280 | 0.3017 |
| `ai_post_share` | (4) + Firm FE (saturated) | -0.1297 | 0.2508 | -0.52 | 0.605 | 1,280 | 0.1122 |

Significance: * p<0.10, ** p<0.05, *** p<0.01.
<!-- DIAG2_STRONG_END -->

### Strong-match (fuzzy)

<!-- DIAG2_STRONG_FUZZY_START -->
_Run: 20260427_162020 · panel: `company_sentiment_annual_20260427_160543.csv` · funda: `funda_20260425_135322.csv` · min_ai_posts=3 · lead=1_

| Regressor | Layer | β | SE | t | p | N | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| `ai_mean_net_sentiment` | (1) Pooled OLS | 0.0006 | 0.0017 | 0.38 | 0.708 | 1,311 | 0.0002 |
| `ai_mean_net_sentiment` | (2) + Year FE | 0.0016 | 0.0017 | 0.92 | 0.360 | 1,311 | 0.0011 |
| `ai_mean_net_sentiment` | (3) + Controls | 0.0012 | 0.0014 | 0.88 | 0.379 | 1,311 | 0.3033 |
| `ai_mean_net_sentiment` | (4) + Firm FE (saturated) | -0.0011 | 0.0010 | -1.04 | 0.300 | 1,311 | 0.1128 |
| `ai_post_share` | (1) Pooled OLS | 0.6216** | 0.3156 | 1.97 | 0.049 | 1,311 | 0.0062 |
| `ai_post_share` | (2) + Year FE | 0.9101** | 0.3613 | 2.52 | 0.012 | 1,311 | 0.0124 |
| `ai_post_share` | (3) + Controls | 0.3435 | 0.2641 | 1.30 | 0.194 | 1,311 | 0.3044 |
| `ai_post_share` | (4) + Firm FE (saturated) | -0.1122 | 0.2481 | -0.45 | 0.651 | 1,311 | 0.1108 |

Significance: * p<0.10, ** p<0.05, *** p<0.01.
<!-- DIAG2_STRONG_FUZZY_END -->

## Diagnostic 3 — Firm performance on general LinkedIn sentiment (match-quality metric)

Per Nick Bloom: *"when firms are doing well their execs are positive on
LinkedIn"*. The strength of this correlation is a match-quality metric —
if the strong-match-only coefficient is meaningfully larger than the full-
sample coefficient, wrong-person noise was attenuating the signal and our
matches are doing real work.

Same staggered ladder, same 3-way sample bracket, but with general
sentiment regressors (`mean_net_sentiment`, `engagement_wtd_sentiment`,
`role_wtd_sentiment`) and firm-performance outcomes.

### Sales growth — Δlog(sale)_{t+1}

#### Full sample
<!-- DIAG3_SALES_FULL_START -->
_Run: 20260428_074104 · outcome: `sales_growth` · panel: `company_sentiment_annual_20260427_160543.csv` · funda: `funda_20260425_135322.csv` · min_posts=10 · lead=1_

| Regressor | Layer | β | SE | t | p | N | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| `mean_net_sentiment` | (1) Pooled OLS | -0.0004*** | 0.0002 | -2.80 | 0.005 | 10,719 | 0.0008 |
| `mean_net_sentiment` | (2) + Year FE | -0.0000 | 0.0002 | -0.29 | 0.774 | 10,719 | 0.0000 |
| `mean_net_sentiment` | (3) + Controls | -0.0000 | 0.0002 | -0.32 | 0.752 | 10,719 | 0.0119 |
| `mean_net_sentiment` | (4) + Firm FE (saturated) | 0.0002 | 0.0002 | 0.85 | 0.394 | 10,719 | 0.0481 |
| `engagement_wtd_sentiment` | (1) Pooled OLS | -0.0003** | 0.0001 | -2.22 | 0.026 | 10,719 | 0.0005 |
| `engagement_wtd_sentiment` | (2) + Year FE | -0.0000 | 0.0001 | -0.40 | 0.692 | 10,719 | 0.0000 |
| `engagement_wtd_sentiment` | (3) + Controls | -0.0001 | 0.0001 | -0.55 | 0.579 | 10,719 | 0.0119 |
| `engagement_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0000 | 0.0001 | 0.34 | 0.732 | 10,719 | 0.0481 |
| `role_wtd_sentiment` | (1) Pooled OLS | -0.0004*** | 0.0001 | -2.87 | 0.004 | 10,719 | 0.0008 |
| `role_wtd_sentiment` | (2) + Year FE | -0.0001 | 0.0001 | -0.37 | 0.709 | 10,719 | 0.0000 |
| `role_wtd_sentiment` | (3) + Controls | -0.0001 | 0.0001 | -0.40 | 0.687 | 10,719 | 0.0119 |
| `role_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0001 | 0.0002 | 0.80 | 0.421 | 10,719 | 0.0481 |

Significance: * p<0.10, ** p<0.05, *** p<0.01.
<!-- DIAG3_SALES_FULL_END -->

#### Strong-match (strict)
<!-- DIAG3_SALES_STRONG_START -->
_Run: 20260428_074105 · outcome: `sales_growth` · panel: `company_sentiment_annual_20260427_160543.csv` · funda: `funda_20260425_135322.csv` · min_posts=10 · lead=1_

| Regressor | Layer | β | SE | t | p | N | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| `mean_net_sentiment` | (1) Pooled OLS | -0.0005** | 0.0002 | -2.35 | 0.019 | 6,933 | 0.0009 |
| `mean_net_sentiment` | (2) + Year FE | 0.0000 | 0.0002 | 0.07 | 0.943 | 6,933 | 0.0000 |
| `mean_net_sentiment` | (3) + Controls | 0.0000 | 0.0002 | 0.18 | 0.858 | 6,933 | 0.0124 |
| `mean_net_sentiment` | (4) + Firm FE (saturated) | 0.0003 | 0.0002 | 1.12 | 0.264 | 6,933 | 0.0548 |
| `engagement_wtd_sentiment` | (1) Pooled OLS | -0.0004** | 0.0002 | -2.38 | 0.018 | 6,933 | 0.0008 |
| `engagement_wtd_sentiment` | (2) + Year FE | -0.0001 | 0.0001 | -0.57 | 0.572 | 6,933 | 0.0000 |
| `engagement_wtd_sentiment` | (3) + Controls | -0.0001 | 0.0001 | -0.61 | 0.541 | 6,933 | 0.0124 |
| `engagement_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0001 | 0.0002 | 0.66 | 0.508 | 6,933 | 0.0548 |
| `role_wtd_sentiment` | (1) Pooled OLS | -0.0004** | 0.0002 | -2.30 | 0.022 | 6,933 | 0.0009 |
| `role_wtd_sentiment` | (2) + Year FE | 0.0000 | 0.0002 | 0.17 | 0.864 | 6,933 | 0.0000 |
| `role_wtd_sentiment` | (3) + Controls | 0.0001 | 0.0002 | 0.29 | 0.773 | 6,933 | 0.0124 |
| `role_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0003 | 0.0002 | 1.24 | 0.214 | 6,933 | 0.0548 |

Significance: * p<0.10, ** p<0.05, *** p<0.01.
<!-- DIAG3_SALES_STRONG_END -->

#### Strong-match (fuzzy)
<!-- DIAG3_SALES_STRONG_FUZZY_START -->
_Run: 20260428_074107 · outcome: `sales_growth` · panel: `company_sentiment_annual_20260427_160543.csv` · funda: `funda_20260425_135322.csv` · min_posts=10 · lead=1_

| Regressor | Layer | β | SE | t | p | N | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| `mean_net_sentiment` | (1) Pooled OLS | -0.0005*** | 0.0002 | -2.58 | 0.010 | 7,125 | 0.0011 |
| `mean_net_sentiment` | (2) + Year FE | -0.0000 | 0.0002 | -0.07 | 0.940 | 7,125 | 0.0000 |
| `mean_net_sentiment` | (3) + Controls | 0.0000 | 0.0002 | 0.06 | 0.952 | 7,125 | 0.0127 |
| `mean_net_sentiment` | (4) + Firm FE (saturated) | 0.0003 | 0.0002 | 1.08 | 0.278 | 7,125 | 0.0546 |
| `engagement_wtd_sentiment` | (1) Pooled OLS | -0.0004** | 0.0001 | -2.51 | 0.012 | 7,125 | 0.0009 |
| `engagement_wtd_sentiment` | (2) + Year FE | -0.0001 | 0.0001 | -0.62 | 0.537 | 7,125 | 0.0001 |
| `engagement_wtd_sentiment` | (3) + Controls | -0.0001 | 0.0001 | -0.65 | 0.514 | 7,125 | 0.0127 |
| `engagement_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0001 | 0.0002 | 0.74 | 0.461 | 7,125 | 0.0546 |
| `role_wtd_sentiment` | (1) Pooled OLS | -0.0005** | 0.0002 | -2.54 | 0.011 | 7,125 | 0.0010 |
| `role_wtd_sentiment` | (2) + Year FE | 0.0000 | 0.0002 | 0.01 | 0.993 | 7,125 | 0.0000 |
| `role_wtd_sentiment` | (3) + Controls | 0.0000 | 0.0002 | 0.16 | 0.873 | 7,125 | 0.0127 |
| `role_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0003 | 0.0002 | 1.18 | 0.239 | 7,125 | 0.0546 |

Significance: * p<0.10, ** p<0.05, *** p<0.01.
<!-- DIAG3_SALES_STRONG_FUZZY_END -->

### ROA — ni / at_{t+1}

#### Full sample
<!-- DIAG3_ROA_FULL_START -->
_Run: 20260428_074358 · outcome: `roa` · panel: `company_sentiment_annual_20260427_160543.csv` · funda: `funda_20260425_135322.csv` · min_posts=10 · lead=1_

| Regressor | Layer | β | SE | t | p | N | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| `mean_net_sentiment` | (1) Pooled OLS | 0.0002*** | 0.0001 | 2.62 | 0.009 | 10,721 | 0.0011 |
| `mean_net_sentiment` | (2) + Year FE | 0.0002** | 0.0001 | 2.36 | 0.018 | 10,721 | 0.0009 |
| `mean_net_sentiment` | (3) + Controls | 0.0003*** | 0.0001 | 3.36 | 0.001 | 10,721 | 0.1972 |
| `mean_net_sentiment` | (4) + Firm FE (saturated) | 0.0001 | 0.0001 | 1.44 | 0.151 | 10,721 | 0.0437 |
| `engagement_wtd_sentiment` | (1) Pooled OLS | 0.0001* | 0.0001 | 1.69 | 0.091 | 10,721 | 0.0004 |
| `engagement_wtd_sentiment` | (2) + Year FE | 0.0001* | 0.0001 | 1.85 | 0.064 | 10,721 | 0.0005 |
| `engagement_wtd_sentiment` | (3) + Controls | 0.0001** | 0.0001 | 2.39 | 0.017 | 10,721 | 0.1965 |
| `engagement_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0000 | 0.0001 | 0.68 | 0.498 | 10,721 | 0.0433 |
| `role_wtd_sentiment` | (1) Pooled OLS | 0.0002*** | 0.0001 | 2.77 | 0.006 | 10,721 | 0.0013 |
| `role_wtd_sentiment` | (2) + Year FE | 0.0002** | 0.0001 | 2.51 | 0.012 | 10,721 | 0.0010 |
| `role_wtd_sentiment` | (3) + Controls | 0.0003*** | 0.0001 | 3.56 | 0.000 | 10,721 | 0.1974 |
| `role_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0001* | 0.0001 | 1.67 | 0.096 | 10,721 | 0.0438 |

Significance: * p<0.10, ** p<0.05, *** p<0.01.
<!-- DIAG3_ROA_FULL_END -->

#### Strong-match (strict)
<!-- DIAG3_ROA_STRONG_START -->
_Run: 20260428_074359 · outcome: `roa` · panel: `company_sentiment_annual_20260427_160543.csv` · funda: `funda_20260425_135322.csv` · min_posts=10 · lead=1_

| Regressor | Layer | β | SE | t | p | N | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| `mean_net_sentiment` | (1) Pooled OLS | 0.0003** | 0.0001 | 2.04 | 0.041 | 6,933 | 0.0011 |
| `mean_net_sentiment` | (2) + Year FE | 0.0002* | 0.0001 | 1.89 | 0.059 | 6,933 | 0.0010 |
| `mean_net_sentiment` | (3) + Controls | 0.0003*** | 0.0001 | 2.87 | 0.004 | 6,933 | 0.2398 |
| `mean_net_sentiment` | (4) + Firm FE (saturated) | 0.0002 | 0.0001 | 1.57 | 0.117 | 6,933 | 0.0486 |
| `engagement_wtd_sentiment` | (1) Pooled OLS | 0.0001 | 0.0001 | 1.44 | 0.151 | 6,933 | 0.0005 |
| `engagement_wtd_sentiment` | (2) + Year FE | 0.0002 | 0.0001 | 1.63 | 0.102 | 6,933 | 0.0006 |
| `engagement_wtd_sentiment` | (3) + Controls | 0.0002** | 0.0001 | 2.27 | 0.023 | 6,933 | 0.2393 |
| `engagement_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0001 | 0.0001 | 0.78 | 0.433 | 6,933 | 0.0487 |
| `role_wtd_sentiment` | (1) Pooled OLS | 0.0003** | 0.0001 | 2.06 | 0.039 | 6,933 | 0.0011 |
| `role_wtd_sentiment` | (2) + Year FE | 0.0002* | 0.0001 | 1.91 | 0.056 | 6,933 | 0.0010 |
| `role_wtd_sentiment` | (3) + Controls | 0.0003*** | 0.0001 | 2.98 | 0.003 | 6,933 | 0.2399 |
| `role_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0002* | 0.0001 | 1.76 | 0.078 | 6,933 | 0.0487 |

Significance: * p<0.10, ** p<0.05, *** p<0.01.
<!-- DIAG3_ROA_STRONG_END -->

#### Strong-match (fuzzy)
<!-- DIAG3_ROA_STRONG_FUZZY_START -->
_Run: 20260428_074401 · outcome: `roa` · panel: `company_sentiment_annual_20260427_160543.csv` · funda: `funda_20260425_135322.csv` · min_posts=10 · lead=1_

| Regressor | Layer | β | SE | t | p | N | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| `mean_net_sentiment` | (1) Pooled OLS | 0.0003** | 0.0001 | 2.14 | 0.032 | 7,125 | 0.0012 |
| `mean_net_sentiment` | (2) + Year FE | 0.0003** | 0.0001 | 1.99 | 0.046 | 7,125 | 0.0011 |
| `mean_net_sentiment` | (3) + Controls | 0.0003*** | 0.0001 | 2.97 | 0.003 | 7,125 | 0.2384 |
| `mean_net_sentiment` | (4) + Firm FE (saturated) | 0.0002* | 0.0001 | 1.72 | 0.085 | 7,125 | 0.0495 |
| `engagement_wtd_sentiment` | (1) Pooled OLS | 0.0001 | 0.0001 | 1.44 | 0.149 | 7,125 | 0.0005 |
| `engagement_wtd_sentiment` | (2) + Year FE | 0.0002 | 0.0001 | 1.64 | 0.100 | 7,125 | 0.0006 |
| `engagement_wtd_sentiment` | (3) + Controls | 0.0002** | 0.0001 | 2.24 | 0.025 | 7,125 | 0.2378 |
| `engagement_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0001 | 0.0001 | 0.81 | 0.415 | 7,125 | 0.0495 |
| `role_wtd_sentiment` | (1) Pooled OLS | 0.0003** | 0.0001 | 2.19 | 0.029 | 7,125 | 0.0012 |
| `role_wtd_sentiment` | (2) + Year FE | 0.0003** | 0.0001 | 2.05 | 0.040 | 7,125 | 0.0011 |
| `role_wtd_sentiment` | (3) + Controls | 0.0003*** | 0.0001 | 3.09 | 0.002 | 7,125 | 0.2385 |
| `role_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0002* | 0.0001 | 1.91 | 0.056 | 7,125 | 0.0496 |

Significance: * p<0.10, ** p<0.05, *** p<0.01.
<!-- DIAG3_ROA_STRONG_FUZZY_END -->

### Stock return — annual buy-and-hold_{t+1}

#### Full sample
<!-- DIAG3_RETURN_FULL_START -->
_Run: 20260428_132757 · outcome: `stock_return` · panel: `company_sentiment_annual_20260427_160543.csv` · funda: `funda_20260425_135322.csv` · min_posts=10 · lead=1_

| Regressor | Layer | β | SE | t | p | N | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| `mean_net_sentiment` | (1) Pooled OLS | -0.0003 | 0.0003 | -1.01 | 0.312 | 9,235 | 0.0001 |
| `mean_net_sentiment` | (2) + Year FE | 0.0000 | 0.0003 | 0.13 | 0.893 | 9,235 | 0.0000 |
| `mean_net_sentiment` | (3) + Controls | 0.0000 | 0.0003 | 0.11 | 0.912 | 9,235 | 0.0068 |
| `mean_net_sentiment` | (4) + Firm FE (saturated) | 0.0000 | 0.0004 | 0.06 | 0.950 | 9,235 | 0.0366 |
| `engagement_wtd_sentiment` | (1) Pooled OLS | -0.0005** | 0.0003 | -1.97 | 0.049 | 9,235 | 0.0004 |
| `engagement_wtd_sentiment` | (2) + Year FE | -0.0003 | 0.0003 | -1.24 | 0.214 | 9,235 | 0.0002 |
| `engagement_wtd_sentiment` | (3) + Controls | -0.0003 | 0.0003 | -1.37 | 0.172 | 9,235 | 0.0070 |
| `engagement_wtd_sentiment` | (4) + Firm FE (saturated) | -0.0004 | 0.0003 | -1.21 | 0.227 | 9,235 | 0.0368 |
| `role_wtd_sentiment` | (1) Pooled OLS | -0.0003 | 0.0003 | -1.02 | 0.307 | 9,235 | 0.0001 |
| `role_wtd_sentiment` | (2) + Year FE | 0.0000 | 0.0003 | 0.06 | 0.955 | 9,235 | 0.0000 |
| `role_wtd_sentiment` | (3) + Controls | 0.0000 | 0.0003 | 0.02 | 0.981 | 9,235 | 0.0068 |
| `role_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0000 | 0.0004 | 0.05 | 0.963 | 9,235 | 0.0366 |

Significance: * p<0.10, ** p<0.05, *** p<0.01.
<!-- DIAG3_RETURN_FULL_END -->

#### Strong-match (strict)
<!-- DIAG3_RETURN_STRONG_START -->
_Run: 20260428_132759 · outcome: `stock_return` · panel: `company_sentiment_annual_20260427_160543.csv` · funda: `funda_20260425_135322.csv` · min_posts=10 · lead=1_

| Regressor | Layer | β | SE | t | p | N | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| `mean_net_sentiment` | (1) Pooled OLS | -0.0004 | 0.0004 | -0.93 | 0.352 | 5,956 | 0.0001 |
| `mean_net_sentiment` | (2) + Year FE | 0.0000 | 0.0004 | 0.02 | 0.986 | 5,956 | 0.0000 |
| `mean_net_sentiment` | (3) + Controls | 0.0000 | 0.0004 | 0.11 | 0.909 | 5,956 | 0.0061 |
| `mean_net_sentiment` | (4) + Firm FE (saturated) | -0.0000 | 0.0006 | -0.01 | 0.996 | 5,956 | 0.0451 |
| `engagement_wtd_sentiment` | (1) Pooled OLS | -0.0007** | 0.0003 | -1.98 | 0.048 | 5,956 | 0.0006 |
| `engagement_wtd_sentiment` | (2) + Year FE | -0.0004 | 0.0003 | -1.36 | 0.174 | 5,956 | 0.0003 |
| `engagement_wtd_sentiment` | (3) + Controls | -0.0004 | 0.0003 | -1.35 | 0.177 | 5,956 | 0.0064 |
| `engagement_wtd_sentiment` | (4) + Firm FE (saturated) | -0.0004 | 0.0004 | -0.99 | 0.324 | 5,956 | 0.0453 |
| `role_wtd_sentiment` | (1) Pooled OLS | -0.0003 | 0.0004 | -0.82 | 0.411 | 5,956 | 0.0001 |
| `role_wtd_sentiment` | (2) + Year FE | 0.0000 | 0.0004 | 0.04 | 0.966 | 5,956 | 0.0000 |
| `role_wtd_sentiment` | (3) + Controls | 0.0000 | 0.0004 | 0.12 | 0.901 | 5,956 | 0.0061 |
| `role_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0001 | 0.0006 | 0.16 | 0.873 | 5,956 | 0.0452 |

Significance: * p<0.10, ** p<0.05, *** p<0.01.
<!-- DIAG3_RETURN_STRONG_END -->

#### Strong-match (fuzzy)
<!-- DIAG3_RETURN_STRONG_FUZZY_START -->
_Run: 20260428_132800 · outcome: `stock_return` · panel: `company_sentiment_annual_20260427_160543.csv` · funda: `funda_20260425_135322.csv` · min_posts=10 · lead=1_

| Regressor | Layer | β | SE | t | p | N | R² |
|---|---|---:|---:|---:|---:|---:|---:|
| `mean_net_sentiment` | (1) Pooled OLS | -0.0004 | 0.0004 | -1.05 | 0.295 | 6,117 | 0.0002 |
| `mean_net_sentiment` | (2) + Year FE | -0.0000 | 0.0004 | -0.06 | 0.949 | 6,117 | 0.0000 |
| `mean_net_sentiment` | (3) + Controls | 0.0000 | 0.0004 | 0.05 | 0.957 | 6,117 | 0.0059 |
| `mean_net_sentiment` | (4) + Firm FE (saturated) | 0.0000 | 0.0006 | 0.01 | 0.994 | 6,117 | 0.0448 |
| `engagement_wtd_sentiment` | (1) Pooled OLS | -0.0007** | 0.0003 | -2.00 | 0.046 | 6,117 | 0.0006 |
| `engagement_wtd_sentiment` | (2) + Year FE | -0.0004 | 0.0003 | -1.36 | 0.174 | 6,117 | 0.0003 |
| `engagement_wtd_sentiment` | (3) + Controls | -0.0004 | 0.0003 | -1.36 | 0.175 | 6,117 | 0.0062 |
| `engagement_wtd_sentiment` | (4) + Firm FE (saturated) | -0.0004 | 0.0004 | -0.86 | 0.392 | 6,117 | 0.0449 |
| `role_wtd_sentiment` | (1) Pooled OLS | -0.0004 | 0.0004 | -0.91 | 0.364 | 6,117 | 0.0001 |
| `role_wtd_sentiment` | (2) + Year FE | -0.0000 | 0.0004 | -0.03 | 0.978 | 6,117 | 0.0000 |
| `role_wtd_sentiment` | (3) + Controls | 0.0000 | 0.0004 | 0.08 | 0.938 | 6,117 | 0.0059 |
| `role_wtd_sentiment` | (4) + Firm FE (saturated) | 0.0001 | 0.0006 | 0.19 | 0.846 | 6,117 | 0.0449 |

Significance: * p<0.10, ** p<0.05, *** p<0.01.
<!-- DIAG3_RETURN_STRONG_FUZZY_END -->

## Diagnostic 4 — 10-K AI mentions vs LinkedIn AI sentiment (convergent validity)

Per Nick: 10-K AI mentions are a separate "AI awareness" signal — the firm's
own official disclosures, independent of what executives post on LinkedIn.
If our LinkedIn AI sentiment measure is real, it should correlate with the
firm's 10-K mentions of the same keyword set, since both proxy the same
underlying construct (whether the firm thinks/talks about AI).

**Method**: same AI keyword regex (`src/data_analysis/sentiment_analysis_full.py`
lines 62-68) applied to:
- LinkedIn posts → `n_ai_posts / n_posts` per firm-year (LinkedIn AI share)
- 10-K filings (`wrds_sec_search.filing_10_k`) → `n_ai_mentions / n_words`
  per firm-year (10-K AI mention rate)

**Universe**: all firms in our LinkedIn sample with at least one 10-K filing
in 2018-2024 and ≥10 LinkedIn posts.

### Firm-level pooled correlation

<!-- DIAG4_CORR_START -->
_Run: 20260429_202831 · 10-K: `tenk_ai_mentions_20260429_165404.csv` · LI: `company_sentiment_annual_20260427_160543.csv`_

| Metric | Value |
|---|---:|
| Firms (n) | 2,042 |
| Pearson ρ  | 0.365 |
| Spearman ρ | 0.392 |

<!-- DIAG4_CORR_END -->

### Top firms by 10-K AI mention rate

<!-- DIAG4_TOP10K_START -->
| Firm | Ticker | 10-Ks | Total words | AI mentions | Per 1K words | LI AI share |
|---|---|---:|---:|---:|---:|---:|
| Progenics Pharmaceutical | PGNX | 3 | 13,857 | 36 | 2.60 | 0.022 |
| Uipath | PATH | 3 | 207,087 | 500 | 2.41 | 0.166 |
| Nvidia | NVDA | 7 | 327,633 | 696 | 2.12 | 0.191 |
| Liveperson | LPSN | 7 | 522,849 | 900 | 1.72 | 0.192 |
| Control4 | CTRL | 2 | 110,423 | 186 | 1.68 | 0.035 |
| Rockwell Automation | ROK | 7 | 345,102 | 566 | 1.64 | 0.244 |
| Teradyne | TER | 7 | 397,596 | 617 | 1.55 | 0.444 |
| Ceva | CEVA | 7 | 395,671 | 586 | 1.48 | 0.405 |
| Omnicell | OMCL | 7 | 461,976 | 682 | 1.48 | 0.090 |
| Helix Energy Solutions Group | HLX | 7 | 364,873 | 501 | 1.37 | 0.077 |
| Emerson Electric | EMR | 7 | 285,179 | 386 | 1.35 | 0.104 |
| Sprinklr | CXM | 3 | 213,310 | 288 | 1.35 | 0.142 |
| Ciena | CIEN | 7 | 508,488 | 679 | 1.34 | 0.170 |
| Azenta | AZTA | 7 | 405,153 | 528 | 1.30 | 0.021 |
| Microsoft | MSFT | 7 | 367,401 | 405 | 1.10 | 0.241 |
<!-- DIAG4_TOP10K_END -->

