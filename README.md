# AD 5sigma experiments

This repository snapshots the 5sigma anomaly-detection experiments for the CCF OpenAIOps challenge.

## Current submitted variant

- Variant: traffic-only frozen 20min baseline, 5sigma, global gap 10min
- Submission ID: 1789157445777
- Total score: 5.975538419667967
- AD score: 4.482387734736461
- Records: 302

The generated JSONL outputs are intentionally not included in this public snapshot.

## Key files

- `bian_mas/implementation/anomaly_detector/five_sigma.py`: frozen baseline 5sigma implementation.
- `tools/run_frozen20_multisource_ad.py`: generate frozen-baseline AD windows with tunable gaps.
- `tools/compare_frozen20_fusion_strategies.py`: compare traffic-anchor and per-source fusion strategies.
- `tools/compare_continuous_stage1.py`: controlled continuous-metric preprocessing experiments.
- `tools/test_continuous_stage1.py`: counter, zero-variance, and compatibility checks.

## Stage 1 continuous-metric experiments

These versions have not been submitted. They hold the baseline at 20 minutes,
metric gap at 5 minutes, global gap at 3 minutes, traffic sigma at 5 and node
sigma at 6. A preserves the original detector; B converts cumulative counters
to per-second rates; C adds explicit effect thresholds for zero-variance
baselines; D separates traffic series identities and removes duplicate samples.
Missing samples and counter resets are not interpreted as zero-valued traffic.
Additional node sigma 7/8 and node weight 0.3 variants test sensitivity separately.

Run the full dataset comparison without submitting:

```powershell
python tools/compare_continuous_stage1.py --data-root /path/to/phaseone_data
python tools/test_continuous_stage1.py
```

Public configuration dependencies are included. The optional
`--prepare-submissions` switch requires the original workspace's submission
format helpers, which are not part of this AD-only snapshot.
Generated candidate files and raw competition data are not published.

## Traffic baseline controls

The C traffic reference (AD 9.535391740361836) is reproduced exactly before
comparing a rolling 69-minute historical baseline and a 14-day/292-block
whole-block baseline. Sigma, metric/global gaps and aggregation remain fixed.
Whole-block statistics use future samples and are an offline experiment.
Blocks do not force an event or prevent cross-boundary aggregation.
New variants have not been submitted.

```powershell
python tools/compare_traffic_baselines.py --data-root /path/to/phaseone_data
python tools/test_traffic_baselines.py
```

This comparison expects the original C window file under
`outputs/continuous_stage1/C_zero_variance_traffic_windows.jsonl` from stage 1.
See `continuous_stage1_report.md` for local counts and interpretation.

## Reproduce the current AD window file locally

```powershell
python tools/run_frozen20_multisource_ad.py --data-root "D:\组\大创\CCFOpenAIOps挑战赛\phaseone_data" --sources traffic --limit 500 --metric-gap-minutes 5 --global-gap-minutes 10 --output outputs\gap_compare\traffic5_globalgap10_windows.jsonl
```
