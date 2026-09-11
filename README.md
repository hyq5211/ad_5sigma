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

## Reproduce the current AD window file locally

```powershell
python tools/run_frozen20_multisource_ad.py --data-root "D:\组\大创\CCFOpenAIOps挑战赛\phaseone_data" --sources traffic --limit 500 --metric-gap-minutes 5 --global-gap-minutes 10 --output outputs\gap_compare\traffic5_globalgap10_windows.jsonl
```
