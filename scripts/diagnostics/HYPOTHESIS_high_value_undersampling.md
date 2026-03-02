# Hypothesis: High-Value Property ($1M+) Negative Growth Bias

## Observation
The current model estimates **negative growth** for properties valued >$1M,
pulling their forecasts toward the population mean. This looks like
mean-reversion bias from undersampling.

## Root Cause Theory
- Each training checkpoint uses only **20K samples**
- **<5% of Harris County properties** are worth >$1M
- At 20K samples, the model sees ~1,000 high-value parcels — not enough
  to learn their distinct appreciation dynamics
- These properties are effectively **out-of-distribution (OOD)**
- The diffusion model's learned prior is dominated by sub-$1M parcels,
  so it regresses outliers back toward the bulk distribution center

## Proposed Experiment: Expanding-Window Retraining Backtest
Test whether the bias shrinks with more training data by running the
backtest with checkpoints retrained at increasing sample sizes:

| Run | Sample Size | Expected $1M+ Coverage | Purpose                        |
|-----|-------------|------------------------|--------------------------------|
| 1   | 20K         | ~1,000 (5%)            | Current baseline               |
| 2   | 40K         | ~2,000 (5%)            | Does doubling help?            |
| 3   | 80K         | ~4,000 (5%)            | Diminishing returns?           |
| 4   | 20K strat.  | ~4,000 (20%)           | Stratified oversample $1M+     |

### Metrics to Track (per value bracket)
- **Accuracy**: Median absolute error by value decile
- **Distributional coverage**: P10–P90 fan hit rate by value decile
- **Growth direction**: % of $1M+ parcels with predicted negative growth
- **Spearman ρ**: Within $1M+ bracket only vs. full population

### Key Question
Is the fix **more data** (runs 2–3) or **stratified sampling** (run 4)?
If the bias shrinks monotonically 1→2→3, undersampling is confirmed.
If run 4 at 20K matches or beats run 3 at 80K for $1M+ accuracy,
then the cheapest fix is stratification, not scaling training data.

## Implementation Notes
- Modify `worldmodel.py` training loop to accept `--sample_size` and
  `--stratify_above` arguments
- Save separate checkpoints per sample-size config: `ckpt_origin_{yr}_N{size}.pt`
- Extend `backtest_entity_screening.py` to loop over checkpoint variants
  and produce a comparison table across sample sizes
