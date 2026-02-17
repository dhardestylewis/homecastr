import matplotlib.pyplot as plt
import numpy as np

# Style similar to the deck
# Bg: F2EDE0
# Fg: 3D3830
# Gold: CA8A04
# Muted: A8A29E

plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']

bg_color = '#F2EDE0'
text_color = '#3D3830'
gold_color = '#CA8A04'
muted_color = '#A8A29E'

def set_style(ax):
    ax.set_facecolor(bg_color)
    for spine in ax.spines.values():
        spine.set_edgecolor(text_color)
        spine.set_linewidth(1.5)
    ax.tick_params(colors=text_color, direction='out', width=1.5)
    ax.xaxis.label.set_color(text_color)
    ax.yaxis.label.set_color(text_color)
    ax.title.set_color(text_color)
    ax.title.set_weight('bold')

# 1. Fan Chart
fig, ax = plt.subplots(figsize=(5, 3.5), facecolor=bg_color)
set_style(ax)

x = np.linspace(0, 48, 100)
# Historical part (imaginary -12 to 0)
x_hist = np.linspace(-12, 0, 25)
y_hist = 300 + 0.5 * x_hist + np.random.normal(0, 2, 25)

# Future paths
np.random.seed(42)
for i in range(150):
    walk = np.cumsum(np.random.normal(0, 1.5, 100))
    trend = 0.8 * x
    y_future = y_hist[-1] + trend + walk
    ax.plot(x, y_future, color=gold_color, alpha=0.05, linewidth=1)

# Solid median
y_median = y_hist[-1] + 0.8 * x
ax.plot(x_hist, y_hist, color=text_color, linewidth=2.5, label='History')
ax.plot(x, y_median, color=gold_color, linewidth=3, label='Forecast')

ax.set_title("Hundreds of Scenarios", fontsize=14, pad=10)
# Clean Axes
ax.set_xticks([])
ax.set_yticks([])
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.spines['bottom'].set_visible(False)

plt.tight_layout()
plt.savefig('scenarios.png', dpi=300, facecolor=bg_color)
plt.close()

# 2. Price Bands (Simplified for consumer understanding)
fig, ax = plt.subplots(figsize=(5, 3.5), facecolor=bg_color)
set_style(ax)

years = [1, 2, 3]
medians = [320, 345, 380]
lows = [305, 315, 330] # 10th percentile
highs = [335, 375, 430] # 90th percentile

# Draw ranges
for y, m, l, h in zip(years, medians, lows, highs):
    # Full range line
    ax.plot([y, y], [l, h], color=muted_color, linewidth=3, alpha=0.5, solid_capstyle='round')
    # Median dot
    ax.scatter(y, m, color=gold_color, s=200, zorder=3, edgecolors='none')
    # Label value
    ax.text(y, m + 8, f"${m}K", ha='center', va='bottom', fontsize=12, fontweight='bold', color=text_color)

ax.set_title("Price Bands", fontsize=14, pad=10)
ax.set_xticks(years)
ax.set_xticklabels(['Year 1', 'Year 2', 'Year 3'], fontsize=12, fontweight='bold')
ax.set_yticks([]) 
ax.set_ylim(280, 460)
ax.spines['left'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_visible(False)

plt.tight_layout()
plt.savefig('pricebands.png', dpi=300, facecolor=bg_color)
plt.close()


# 3. Explainability (Positive Magnitude Drivers)
fig, ax = plt.subplots(figsize=(5, 3.5), facecolor=bg_color)
set_style(ax)

features = ['Market Momentum', 'Local Inventory', 'School Ratings', 'New Develop.']
scores = [85, 65, 45, 30] 
y_pos = np.arange(len(features))

colors_bar = [gold_color for _ in scores]

ax.barh(y_pos, scores, align='center', color=colors_bar, height=0.5)
ax.set_yticks(y_pos)
ax.set_yticklabels(features, fontsize=11, fontweight='bold')
ax.invert_yaxis()  # labels read top-to-bottom
ax.set_xticks([])
ax.set_title("Key Value Drivers", fontsize=14, pad=10)

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.axvline(0, color=text_color, linewidth=1, linestyle='--')

plt.tight_layout()
plt.savefig('explainable.png', dpi=300, facecolor=bg_color)
plt.close()

# 4. Global Background (Subtle Outlines Only - No obvious repeats)
fig, ax = plt.subplots(figsize=(16, 9), facecolor=bg_color)
ax.set_facecolor(bg_color)
ax.axis('off')

from matplotlib.patches import Rectangle
import random

# Recursive splitting function for non-overlapping tiling
def split_rect(x, y, w, h, depth=0):
    if depth > 4 or (w < 1 and h < 1):
        # Mostly empty outlines to create subtle texture without distracting repeats
        if random.random() < 0.98: # 98% are just faint outlines
            color = 'none' 
            alpha = 0
            edge_alpha = random.uniform(0.05, 0.15)
        else:
            # VERY rare subtle fill
            color = gold_color
            alpha = 0.05 
            edge_alpha = 0.15
        
        # Transparent face, thin edge
        rect = Rectangle((x, y), w, h, facecolor=color, edgecolor=text_color, alpha=0.1 if color!='none' else 0, linewidth=0.5)
        outline = Rectangle((x, y), w, h, facecolor='none', edgecolor=text_color, alpha=edge_alpha, linewidth=0.5)
        
        if color != 'none':
             ax.add_patch(Rectangle((x, y), w, h, facecolor=color, edgecolor='none', alpha=alpha))
        
        ax.add_patch(outline)
        return

    # Split horizontally or vertically
    if w > h:
        split = random.uniform(0.3, 0.7) * w
        split_rect(x, y, split, h, depth+1)
        split_rect(x + split, y, w - split, h, depth+1)
    else:
        split = random.uniform(0.3, 0.7) * h
        split_rect(x, y, w, split, depth+1)
        split_rect(x, y + split, w, h - split, depth+1)

random.seed(42) # Consistent pattern
split_rect(0, 0, 16, 9)

ax.set_xlim(0, 16)
ax.set_ylim(0, 9)

plt.savefig('title_bg.png', dpi=300, facecolor=bg_color, bbox_inches='tight', pad_inches=0)
plt.close()
