import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams

# Set publication-quality style parameters
rcParams['font.family'] = 'serif'
rcParams['font.serif'] = ['Times New Roman', 'Times', 'DejaVu Serif']
rcParams['font.size'] = 12
rcParams['axes.labelsize'] = 14
rcParams['axes.titlesize'] = 16
rcParams['xtick.labelsize'] = 12
rcParams['ytick.labelsize'] = 12
rcParams['legend.fontsize'] = 12
rcParams['figure.titlesize'] = 18

# Sample data
categories = ['Method A', 'Method B', 'Method C', 'Method D', 'Method E']
values = [23.5, 45.2, 38.7, 52.1, 29.8]
errors = [2.1, 3.4, 2.8, 3.9, 2.5]  # Error bars

# Create figure with specific size for publication (adjust as needed)
fig, ax = plt.subplots(figsize=(10, 6), dpi=300)

# Create bars with custom colors
colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#5D737E']
bars = ax.bar(categories, values, color=colors, alpha=0.8, 
              edgecolor='black', linewidth=1.2, capsize=5)

# Add error bars
ax.errorbar(categories, values, yerr=errors, fmt='none', 
            color='black', capsize=4, capthick=1.5, elinewidth=1.5)

# Customize the plot
ax.set_xlabel('Experimental Conditions', fontweight='bold')
ax.set_ylabel('Performance Metric (%)', fontweight='bold')
ax.set_title('Comparative Analysis of Different Methods', fontweight='bold', pad=20)

# Set y-axis limits with some padding
y_max = max([v + e for v, e in zip(values, errors)])
ax.set_ylim(0, y_max * 1.1)

# Add grid for better readability
ax.grid(axis='y', alpha=0.3, linestyle='-', linewidth=0.5)
ax.set_axisbelow(True)  # Put grid behind bars

# Customize spines (borders)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_linewidth(1.2)
ax.spines['bottom'].set_linewidth(1.2)

# Add value labels on top of bars
for bar, value, error in zip(bars, values, errors):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height + error + 0.5,
            f'{value:.1f}', ha='center', va='bottom', fontweight='bold')

# Rotate x-axis labels if needed (uncomment if labels are long)
# plt.xticks(rotation=45, ha='right')

# Adjust layout to prevent label cutoff
plt.tight_layout()

# Save as high-quality formats
# plt.savefig('publication_barchart.png', dpi=300, bbox_inches='tight', 
#             facecolor='white', edgecolor='none')
# plt.savefig('publication_barchart.pdf', bbox_inches='tight', 
#             facecolor='white', edgecolor='none')
# plt.savefig('publication_barchart.eps', bbox_inches='tight', 
#             facecolor='white', edgecolor='none')

plt.show()

# Alternative: Using seaborn style for even cleaner look
# import seaborn as sns
# plt.style.use('seaborn-v0_8-whitegrid')  # or just 'seaborn' in older versions

# Advanced customization example with grouped bars
def create_grouped_barchart():
    """Create a grouped bar chart for comparing multiple datasets"""
    
    # Sample data for grouped bars
    methods = ['Method A', 'Method B', 'Method C', 'Method D']
    dataset1 = [23.5, 45.2, 38.7, 52.1]
    dataset2 = [26.8, 42.1, 41.3, 48.9]
    dataset3 = [21.2, 47.8, 35.4, 54.3]
    
    x = np.arange(len(methods))
    width = 0.25
    
    fig, ax = plt.subplots(figsize=(12, 7), dpi=300)
    
    # Create grouped bars
    bars1 = ax.bar(x - width, dataset1, width, label='Dataset 1', 
                   color='#2E86AB', alpha=0.8, edgecolor='black', linewidth=1)
    bars2 = ax.bar(x, dataset2, width, label='Dataset 2', 
                   color='#A23B72', alpha=0.8, edgecolor='black', linewidth=1)
    bars3 = ax.bar(x + width, dataset3, width, label='Dataset 3', 
                   color='#F18F01', alpha=0.8, edgecolor='black', linewidth=1)
    
    # Customize
    ax.set_xlabel('Methods', fontweight='bold')
    ax.set_ylabel('Performance Score', fontweight='bold')
    ax.set_title('Comparative Performance Analysis', fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(methods)
    
    # Add legend
    ax.legend(frameon=True, fancybox=True, shadow=True, loc='upper left')
    
    # Grid and spines
    ax.grid(axis='y', alpha=0.3, linestyle='-', linewidth=0.5)
    ax.set_axisbelow(True)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.show()

# Uncomment to run the grouped bar chart example
# create_grouped_barchart()

# Tips for publication quality:
"""
PUBLICATION TIPS:
1. Use vector formats (PDF, EPS) for scalable graphics
2. Set DPI to 300+ for raster formats (PNG, JPG)
3. Use consistent fonts throughout your document
4. Keep color schemes accessible (consider colorblind-friendly palettes)
5. Include error bars when appropriate
6. Label axes clearly with units
7. Use tight_layout() or bbox_inches='tight' to avoid clipping
8. Consider journal-specific figure size requirements
9. Test how your figure looks in grayscale
10. Keep text readable when scaled down for publication
"""