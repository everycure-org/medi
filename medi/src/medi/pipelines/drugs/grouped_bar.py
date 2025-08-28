import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.patches import Rectangle
import matplotlib.patches as mpatches

# Set publication-quality style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")

def create_grouped_bar_chart(data=None, title="Database Comparison Across Regions", 
                           xlabel="Regions", ylabel="Values", 
                           figsize=(12, 8), save_path=None, dpi=300):
    """
    Creates a publication-quality grouped bar chart with 4 regional groups comparing 3 databases.
    
    Parameters:
    - data: DataFrame with regions as index and databases as columns
    - title: Chart title
    - xlabel, ylabel: Axis labels
    - figsize: Figure size tuple
    - save_path: Path to save the figure (optional)
    - dpi: Resolution for saved figure
    """
    
    # Sample data if none provided
    if data is None:
        data = pd.DataFrame({
            'MeDI': [25, 28, 22, 30],
            'Drug Central': [30, 33, 27, 35], 
            'DrugBank': [35, 38, 32, 40]
        }, index=['USA', 'Europe', 'Japan', 'Overall'])
    
    # Define distinct colors for each database
    database_colors = {
        'MeDI': '#04471C',        # Dark Green
        'Drug Central': '#2B4162',  # Dark Blue  
        'DrugBank': '#AC7B7D'      # Dusty Rose
    }
    
    # Set up the figure
    fig, ax = plt.subplots(figsize=figsize, dpi=100)
    
    # Bar width and positions
    n_groups = len(data.index)  # Number of regions (4)
    n_bars = len(data.columns)  # Number of databases (3)
    bar_width = 0.25  # Width of each bar
    
    # Calculate positions for each bar group
    group_positions = np.arange(n_groups)
    
    # Create bars for each database
    bars = []
    for i, database in enumerate(data.columns):
        # Calculate position offset for this database
        offset = (i - n_bars/2 + 0.5) * bar_width
        positions = group_positions + offset
        
        # Create bars
        bar = ax.bar(positions, data[database], bar_width, 
                    label=database, 
                    color=database_colors[database], 
                    alpha=0.8,
                    edgecolor='white', 
                    linewidth=0.7)
        bars.append(bar)
    
    # Customize the chart
    ax.set_xlabel(xlabel, fontsize=18, fontweight='bold')
    ax.set_ylabel(ylabel, fontsize=18, fontweight='bold')
    ax.set_title(title, fontsize=20, fontweight='bold', pad=20)
    
    # Set x-axis
    ax.set_xticks(group_positions)
    ax.set_xticklabels(data.index, fontsize=22)
    
    # Customize y-axis
    ax.tick_params(axis='y', labelsize=20)
    ax.tick_params(axis='x', labelsize=20)
    
    # Remove grid lines for cleaner look
    ax.grid(False)
    ax.set_axisbelow(True)
    
    # Create legend
    ax.legend(loc='upper left', bbox_to_anchor=(1, 1), 
             fontsize=20, frameon=True, fancybox=True, shadow=True)
    
    # Adjust layout to prevent legend cutoff
    plt.tight_layout()
    
    # Add value labels on bars (optional)
    for bar_group in bars:
        for bar in bar_group:
            height = bar.get_height()
            ax.annotate(f'{height:.0f}',
                       xy=(bar.get_x() + bar.get_width() / 2, height),
                       xytext=(0, 3),  # 3 points vertical offset
                       textcoords="offset points",
                       ha='center', va='bottom', fontsize=16, 
                       alpha=0.8)
    
    # Set y-axis to start from 0
    ax.set_ylim(bottom=0)
    
    # Remove top and right spines for cleaner look
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Save if path provided
    if save_path:
        plt.savefig(save_path, dpi=dpi, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        print(f"Chart saved to: {save_path}")
    
    plt.show()
    return fig, ax

# Alternative version with different color schemes
def create_grouped_bar_chart_custom_colors(data=None, color_scheme='default', **kwargs):
    """
    Create grouped bar chart with different color scheme options.
    
    Parameters:
    - color_scheme: 'default', 'colorblind', 'grayscale', 'publication'
    """
    
    color_schemes = {
        'default': {
            'MeDI': '#86BBD8',
            'Drug Central': '#04771C', 
            'DrugBank': '#EF8354'
        },
        'colorblind': {
            'MeDI': '#377eb8',     # Blue
            'Drug Central': '#e41a1c',  # Red
            'DrugBank': '#4daf4a'      # Green
        },
        'grayscale': {
            'MeDI': '#2b2b2b',     # Dark gray
            'Drug Central': '#636363',  # Medium gray
            'DrugBank': '#969696'      # Light gray
        },
        'publication': {
            'MeDI': '#2166ac',     # Professional blue
            'Drug Central': '#762a83',  # Professional purple
            'DrugBank': '#1b7837'      # Professional green
        }
    }
    
    # Temporarily replace the global color dict
    global database_colors
    original_colors = database_colors.copy() if 'database_colors' in globals() else {}
    database_colors = color_schemes.get(color_scheme, color_schemes['default'])
    
    try:
        return create_grouped_bar_chart(data=data, **kwargs)
    finally:
        # Restore original colors
        if original_colors:
            database_colors = original_colors

# Example usage and test function
def test_with_sample_data():
    """Test the function with sample data"""
    
    # Create sample data
    sample_data = pd.DataFrame({
        'MeDI': [45.2, 42.8, 38.1, 41.7],
        'Drug Central': [52.3, 48.9, 44.6, 48.6], 
        'DrugBank': [58.7, 55.2, 51.3, 55.1]
    }, index=['USA', 'Europe', 'Japan', 'Overall'])
    
    print("Sample Data:")
    print(sample_data)
    print("\nCreating chart...")
    
    # Create the chart
    fig, ax = create_grouped_bar_chart(
        data=sample_data,
        title="Database Coverage Comparison by Region",
        xlabel="Geographic Regions",
        ylabel="Coverage Score (%)",
        save_path="database_comparison.png"
    )
    
    return fig, ax

# Function to easily create chart with your data
def create_database_chart(medi_values, DrugCentral_values, drugbank_values, 
                         regions=['USA', 'Europe', 'Japan', 'Overall'],
                         title="Database Comparison Across Regions",
                         xlabel="Regions", ylabel="Values"):
    """
    Convenience function to create chart with your specific data.
    
    Parameters:
    - medi_values: List of 4 values for MeDI database
    - DrugCentral_values: List of 4 values for Drug Central database  
    - drugbank_values: List of 4 values for DrugBank database
    - regions: List of 4 region names
    """
    
    data = pd.DataFrame({
        'MeDI': medi_values,
        'Drug Central': DrugCentral_values,
        'DrugBank': drugbank_values,
    }, index=regions)
    
    return create_grouped_bar_chart(
        data=data,
        title=title,
        xlabel=xlabel,
        ylabel=ylabel
    )

if __name__ == "__main__":
    print("Testing the fixed grouped bar chart...")
    
    # Test with sample data
    fig, ax = test_with_sample_data()
    
    print("\nChart created successfully!")
    print("\nTo use with your data, call:")
    print("create_database_chart([medi_usa, medi_eu, medi_jp, medi_overall],")
    print("                     [dc_usa, dc_eu, dc_jp, dc_overall],")
    print("                     [db_usa, db_eu, db_jp, db_overall])")