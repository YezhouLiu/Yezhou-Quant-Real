import matplotlib.pyplot as plt
from typing import Dict
import pandas as pd


def plot_nav(series: Dict[str, pd.DataFrame], title="NAV Comparison"):
    plt.figure(figsize=(12, 6))

    for label, df in series.items():
        plt.plot(df.index, df["nav"], label=label)

    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
