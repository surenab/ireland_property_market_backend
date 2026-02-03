"""
Clustering algorithms for property data.
"""

from typing import List, Dict
from collections import defaultdict


def cluster_by_price_range(
    prices: List[float], n_ranges: int = 5
) -> Dict[int, List[float]]:
    """Cluster prices into ranges."""
    if not prices:
        return {}

    sorted(prices)
    range_size = (max(prices) - min(prices)) / n_ranges

    clusters = {i: [] for i in range(n_ranges)}

    for price in prices:
        if price == max(prices):
            clusters[n_ranges - 1].append(price)
        else:
            cluster_id = int((price - min(prices)) / range_size)
            clusters[cluster_id].append(price)

    return clusters


def cluster_by_size_category(sizes: List[str]) -> Dict[str, List[int]]:
    """Cluster properties by size category."""
    if not sizes:
        return {}

    categories = defaultdict(list)

    for i, size in enumerate(sizes):
        if not size:
            categories["unknown"].append(i)
        elif "less than 38" in size.lower():
            categories["small"].append(i)
        elif "38" in size and "125" in size:
            categories["medium"].append(i)
        elif "greater than 125" in size.lower() or "125" in size:
            categories["large"].append(i)
        else:
            categories["other"].append(i)

    return dict(categories)


def temporal_clustering(dates: List[str], period: str = "year") -> Dict[str, List[int]]:
    """Cluster properties by sale date periods."""
    from datetime import datetime

    clusters = defaultdict(list)

    for i, date_str in enumerate(dates):
        date = datetime.strptime(date_str, "%d/%m/%Y")

        if period == "year":
            key = str(date.year)
        elif period == "quarter":
            quarter = (date.month - 1) // 3 + 1
            key = f"{date.year}-Q{quarter}"
        elif period == "month":
            key = f"{date.year}-{date.month:02d}"
        else:
            key = str(date.year)

        clusters[key].append(i)

    return dict(clusters)
