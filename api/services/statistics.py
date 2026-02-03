"""
Statistical analysis service for property price data.
"""

from typing import List, Dict
import pandas as pd
import numpy as np
from collections import defaultdict

try:
    from sklearn.cluster import KMeans, DBSCAN

    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from scipy.stats import pearsonr

    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False


def calculate_price_trends(
    price_history: List[Dict], period: str = "monthly"
) -> List[Dict]:
    """
    Calculate price trends over time.

    Args:
        price_history: List of price history records
        period: Aggregation period (monthly, quarterly, yearly)

    Returns:
        List of trend data points
    """
    if not price_history:
        return []

    df = pd.DataFrame(price_history)
    df["date"] = pd.to_datetime(df["date_of_sale"], format="%d/%m/%Y", errors="coerce")
    df = df.dropna(subset=["date", "price"])

    if df.empty:
        return []

    # Group by period
    if period == "monthly":
        df["period"] = df["date"].dt.to_period("M")
    elif period == "quarterly":
        df["period"] = df["date"].dt.to_period("Q")
    elif period == "yearly":
        df["period"] = df["date"].dt.to_period("Y")
    else:
        df["period"] = df["date"].dt.to_period("M")

    # Aggregate
    grouped = (
        df.groupby("period")["price"]
        .agg(
            [
                ("average_price", "mean"),
                ("median_price", "median"),
                ("std_deviation", "std"),
                ("min_price", "min"),
                ("max_price", "max"),
                ("count", "count"),
            ]
        )
        .reset_index()
    )

    # Convert to list of dicts
    trends = []
    for _, row in grouped.iterrows():
        trends.append(
            {
                "date": str(row["period"]),
                "average_price": (
                    float(row["average_price"])
                    if pd.notna(row["average_price"])
                    else 0.0
                ),
                "median_price": (
                    float(row["median_price"]) if pd.notna(row["median_price"]) else 0.0
                ),
                "std_deviation": (
                    float(row["std_deviation"])
                    if pd.notna(row["std_deviation"])
                    else 0.0
                ),
                "min_price": (
                    float(row["min_price"]) if pd.notna(row["min_price"]) else 0.0
                ),
                "max_price": (
                    float(row["max_price"]) if pd.notna(row["max_price"]) else 0.0
                ),
                "count": int(row["count"]),
            }
        )

    return trends


def calculate_price_clusters(
    prices: List[float], n_clusters: int = 5, algorithm: str = "kmeans"
) -> List[Dict]:
    """
    Cluster properties by price.

    Args:
        prices: List of property prices
        n_clusters: Number of clusters
        algorithm: Clustering algorithm (kmeans, dbscan)

    Returns:
        List of cluster information
    """
    if not prices or len(prices) < n_clusters:
        return []

    prices_array = np.array(prices).reshape(-1, 1)

    if algorithm == "kmeans" and SKLEARN_AVAILABLE:
        kmeans = KMeans(
            n_clusters=min(n_clusters, len(prices)), random_state=42, n_init=10
        )
        labels = kmeans.fit_predict(prices_array)
        centers = kmeans.cluster_centers_.flatten()

        clusters = []
        for i in range(len(centers)):
            cluster_prices = [prices[j] for j in range(len(prices)) if labels[j] == i]
            if cluster_prices:
                clusters.append(
                    {
                        "cluster_id": i,
                        "price_range": {
                            "min": float(min(cluster_prices)),
                            "max": float(max(cluster_prices)),
                        },
                        "count": len(cluster_prices),
                        "average_price": float(np.mean(cluster_prices)),
                        "center_price": float(centers[i]),
                    }
                )

        return clusters

    elif algorithm == "dbscan" and SKLEARN_AVAILABLE:
        # DBSCAN for price clustering
        dbscan = DBSCAN(eps=50000, min_samples=5)  # 50k price difference
        labels = dbscan.fit_predict(prices_array)

        clusters = []
        unique_labels = set(labels)
        if -1 in unique_labels:
            unique_labels.remove(-1)  # Remove noise label

        for label in unique_labels:
            cluster_prices = [
                prices[i] for i in range(len(prices)) if labels[i] == label
            ]
            if cluster_prices:
                clusters.append(
                    {
                        "cluster_id": int(label),
                        "price_range": {
                            "min": float(min(cluster_prices)),
                            "max": float(max(cluster_prices)),
                        },
                        "count": len(cluster_prices),
                        "average_price": float(np.mean(cluster_prices)),
                    }
                )

        return clusters

    else:
        # Fallback: simple range-based clustering
        return simple_price_clustering(prices, n_clusters)


def simple_price_clustering(prices: List[float], n_clusters: int) -> List[Dict]:
    """Simple range-based price clustering."""
    if not prices:
        return []

    prices_sorted = sorted(prices)
    cluster_size = len(prices_sorted) // n_clusters

    clusters = []
    for i in range(n_clusters):
        start_idx = i * cluster_size
        end_idx = start_idx + cluster_size if i < n_clusters - 1 else len(prices_sorted)

        cluster_prices = prices_sorted[start_idx:end_idx]
        if cluster_prices:
            clusters.append(
                {
                    "cluster_id": i,
                    "price_range": {
                        "min": float(min(cluster_prices)),
                        "max": float(max(cluster_prices)),
                    },
                    "count": len(cluster_prices),
                    "average_price": float(np.mean(cluster_prices)),
                }
            )

    return clusters


def calculate_county_statistics(properties_data: List[Dict]) -> List[Dict]:
    """
    Calculate statistics by county.

    Args:
        properties_data: List of property data with county and price

    Returns:
        List of county statistics
    """
    if not properties_data:
        return []

    county_data = defaultdict(lambda: {"prices": [], "count": 0})

    for prop in properties_data:
        county = prop.get("county")
        price = prop.get("price")

        if county and price is not None:
            county_data[county]["prices"].append(price)
            county_data[county]["count"] += 1

    statistics = []
    for county, data in county_data.items():
        prices = data["prices"]
        if prices:
            statistics.append(
                {
                    "county": county,
                    "property_count": data["count"],
                    "average_price": float(np.mean(prices)),
                    "median_price": float(np.median(prices)),
                    "min_price": float(min(prices)),
                    "max_price": float(max(prices)),
                }
            )

    return sorted(statistics, key=lambda x: x["average_price"], reverse=True)


def calculate_correlation(x_values: List[float], y_values: List[float]) -> Dict:
    """
    Calculate correlation between two variables.

    Args:
        x_values: First variable (e.g., size)
        y_values: Second variable (e.g., price)

    Returns:
        Correlation information
    """
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return {
            "correlation_coefficient": 0.0,
            "p_value": 1.0,
            "sample_size": 0,
            "interpretation": "Insufficient data",
        }

    # Remove NaN values
    pairs = [
        (x, y) for x, y in zip(x_values, y_values) if not (np.isnan(x) or np.isnan(y))
    ]
    if len(pairs) < 2:
        return {
            "correlation_coefficient": 0.0,
            "p_value": 1.0,
            "sample_size": len(pairs),
            "interpretation": "Insufficient valid data",
        }

    x_clean = [p[0] for p in pairs]
    y_clean = [p[1] for p in pairs]

    if SCIPY_AVAILABLE:
        corr, p_value = pearsonr(x_clean, y_clean)
    else:
        # Manual correlation calculation
        x_mean = np.mean(x_clean)
        y_mean = np.mean(y_clean)

        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_clean, y_clean))
        x_var = sum((x - x_mean) ** 2 for x in x_clean)
        y_var = sum((y - y_mean) ** 2 for y in y_clean)

        denominator = np.sqrt(x_var * y_var)
        corr = numerator / denominator if denominator > 0 else 0.0
        p_value = 0.0  # Simplified

    # Interpretation
    abs_corr = abs(corr)
    if abs_corr < 0.1:
        interpretation = "Negligible correlation"
    elif abs_corr < 0.3:
        interpretation = "Weak correlation"
    elif abs_corr < 0.5:
        interpretation = "Moderate correlation"
    elif abs_corr < 0.7:
        interpretation = "Strong correlation"
    else:
        interpretation = "Very strong correlation"

    return {
        "correlation_coefficient": float(corr),
        "p_value": float(p_value),
        "sample_size": len(pairs),
        "interpretation": interpretation,
    }
