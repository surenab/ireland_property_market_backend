"""
Heatmap computation: grid aggregation with NumPy, output polygon features with metadata.
"""

import numpy as np
from typing import List, Dict, Any, Optional


def _grid_cell_polygon(lat_lo: float, lat_hi: float, lng_lo: float, lng_hi: float) -> List[List[float]]:
    """Return GeoJSON-style ring [lng, lat] closed (5 points)."""
    return [
        [lng_lo, lat_lo],
        [lng_hi, lat_lo],
        [lng_hi, lat_hi],
        [lng_lo, lat_hi],
        [lng_lo, lat_lo],
    ]


def compute_heatmap_polygons(
    properties_data: List[Dict[str, Any]],
    north: float,
    south: float,
    east: float,
    west: float,
    analysis_mode: str,
    grid_cells: int = 40,
) -> List[Dict[str, Any]]:
    """
    Aggregate points into a grid and return one polygon per cell with metadata.
    Each polygon is a rectangle (closed ring of 5 [lng, lat] points).
    """
    if not properties_data:
        return []

    lats = np.array([p["latitude"] for p in properties_data if p.get("latitude") is not None and p.get("longitude") is not None])
    lngs = np.array([p["longitude"] for p in properties_data if p.get("latitude") is not None and p.get("longitude") is not None])
    if lats.size == 0:
        return []

    prices = np.array(
        [p.get("price") if p.get("price") is not None else np.nan for p in properties_data if p.get("latitude") is not None and p.get("longitude") is not None]
    )

    lat_edges = np.linspace(south, north, grid_cells + 1)
    lng_edges = np.linspace(west, east, grid_cells + 1)

    # Count per cell
    count_2d, _, _ = np.histogram2d(lats, lngs, bins=[lat_edges, lng_edges])
    # Sum of price per cell (for average)
    price_sum_2d = np.zeros((grid_cells, grid_cells))
    price_count_2d = np.zeros((grid_cells, grid_cells))
    for i in range(lats.size):
        lat, lng, pr = lats[i], lngs[i], prices[i]
        if np.isnan(pr):
            continue
        i_lat = np.searchsorted(lat_edges, lat, side="right") - 1
        i_lng = np.searchsorted(lng_edges, lng, side="right") - 1
        if 0 <= i_lat < grid_cells and 0 <= i_lng < grid_cells:
            price_sum_2d[i_lat, i_lng] += pr
            price_count_2d[i_lat, i_lng] += 1

    max_count = float(np.max(count_2d)) if np.max(count_2d) > 0 else 1.0
    polygons: List[Dict[str, Any]] = []

    for i in range(grid_cells):
        for j in range(grid_cells):
            count = int(count_2d[i, j])
            if count == 0:
                continue
            lat_lo = float(lat_edges[i])
            lat_hi = float(lat_edges[i + 1])
            lng_lo = float(lng_edges[j])
            lng_hi = float(lng_edges[j + 1])
            coordinates = _grid_cell_polygon(lat_lo, lat_hi, lng_lo, lng_hi)
            intensity = min(count / max_count, 1.0)
            pc = price_count_2d[i, j]
            avg_price = int(round(price_sum_2d[i, j] / pc)) if pc > 0 else None
            metadata: Dict[str, Any] = {
                "intensity": intensity,
                "sales_count": count,
            }
            if avg_price is not None:
                metadata["avg_price"] = avg_price
            polygons.append({"coordinates": [coordinates], "metadata": metadata})

    return polygons
