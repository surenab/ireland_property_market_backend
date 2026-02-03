"""
Map clustering service for geographic data visualization.
"""

from typing import List, Dict
from api.schemas import MapCluster, MapPoint


def cluster_properties(
    properties: List[Dict], zoom: int, mode: str = "geographic"
) -> List[MapCluster]:
    """
    Cluster properties based on mode and zoom level.

    Args:
        properties: List of property dictionaries with lat/lng
        zoom: Map zoom level (1-20)
        mode: Clustering mode (geographic, price, size)

    Returns:
        List of MapCluster objects
    """
    if mode == "geographic":
        return geographic_clustering(properties, zoom)
    elif mode == "price":
        return price_clustering(properties, zoom)
    elif mode == "size":
        return size_clustering(properties, zoom)
    else:
        return geographic_clustering(properties, zoom)


def geographic_clustering(properties: List[Dict], zoom: int) -> List[MapCluster]:
    """Geographic clustering using grid-based approach."""
    if not properties:
        return []

    # Calculate grid size based on zoom level
    # Higher zoom = smaller grid cells = more clusters
    grid_size = max(0.01, 0.5 / (2 ** (zoom - 5)))

    # Create grid
    grid = {}

    for prop in properties:
        lat = prop["latitude"]
        lng = prop["longitude"]

        # Calculate grid cell
        grid_lat = int(lat / grid_size)
        grid_lng = int(lng / grid_size)
        key = (grid_lat, grid_lng)

        if key not in grid:
            grid[key] = []
        grid[key].append(prop)

    # Convert grid to clusters
    clusters = []
    for key, props in grid.items():
        if len(props) == 1:
            # Single property - create point
            prop = props[0]
            clusters.append(
                MapCluster(
                    center_lat=prop["latitude"],
                    center_lng=prop["longitude"],
                    count=1,
                    bounds={
                        "north": prop["latitude"],
                        "south": prop["latitude"],
                        "east": prop["longitude"],
                        "west": prop["longitude"],
                    },
                    properties=[
                        MapPoint(
                            id=prop["id"],
                            latitude=prop["latitude"],
                            longitude=prop["longitude"],
                            price=prop.get("price"),
                            address=prop.get("address"),
                            county=prop.get("county"),
                        )
                    ],
                )
            )
        else:
            # Multiple properties - create cluster
            lats = [p["latitude"] for p in props]
            lngs = [p["longitude"] for p in props]

            center_lat = sum(lats) / len(lats)
            center_lng = sum(lngs) / len(lngs)

            clusters.append(
                MapCluster(
                    center_lat=center_lat,
                    center_lng=center_lng,
                    count=len(props),
                    bounds={
                        "north": max(lats),
                        "south": min(lats),
                        "east": max(lngs),
                        "west": min(lngs),
                    },
                    properties=[
                        MapPoint(
                            id=p["id"],
                            latitude=p["latitude"],
                            longitude=p["longitude"],
                            price=p.get("price"),
                            address=p.get("address"),
                            county=p.get("county"),
                        )
                        for p in props
                    ],
                )
            )

    return clusters


def price_clustering(properties: List[Dict], zoom: int) -> List[MapCluster]:
    """Price-based clustering."""
    if not properties:
        return []

    # Group by price ranges
    price_ranges = [
        (0, 100000),
        (100000, 200000),
        (200000, 300000),
        (300000, 400000),
        (400000, 500000),
        (500000, 750000),
        (750000, 1000000),
        (1000000, float("inf")),
    ]

    price_groups = {i: [] for i in range(len(price_ranges))}

    for prop in properties:
        price = prop.get("price")
        if price is None:
            continue

        for i, (min_price, max_price) in enumerate(price_ranges):
            if min_price <= price < max_price:
                price_groups[i].append(prop)
                break

    clusters = []
    for group_props in price_groups.values():
        if not group_props:
            continue

        # Create geographic clusters within price group
        sub_clusters = geographic_clustering(group_props, zoom)
        clusters.extend(sub_clusters)

    return clusters


def size_clustering(properties: List[Dict], zoom: int) -> List[MapCluster]:
    """Size-based clustering (placeholder - requires size data)."""
    # For now, fall back to geographic clustering
    # This can be enhanced when size data is available
    return geographic_clustering(properties, zoom)
