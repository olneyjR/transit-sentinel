#!/usr/bin/env python3
"""
Transit Desert Analysis - Find real gaps in Portland transit coverage
Shows WHERE transit is missing, not just density stats
"""

import duckdb
from pathlib import Path
from datetime import datetime
import math

print("=" * 70)
print("  TRANSIT DESERT FINDER - Where Can't People Get Transit?")
print("=" * 70)

conn = duckdb.connect('data/trimet_analysis.duckdb')

# Get all stops
all_stops = conn.execute("""
    SELECT stop_id, stop_name, stop_lat, stop_lon
    FROM stops
    ORDER BY stop_lat, stop_lon
""").fetchall()

print(f"\nAnalyzing {len(all_stops)} transit stops...")

# Find gaps - areas far from any stop
print("\n[STEP 1] Finding gaps in coverage...")

# Create grid and find cells with NO nearby stops
gaps = []
lat_min, lat_max = 45.40, 45.65
lon_min, lon_max = -122.80, -122.30

for lat in [lat_min + i*0.02 for i in range(int((lat_max-lat_min)/0.02))]:
    for lon in [lon_min + i*0.02 for i in range(int((lon_max-lon_min)/0.02))]:
        # Check if any stop within 800m (~0.5 miles, 10 min walk)
        nearest_distance = float('inf')
        
        for stop in all_stops:
            # Quick distance approximation
            lat_diff = abs(stop[2] - lat)
            lon_diff = abs(stop[3] - lon)
            
            if lat_diff < 0.01 and lon_diff < 0.01:  # Worth calculating
                # Rough distance in km
                dist_km = math.sqrt(lat_diff**2 + lon_diff**2) * 111
                nearest_distance = min(nearest_distance, dist_km)
        
        # If nearest stop is > 800m away, it's a gap
        if nearest_distance > 0.8:
            gaps.append({
                'lat': lat,
                'lon': lon,
                'distance_km': nearest_distance
            })

print(f"Found {len(gaps)} coverage gaps (>800m from nearest stop)")

# Find the biggest gaps
gaps.sort(key=lambda x: x['distance_km'], reverse=True)

print(f"\nBiggest transit deserts:")
for gap in gaps[:10]:
    print(f"  ({gap['lat']:.4f}, {gap['lon']:.4f}): {gap['distance_km']:.2f}km to nearest stop")

# Find stops that are isolated (far from other stops)
print("\n[STEP 2] Finding isolated stops...")

isolated_stops = []
for i, stop in enumerate(all_stops):
    # Find distance to nearest other stop
    min_dist = float('inf')
    
    for j, other_stop in enumerate(all_stops):
        if i != j:
            lat_diff = abs(stop[2] - other_stop[2])
            lon_diff = abs(stop[3] - other_stop[3])
            
            if lat_diff < 0.05 and lon_diff < 0.05:
                dist = math.sqrt(lat_diff**2 + lon_diff**2) * 111
                min_dist = min(min_dist, dist)
    
    # If nearest stop is >1km away, it's isolated
    if min_dist > 1.0:
        isolated_stops.append({
            'name': stop[1],
            'lat': stop[2],
            'lon': stop[3],
            'distance_km': min_dist
        })

isolated_stops.sort(key=lambda x: x['distance_km'], reverse=True)

print(f"\nFound {len(isolated_stops)} isolated stops (>1km from next stop):")
for stop in isolated_stops[:10]:
    print(f"  {stop['name'][:50]:50} | {stop['distance_km']:.2f}km to next stop")

# Get route coverage for context
routes = conn.execute("SELECT COUNT(*) FROM routes WHERE route_type = 3").fetchone()[0]
max_stations = conn.execute("SELECT COUNT(*) FROM routes WHERE route_type = 0").fetchone()[0]

# Create interactive map
html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Portland Transit Deserts - Where Coverage is Missing</title>
    <meta charset="utf-8">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        body {{ margin: 0; padding: 0; font-family: -apple-system, sans-serif; }}
        .header {{
            background: linear-gradient(135deg, #dc2626 0%, #991b1b 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{ margin: 0; font-size: 32px; }}
        .header p {{ margin: 10px 0 0 0; }}
        #map {{ height: 70vh; width: 100%; }}
        .info {{
            position: absolute;
            top: 80px;
            right: 10px;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.2);
            z-index: 1000;
            max-width: 300px;
        }}
        .info h3 {{ margin: 0 0 15px 0; color: #dc2626; }}
        .stat {{ margin: 10px 0; }}
        .stat-num {{ font-size: 24px; font-weight: bold; color: #dc2626; }}
        .stat-label {{ font-size: 12px; color: #666; }}
        .insight-box {{
            max-width: 1200px;
            margin: 30px auto;
            padding: 0 20px;
        }}
        .insight {{
            background: #fef2f2;
            border-left: 4px solid #dc2626;
            padding: 20px;
            margin: 20px 0;
            border-radius: 4px;
        }}
        .insight h3 {{ margin: 0 0 10px 0; color: #dc2626; }}
        .insight ul {{ margin: 10px 0; padding-left: 20px; }}
        .legend {{
            position: absolute;
            bottom: 30px;
            left: 10px;
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            z-index: 1000;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            margin: 8px 0;
        }}
        .legend-color {{
            width: 20px;
            height: 20px;
            margin-right: 10px;
            border-radius: 50%;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Portland Transit Deserts</h1>
        <p>Where people can't access public transportation</p>
    </div>
    
    <div id="map"></div>
    
    <div class="info">
        <h3>Coverage Gaps</h3>
        <div class="stat">
            <div class="stat-num">{len(gaps)}</div>
            <div class="stat-label">Areas >800m from transit</div>
        </div>
        <div class="stat">
            <div class="stat-num">{len(isolated_stops)}</div>
            <div class="stat-label">Isolated stops</div>
        </div>
        <div class="stat">
            <div class="stat-num">{max(g['distance_km'] for g in gaps):.1f}km</div>
            <div class="stat-label">Largest gap</div>
        </div>
        <hr>
        <small>Red areas show where transit is missing. Click markers for details.</small>
    </div>
    
    <div class="legend">
        <strong>Map Legend</strong>
        <div class="legend-item">
            <div class="legend-color" style="background: #dc2626;"></div>
            <span>Severe gap (&gt;1.5km)</span>
        </div>
        <div class="legend-item">
            <div class="legend-color" style="background: #f97316;"></div>
            <span>Transit desert (800m-1.5km)</span>
        </div>
        <div class="legend-item">
            <div class="legend-color" style="background: #fbbf24;"></div>
            <span>Isolated stop</span>
        </div>
        <div class="legend-item">
            <div class="legend-color" style="background: #10b981; opacity: 0.3;"></div>
            <span>All transit stops</span>
        </div>
    </div>
    
    <div class="insight-box">
        <div class="insight">
            <h3>What This Shows</h3>
            <p><strong>RED AREAS:</strong> Places where residents are more than 800 meters (0.5 miles / 10 minute walk) from the nearest bus or MAX stop.</p>
            <p><strong>ORANGE AREAS:</strong> Moderate gaps where people need to walk 800m-1.5km to reach transit.</p>
            <p><strong>YELLOW MARKERS:</strong> Isolated stops that are far from other stops, creating service vulnerability.</p>
        </div>
        
        <div class="insight">
            <h3>Why This Matters</h3>
            <ul>
                <li><strong>Transportation Equity:</strong> Low-income residents without cars need nearby transit access</li>
                <li><strong>Climate Goals:</strong> Can't reduce car dependency if transit isn't accessible</li>
                <li><strong>Economic Access:</strong> People can't get to jobs if there's no transit in their neighborhood</li>
                <li><strong>Emergency Resilience:</strong> Isolated stops create single points of failure</li>
            </ul>
        </div>
        
        <div class="insight">
            <h3>Recommendations</h3>
            <p><strong>For the {len([g for g in gaps if g['distance_km'] > 1.5])} severe gaps:</strong></p>
            <ul>
                <li>Prioritize new route planning in these areas</li>
                <li>Consider on-demand shuttle services as interim solution</li>
                <li>Partner with ride-sharing for first/last mile connections</li>
            </ul>
            <p><strong>For the {len(isolated_stops)} isolated stops:</strong></p>
            <ul>
                <li>Add connecting routes to create network redundancy</li>
                <li>Increase service frequency to reduce wait times</li>
                <li>Install real-time arrival boards to improve user experience</li>
            </ul>
        </div>
        
        <div class="insight">
            <h3>Data & Methodology</h3>
            <p>Analysis based on {len(all_stops):,} actual TriMet bus and MAX stops.</p>
            <p><strong>Transit desert definition:</strong> Area where walking distance to nearest stop exceeds 800 meters (international best practice standard).</p>
            <p><strong>Isolated stop definition:</strong> Stop where the next nearest stop is over 1 kilometer away, creating network fragility.</p>
            <p><em>Data: TriMet GTFS feed, {datetime.now().strftime('%B %Y')}</em></p>
        </div>
    </div>
    
    <script>
        var map = L.map('map').setView([45.5152, -122.6784], 11);
        
        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '© OpenStreetMap'
        }}).addTo(map);
        
        // Show ALL stops in light green for context
        var allStops = {str([(s[2], s[3]) for s in all_stops[:500]])};
        allStops.forEach(function(stop) {{
            L.circleMarker([stop[0], stop[1]], {{
                radius: 2,
                fillColor: "#10b981",
                color: "#10b981",
                weight: 1,
                opacity: 0.3,
                fillOpacity: 0.3
            }}).addTo(map);
        }});
        
        // Show coverage gaps in RED
        var gaps = {str([(g['lat'], g['lon'], g['distance_km']) for g in gaps])};
        gaps.forEach(function(gap) {{
            var color = gap[2] > 1.5 ? '#dc2626' : '#f97316';
            var radius = gap[2] * 400;
            
            L.circle([gap[0], gap[1]], {{
                color: color,
                fillColor: color,
                fillOpacity: 0.4,
                radius: radius,
                weight: 2
            }}).addTo(map).bindPopup(
                '<b>Transit Desert</b><br>' +
                gap[2].toFixed(2) + ' km to nearest stop<br>' +
                '<em>' + (gap[2] * 0.621).toFixed(1) + ' miles / ' + (gap[2] * 12.5).toFixed(0) + ' min walk</em>'
            );
        }});
        
        // Show isolated stops in YELLOW
        var isolated = {str([(s['lat'], s['lon'], s['name'], s['distance_km']) for s in isolated_stops])};
        isolated.forEach(function(stop) {{
            L.circleMarker([stop[0], stop[1]], {{
                radius: 8,
                fillColor: "#fbbf24",
                color: "#000",
                weight: 2,
                opacity: 1,
                fillOpacity: 0.8
            }}).addTo(map).bindPopup(
                '<b>Isolated Stop</b><br>' +
                stop[2] + '<br>' +
                stop[3].toFixed(2) + ' km to next stop'
            );
        }});
        
        console.log('Loaded ' + gaps.length + ' coverage gaps');
        console.log('Loaded ' + isolated.length + ' isolated stops');
    </script>
</body>
</html>
"""

output_path = Path("data/transit_desert.html")
with open(output_path, 'w') as f:
    f.write(html)

print("\n" + "=" * 70)
print("  ANALYSIS COMPLETE")
print("=" * 70)
print(f"\nKey findings:")
print(f"  • {len(gaps)} areas lack accessible transit (>800m away)")
print(f"  • {len([g for g in gaps if g['distance_km'] > 1.5])} severe gaps (>1.5km)")
print(f"  • {len(isolated_stops)} isolated stops need network connections")
print(f"  • Largest gap: {max(g['distance_km'] for g in gaps):.2f}km from transit")
print(f"\nThis shows REAL problems people face accessing transit.")
print(f"\nOpen report: open {output_path}")

conn.close()