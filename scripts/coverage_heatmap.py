#!/usr/bin/env python3
"""
Transit Coverage Heat Map - Show good vs bad coverage areas
"""

import duckdb
from pathlib import Path

print("Creating coverage heat map...")

conn = duckdb.connect('data/trimet_analysis.duckdb')

# Get density data - how many stops in each area
density = conn.execute("""
    SELECT 
        ROUND(stop_lat, 2) as lat,
        ROUND(stop_lon, 2) as lon,
        COUNT(*) as stop_count
    FROM stops
    GROUP BY lat, lon
    ORDER BY stop_count DESC
""").fetchall()

print(f"Analyzed {len(density)} areas")

# Categorize
high_density = [d for d in density if d[2] >= 20]  # 20+ stops = good
medium_density = [d for d in density if 5 <= d[2] < 20]  # 5-19 = ok
low_density = [d for d in density if 2 <= d[2] < 5]  # 2-4 = poor
desert = [d for d in density if d[2] == 1]  # 1 = desert

print(f"High coverage: {len(high_density)} areas")
print(f"Medium coverage: {len(medium_density)} areas")
print(f"Low coverage: {len(low_density)} areas")
print(f"Transit deserts: {len(desert)} areas (only 1 stop)")

# Calculate insights
total_poor = len(desert) + len(low_density)
inequality_ratio = max(d[2] for d in high_density) / 1 if desert else 0
total_stops = sum(d[2] for d in density)
avg_stops = total_stops / len(density) if density else 0

html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Portland Transit Coverage Heat Map</title>
    <meta charset="utf-8">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        body {{ margin: 0; padding: 0; font-family: -apple-system, sans-serif; }}
        .header {{
            background: linear-gradient(135deg, #1e40af 0%, #7c3aed 100%);
            color: white;
            padding: 20px 30px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        }}
        .header h1 {{ margin: 0; font-size: 24px; }}
        .insights {{
            margin-top: 10px;
            padding: 10px;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 6px;
            font-size: 13px;
        }}
        .insights strong {{ color: #fbbf24; }}
        .header-stats {{
            display: flex;
            gap: 30px;
            margin-top: 15px;
        }}
        .header-stat {{
            flex: 1;
        }}
        .header-stat-value {{
            font-size: 28px;
            font-weight: bold;
        }}
        .header-stat-label {{
            font-size: 12px;
            opacity: 0.9;
        }}
        #map {{ height: calc(100vh - 120px); width: 100%; }}
        .overlay {{
            position: absolute;
            bottom: 20px;
            right: 20px;
            background: rgba(255, 255, 255, 0.95);
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.3);
            z-index: 1000;
            max-width: 200px;
        }}
        .overlay h2 {{ margin: 0 0 10px 0; font-size: 16px; color: #1f2937; }}
        .coverage-item {{
            display: flex;
            align-items: center;
            margin: 6px 0;
            font-size: 12px;
        }}
        .color-box {{
            width: 18px;
            height: 18px;
            border-radius: 3px;
            margin-right: 8px;
            border: 1px solid #fff;
            box-shadow: 0 1px 2px rgba(0,0,0,0.2);
            flex-shrink: 0;
        }}
        .coverage-text {{
            flex: 1;
        }}
        .coverage-label {{
            font-weight: 600;
            font-size: 12px;
        }}
        .coverage-count {{
            font-size: 10px;
            color: #6b7280;
        }}
        .insight {{
            margin-top: 12px;
            padding: 8px;
            background: #fef3c7;
            border-left: 3px solid #f59e0b;
            border-radius: 3px;
            font-size: 10px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Portland Transit Coverage Analysis</h1>
        <div class="insights">
            <strong>Key Finding:</strong> {total_poor} areas ({(total_poor/len(density)*100):.1f}%) have inadequate transit access. 
            Best-served areas have {int(inequality_ratio)}x more stops than deserts. 
            Average: {avg_stops:.1f} stops per area.
        </div>
        <div class="header-stats">
            <div class="header-stat">
                <div class="header-stat-value">{len(density)}</div>
                <div class="header-stat-label">Total Coverage Areas</div>
            </div>
            <div class="header-stat">
                <div class="header-stat-value">{len(desert)}</div>
                <div class="header-stat-label">Transit Deserts (Red)</div>
            </div>
            <div class="header-stat">
                <div class="header-stat-value">{len(high_density)}</div>
                <div class="header-stat-label">Well-Served Areas (Green)</div>
            </div>
            <div class="header-stat">
                <div class="header-stat-value">{(len(desert)/len(density)*100):.1f}%</div>
                <div class="header-stat-label">Areas with Poor Access</div>
            </div>
        </div>
    </div>
    
    <div id="map"></div>
    
    <div class="overlay">
        <h2>Transit Coverage</h2>
        
        <div class="coverage-item">
            <div class="color-box" style="background: #10b981;"></div>
            <div class="coverage-text">
                <div class="coverage-label">Excellent</div>
                <div class="coverage-count">{len(high_density)} areas (20+ stops)</div>
            </div>
        </div>
        
        <div class="coverage-item">
            <div class="color-box" style="background: #3b82f6;"></div>
            <div class="coverage-text">
                <div class="coverage-label">Good</div>
                <div class="coverage-count">{len(medium_density)} areas (5-19 stops)</div>
            </div>
        </div>
        
        <div class="coverage-item">
            <div class="color-box" style="background: #f59e0b;"></div>
            <div class="coverage-text">
                <div class="coverage-label">Poor</div>
                <div class="coverage-count">{len(low_density)} areas (2-4 stops)</div>
            </div>
        </div>
        
        <div class="coverage-item">
            <div class="color-box" style="background: #dc2626;"></div>
            <div class="coverage-text">
                <div class="coverage-label">Desert</div>
                <div class="coverage-count">{len(desert)} areas (1 stop only)</div>
            </div>
        </div>
        
        <div class="insight">
            <strong>Key Insight:</strong> {len(desert)} areas depend on a single transit stop with no alternatives nearby.
        </div>
    </div>
    
    <script>
        var map = L.map('map').setView([45.5152, -122.6784], 11);
        
        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '© OpenStreetMap'
        }}).addTo(map);
        
        // DESERT AREAS (RED) - worst coverage
        var deserts = {str([[d[0], d[1], d[2]] for d in desert])};
        deserts.forEach(function(area) {{
            L.rectangle(
                [[area[0]-0.005, area[1]-0.005], [area[0]+0.005, area[1]+0.005]],
                {{
                    color: '#dc2626',
                    fillColor: '#dc2626',
                    fillOpacity: 0.6,
                    weight: 2
                }}
            ).addTo(map).bindPopup('<b>Transit Desert</b><br>Only ' + area[2] + ' stop in this area');
        }});
        
        // LOW COVERAGE (ORANGE)
        var low = {str([[d[0], d[1], d[2]] for d in low_density])};
        low.forEach(function(area) {{
            L.rectangle(
                [[area[0]-0.005, area[1]-0.005], [area[0]+0.005, area[1]+0.005]],
                {{
                    color: '#f59e0b',
                    fillColor: '#f59e0b',
                    fillOpacity: 0.5,
                    weight: 1
                }}
            ).addTo(map).bindPopup('<b>Low Coverage</b><br>' + area[2] + ' stops in this area');
        }});
        
        // MEDIUM COVERAGE (BLUE)
        var medium = {str([[d[0], d[1], d[2]] for d in medium_density])};
        medium.forEach(function(area) {{
            L.rectangle(
                [[area[0]-0.005, area[1]-0.005], [area[0]+0.005, area[1]+0.005]],
                {{
                    color: '#3b82f6',
                    fillColor: '#3b82f6',
                    fillOpacity: 0.4,
                    weight: 1
                }}
            ).addTo(map).bindPopup('<b>Good Coverage</b><br>' + area[2] + ' stops in this area');
        }});
        
        // HIGH COVERAGE (GREEN)
        var high = {str([[d[0], d[1], d[2]] for d in high_density])};
        high.forEach(function(area) {{
            L.rectangle(
                [[area[0]-0.005, area[1]-0.005], [area[0]+0.005, area[1]+0.005]],
                {{
                    color: '#10b981',
                    fillColor: '#10b981',
                    fillOpacity: 0.3,
                    weight: 1
                }}
            ).addTo(map).bindPopup('<b>Excellent Coverage</b><br>' + area[2] + ' stops in this area');
        }});
        
        console.log('Heat map loaded');
        console.log('Red areas: ' + deserts.length + ' transit deserts');
        console.log('Orange areas: ' + low.length + ' low coverage');
        console.log('Blue areas: ' + medium.length + ' good coverage');
        console.log('Green areas: ' + high.length + ' excellent coverage');
    </script>
</body>
</html>
"""

output = Path("data/coverage_heatmap.html")
with open(output, 'w') as f:
    f.write(html)

print(f"\n✓ Heat map created: {output}")
print(f"\nThis shows:")
print(f"  RED = Desert areas (only 1 stop)")
print(f"  ORANGE = Poor coverage (2-4 stops)")
print(f"  BLUE = Good coverage (5-19 stops)")
print(f"  GREEN = Excellent coverage (20+ stops)")
print(f"\nOpen: open {output}")

conn.close()