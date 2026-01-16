"use client"

import React, { useEffect, useRef } from 'react';
import maplibregl, { AddLayerObject } from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { Protocol } from 'pmtiles';

const PMTILES_URL = 'http://localhost:3000/tiles/h3_data.pmtiles';

interface H3MapProps {
    year?: number;
    colorMode?: "growth" | "value";
}

export default function H3Map({ year = 2026, colorMode = "growth" }: H3MapProps) {
    const mapContainer = useRef<HTMLDivElement>(null);
    const map = useRef<maplibregl.Map | null>(null);
    const currentYear = useRef<number>(year);
    const resLevels = [7, 8, 9, 10, 11];

    // Initialize map once
    useEffect(() => {
        if (map.current || !mapContainer.current) return;

        // Register PMTiles protocol
        const protocol = new Protocol();
        maplibregl.addProtocol('pmtiles', protocol.tile);

        map.current = new maplibregl.Map({
            container: mapContainer.current,
            style: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
            center: [-95.36, 29.76], // Houston
            zoom: 10,
            pitch: 0,
            bearing: 0,
            attributionControl: false
        });

        map.current.addControl(new maplibregl.NavigationControl(), 'top-right');
        map.current.addControl(new maplibregl.AttributionControl({ compact: true }), 'bottom-right');

        map.current.on('load', () => {
            if (!map.current) return;

            console.log("[H3Map] Adding PMTiles source:", PMTILES_URL);

            map.current.addSource('h3-pmtiles', {
                type: 'vector',
                url: `pmtiles://${PMTILES_URL}`,
                attribution: '© Properlytic'
            });

            // Add layers for each resolution level (7-11)
            resLevels.forEach(res => {
                const layerId = `h3-res${res}-fill`;

                // Growth mode color scale (opportunity-based)
                const growthColorExpr = [
                    'interpolate',
                    ['linear'],
                    ['get', 'opportunity_pct'],
                    -50, '#8b5cf6',  // Purple for negative
                    0, '#f5f5f5',    // White for neutral
                    50, '#3b82f6'    // Blue for positive
                ];

                // Value mode color scale (med_predicted_value-based)
                const valueColorExpr = [
                    'interpolate',
                    ['linear'],
                    ['coalesce', ['get', 'med_predicted_value'], 0],
                    100000, '#3d1f5c',   // Deep purple (low)
                    500000, '#c44536',   // Red-orange (mid)
                    1500000, '#f5e663'   // Yellow (high)
                ];

                map.current?.addLayer({
                    'id': layerId,
                    'type': 'fill',
                    'source': 'h3-pmtiles',
                    'source-layer': `h3_res${res}`,
                    'minzoom': res === 7 ? 0 : res + 2,
                    'maxzoom': res === 11 ? 24 : res + 3,
                    'paint': {
                        'fill-color': colorMode === 'value' ? valueColorExpr : growthColorExpr,
                        'fill-opacity': 0.6,
                        'fill-outline-color': 'rgba(0,0,0,0.1)'
                    },
                    'filter': ['==', ['get', 'forecast_year'], currentYear.current]
                } as AddLayerObject);

                // Hover effect layer - starts with false filter (nothing highlighted)
                map.current?.addLayer({
                    'id': `h3-res${res}-hover`,
                    'type': 'line',
                    'source': 'h3-pmtiles',
                    'source-layer': `h3_res${res}`,
                    'minzoom': res === 7 ? 0 : res + 2,
                    'maxzoom': res === 11 ? 24 : res + 3,
                    'paint': {
                        'line-color': '#fff',
                        'line-width': 2
                    },
                    'filter': ['==', ['get', 'h3_id'], ''] // Empty string = no match initially
                } as AddLayerObject);
            });

            // Mouse interaction for hover
            const hoverLayers = resLevels.map(r => `h3-res${r}-fill`);

            hoverLayers.forEach(layer => {
                map.current?.on('mousemove', layer, (e) => {
                    if (!map.current) return;
                    if (e.features && e.features.length > 0) {
                        const h3Id = e.features[0].properties.h3_id;
                        const res = e.features[0].properties.h3_res;
                        // Set filter to highlight this specific hex
                        map.current.setFilter(`h3-res${res}-hover`, ['==', ['get', 'h3_id'], h3Id]);
                        map.current.getCanvas().style.cursor = 'pointer';
                    }
                });

                map.current?.on('mouseleave', layer, () => {
                    if (!map.current) return;
                    // Reset all hover filters to empty
                    resLevels.forEach(r => {
                        map.current?.setFilter(`h3-res${r}-hover`, ['==', ['get', 'h3_id'], '']);
                    });
                    map.current.getCanvas().style.cursor = '';
                });
            });
        });

        return () => {
            // Cleanup on unmount
            if (map.current) {
                map.current.remove();
                map.current = null;
            }
        };
    }, []);

    // Update filters when year changes
    useEffect(() => {
        if (!map.current || !map.current.isStyleLoaded()) return;

        currentYear.current = year;
        console.log(`[H3Map] Updating filters for year ${year}`);

        resLevels.forEach(res => {
            const fillLayerId = `h3-res${res}-fill`;
            const hoverLayerId = `h3-res${res}-hover`;

            // Check if layer exists before setting filter
            if (map.current?.getLayer(fillLayerId)) {
                map.current.setFilter(fillLayerId, ['==', ['get', 'forecast_year'], year]);
            }
            if (map.current?.getLayer(hoverLayerId)) {
                map.current.setFilter(hoverLayerId, ['all', ['==', ['get', 'forecast_year'], year], ['==', 'h3_id', '']]);
            }
        });
    }, [year]);

    // Update paint properties when colorMode changes
    useEffect(() => {
        if (!map.current || !map.current.isStyleLoaded()) return;

        console.log(`[H3Map] Updating color mode to ${colorMode}`);

        const growthColorExpr: any = [
            'interpolate',
            ['linear'],
            ['get', 'opportunity_pct'],
            -50, '#8b5cf6',
            0, '#f5f5f5',
            50, '#3b82f6'
        ];

        const valueColorExpr: any = [
            'interpolate',
            ['linear'],
            ['coalesce', ['get', 'med_predicted_value'], 0],
            100000, '#3d1f5c',
            500000, '#c44536',
            1500000, '#f5e663'
        ];

        resLevels.forEach(res => {
            const fillLayerId = `h3-res${res}-fill`;
            if (map.current?.getLayer(fillLayerId)) {
                map.current.setPaintProperty(
                    fillLayerId,
                    'fill-color',
                    colorMode === 'value' ? valueColorExpr : growthColorExpr
                );
            }
        });
    }, [colorMode]);

    return <div ref={mapContainer} style={{ width: '100%', height: '100%' }} />;
}
