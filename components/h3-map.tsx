"use client"

import React, { useEffect, useRef } from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
const FORECAST_YEAR = 2026;

export default function H3Map() {
    const mapContainer = useRef<HTMLDivElement>(null);
    const map = useRef<maplibregl.Map | null>(null);

    useEffect(() => {
        if (map.current || !mapContainer.current) return;

        if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
            console.error("Supabase URL or Key missing");
            return;
        }

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

            // Construct RPC URL for MVT
            const rpcUrl = `${SUPABASE_URL}/rest/v1/rpc/get_h3_mvt?z={z}&x={x}&y={y}&year=${FORECAST_YEAR}&apikey=${SUPABASE_ANON_KEY}`;

            console.log("Adding H3 source with URL:", rpcUrl);

            map.current.addSource('h3-forecast', {
                type: 'vector',
                tiles: [rpcUrl],
                minzoom: 4,
                maxzoom: 14,
            });

            map.current.addLayer({
                'id': 'h3-fill',
                'type': 'fill',
                'source': 'h3-forecast',
                'source-layer': 'h3_layer',
                'paint': {
                    'fill-color': [
                        'interpolate',
                        ['linear'],
                        ['get', 'score'],
                        0, '#d73027',
                        5, '#ffffbf',
                        10, '#1a9850'
                    ],
                    'fill-opacity': 0.6,
                    'fill-outline-color': 'rgba(0,0,0,0.1)'
                }
            });

            map.current.addLayer({
                'id': 'h3-hover',
                'type': 'line',
                'source': 'h3-forecast',
                'source-layer': 'h3_layer',
                'paint': {
                    'line-color': '#fff',
                    'line-width': 2
                },
                'filter': ['==', 'h3_id', '']
            });

            map.current.on('mousemove', 'h3-fill', (e) => {
                if (!map.current) return;
                if (e.features && e.features.length > 0) {
                    map.current.setFilter('h3-hover', ['==', 'h3_id', e.features[0].properties.h3_id]);
                    map.current.getCanvas().style.cursor = 'pointer';
                }
            });

            map.current.on('mouseleave', 'h3-fill', () => {
                if (!map.current) return;
                map.current.setFilter('h3-hover', ['==', 'h3_id', '']);
                map.current.getCanvas().style.cursor = '';
            });
        });
    }, []);

    return <div ref={mapContainer} style={{ width: '100%', height: '100%' }} />;
}
