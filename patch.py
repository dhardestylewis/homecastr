from pathlib import Path

path = Path("components/forecast-map.tsx")
text = path.read_text("utf8")

old = """        // MONKEY-PATCH MapLibre Events to trace the crash
        const originalOn = map.on;
        map.on = function(type: any, layerIdsOrListener: any, listener?: any) {
            if (listener !== undefined && typeof layerIdsOrListener === 'object' && !Array.isArray(layerIdsOrListener) && layerIdsOrListener !== null) {
                console.error("[MAP DEBUG] BAD MAP.ON!!!", type, layerIdsOrListener, listener);
            }
            return (originalOn as any).call(this, type, layerIdsOrListener, listener);
        };

        const originalOnce = map.once;
        map.once = function(type: any, layerIdsOrListener?: any, listener?: any) {
            if (listener !== undefined && typeof layerIdsOrListener === 'object' && !Array.isArray(layerIdsOrListener) && layerIdsOrListener !== null) {
                console.error("[MAP DEBUG] BAD MAP.ONCE!!!", type, layerIdsOrListener, listener);
            }
            return (originalOnce as any).call(this, type, layerIdsOrListener, listener);
        };"""

new = """        // MONKEY-PATCH MapLibre Events to trace invalid 3-arg registrations
        // IMPORTANT: forward EXACT original arity/args. Do not always pass 3 args,
        // or valid 2-arg calls like map.on("load", handler) can be misread internally.
        const originalOn = map.on.bind(map)
        map.on = function (...args: any[]) {
            const [type, second, third] = args
            if (
                args.length === 3 &&
                typeof second === "object" &&
                !Array.isArray(second) &&
                second !== null
            ) {
                console.error("[MAP DEBUG] BAD MAP.ON!!!", type, second, third)
            }
            return (originalOn as any)(...args)
        } as any

        const originalOnce = map.once.bind(map)
        map.once = function (...args: any[]) {
            const [type, second, third] = args
            if (
                args.length === 3 &&
                typeof second === "object" &&
                !Array.isArray(second) &&
                second !== null
            ) {
                console.error("[MAP DEBUG] BAD MAP.ONCE!!!", type, second, third)
            }
            return (originalOnce as any)(...args)
        } as any"""

if old not in text:
    print("Old block not found exactly; patch not applied.")
    import sys
    sys.exit(1)

text = text.replace(old, new, 1)
path.write_text(text, "utf8")
print(f"Patched {path}")
