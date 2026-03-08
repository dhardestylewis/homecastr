/**
 * Lightweight RFC 4180-compliant CSV serializer.
 * No external dependencies.
 */

export interface CsvColumn<T> {
    key: keyof T
    header: string
    /** Optional value formatter. Raw value is used if omitted. */
    format?: (value: T[keyof T], row: T) => string
}

function escapeField(value: unknown): string {
    const s = value == null ? "" : String(value)
    // Quote if the field contains comma, double-quote, or newline
    if (s.includes(",") || s.includes('"') || s.includes("\n") || s.includes("\r")) {
        return `"${s.replace(/"/g, '""')}"`
    }
    return s
}

/**
 * Convert rows to a CSV string.
 *
 * @param columns  Column definitions (key + header + optional formatter)
 * @param rows     Data rows
 * @param comment  Optional comment block prepended as `# ...` lines
 */
export function toCsv<T extends Record<string, unknown>>(
    columns: CsvColumn<T>[],
    rows: T[],
    comment?: string,
): string {
    const lines: string[] = []

    // Comment header
    if (comment) {
        for (const line of comment.split("\n")) {
            lines.push(`# ${line}`)
        }
    }

    // Header row
    lines.push(columns.map(c => escapeField(c.header)).join(","))

    // Data rows
    for (const row of rows) {
        const fields = columns.map(c => {
            const raw = row[c.key]
            const formatted = c.format ? c.format(raw, row) : raw
            return escapeField(formatted)
        })
        lines.push(fields.join(","))
    }

    return lines.join("\r\n") + "\r\n"
}
