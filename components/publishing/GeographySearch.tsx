"use client"

import { useState, useRef, useEffect, useCallback } from "react"
import { Search, MapPin, Loader2, X } from "lucide-react"
import { useRouter } from "next/navigation"
import { useDebounce } from "@/hooks/use-debounce"
import { searchGeographies, type SearchGeoResult } from "@/app/actions/search-geographies"
import { Input } from "@/components/ui/input"

export function GeographySearch() {
    const router = useRouter()
    const [query, setQuery] = useState("")
    const [isOpen, setIsOpen] = useState(false)
    const [isLoading, setIsLoading] = useState(false)
    const [results, setResults] = useState<SearchGeoResult[]>([])

    const inputRef = useRef<HTMLInputElement>(null)
    const containerRef = useRef<HTMLDivElement>(null)

    const debouncedQuery = useDebounce(query, 250)

    useEffect(() => {
        function handleClickOutside(event: MouseEvent) {
            if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
                setIsOpen(false)
            }
        }
        document.addEventListener("mousedown", handleClickOutside)
        return () => document.removeEventListener("mousedown", handleClickOutside)
    }, [])

    useEffect(() => {
        async function fetchResults() {
            if (debouncedQuery.length < 2) {
                setResults([])
                setIsOpen(false)
                return
            }

            setIsLoading(true)
            try {
                const data = await searchGeographies(debouncedQuery)
                setResults(data)
                setIsOpen(data.length > 0)
            } catch (error) {
                console.error("Failed to search geographies:", error)
                setResults([])
            } finally {
                setIsLoading(false)
            }
        }

        fetchResults()
    }, [debouncedQuery])

    const handleSelect = useCallback((item: SearchGeoResult) => {
        setQuery("") // Clear the search after selection
        setIsOpen(false)
        router.push(item.url)
    }, [router])

    const handleClear = () => {
        setQuery("")
        setResults([])
        setIsOpen(false)
        inputRef.current?.focus()
    }

    const typeLabels: Record<string, string> = {
        state: "State",
        county: "County",
        city: "City",
        neighborhood: "Neighborhood",
        zip: "ZIP Code"
    }

    return (
        <div className="relative w-full sm:w-80" ref={containerRef}>
            <div className="relative flex items-center">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground z-10" />
                <Input
                    ref={inputRef}
                    type="text"
                    placeholder="Search any state, city, or ZIP..."
                    value={query}
                    onChange={(e) => {
                        setQuery(e.target.value)
                        if (!isOpen && e.target.value.length >= 2) setIsOpen(true)
                    }}
                    onFocus={() => {
                        if (query.length >= 2 && results.length > 0) setIsOpen(true)
                    }}
                    className="w-full bg-background border border-border rounded-lg pl-9 pr-8 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground placeholder:text-muted-foreground transition-all"
                />
                
                {isLoading && (
                    <div className="absolute right-3 top-1/2 -translate-y-1/2">
                        <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                    </div>
                )}
                
                {!isLoading && query && (
                    <button 
                        onClick={handleClear}
                        className="absolute right-2 top-1/2 -translate-y-1/2 p-1 rounded-full hover:bg-muted/50 text-muted-foreground transition-colors"
                    >
                        <X className="h-3.5 w-3.5" />
                    </button>
                )}
            </div>

            {isOpen && results.length > 0 && (
                <div className="absolute top-full left-0 right-0 mt-1.5 bg-card/95 backdrop-blur-md border border-border rounded-xl shadow-lg z-[200] max-h-80 overflow-y-auto overflow-x-hidden flex flex-col gap-1 p-1">
                    {results.map((item, idx) => (
                        <button
                            key={`${item.type}-${item.name}-${item.stateAbbr}-${idx}`}
                            className="w-full text-left px-3 py-2.5 rounded-lg hover:bg-accent/50 transition-colors flex items-center justify-between group"
                            onClick={() => handleSelect(item)}
                        >
                            <div className="flex items-center gap-3 overflow-hidden">
                                <div className="bg-muted text-muted-foreground p-1.5 rounded-md group-hover:bg-primary/10 group-hover:text-primary transition-colors shrink-0">
                                    <MapPin className="h-4 w-4" />
                                </div>
                                <div className="flex flex-col overflow-hidden">
                                    <span className="text-sm font-medium text-foreground truncate">
                                        {item.name}
                                        {item.type !== "state" && <span className="text-muted-foreground font-normal ml-1">, {item.stateAbbr}</span>}
                                    </span>
                                    <span className="text-[10px] uppercase tracking-wider text-muted-foreground/70 font-semibold mt-0.5">
                                        {typeLabels[item.type]}
                                    </span>
                                </div>
                            </div>
                        </button>
                    ))}
                </div>
            )}
        </div>
    )
}
