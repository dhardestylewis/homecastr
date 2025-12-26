"use client"

import { useMemo } from "react"
import { Line, LineChart, XAxis, YAxis, CartesianGrid, ResponsiveContainer } from "recharts"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart"
import type { PropertyForecast } from "@/app/actions/property-forecast"

interface ForecastChartProps {
  acct: string
  data: PropertyForecast[]
}

export function ForecastChart({ acct, data }: ForecastChartProps) {
  const chartData = useMemo(() => {
    return data.map((item) => ({
      year: item.yr,
      valuation: item.valuation,
      is_imputed: item.is_imputed,
    }))
  }, [data])

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 0,
    }).format(value)
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Price History</CardTitle>
        <CardDescription>
          Property valuations for account <span className="font-mono">{acct}</span>
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer
          config={{
            valuation: {
              label: "Valuation",
              color: "hsl(var(--chart-1))",
            },
          }}
          className="h-[300px]"
        >
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis dataKey="year" className="text-xs" tick={{ fill: "hsl(var(--muted-foreground))" }} />
              <YAxis
                tickFormatter={formatCurrency}
                className="text-xs"
                tick={{ fill: "hsl(var(--muted-foreground))" }}
              />
              <ChartTooltip content={<ChartTooltipContent formatter={(value) => formatCurrency(value as number)} />} />
              <Line
                type="monotone"
                dataKey="valuation"
                stroke="var(--color-valuation)"
                strokeWidth={2}
                dot={(props) => {
                  const { cx, cy, payload } = props
                  return (
                    <circle
                      cx={cx}
                      cy={cy}
                      r={4}
                      fill={payload.is_imputed ? "hsl(var(--muted))" : "var(--color-valuation)"}
                      stroke={payload.is_imputed ? "hsl(var(--muted-foreground))" : "var(--color-valuation)"}
                      strokeWidth={1}
                    />
                  )
                }}
              />
            </LineChart>
          </ResponsiveContainer>
        </ChartContainer>
        <div className="mt-4 flex items-center gap-4 text-xs text-muted-foreground">
          <div className="flex items-center gap-1.5">
            <div className="h-3 w-3 rounded-full bg-[var(--color-valuation)]" />
            <span>Actual Value</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="h-3 w-3 rounded-full bg-muted border border-muted-foreground" />
            <span>Imputed Value</span>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
