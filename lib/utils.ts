import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function getZoomForRes(res: number): number {
  if (res <= 7) return 10.5
  if (res === 8) return 12
  if (res === 9) return 13.2
  if (res === 10) return 14.5
  return 15.5 // Res 11+
}
