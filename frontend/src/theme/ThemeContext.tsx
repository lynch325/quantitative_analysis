import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from 'react'

export type ThemeMode = 'dark' | 'light'

/** 配色方案：只换底色层次 / 强调色 / 文字色温；涨跌色固定 A 股口径（红涨绿跌） */
export type ThemeScheme = 'indigo' | 'bloomberg' | 'tradingview' | 'eikon' | 'gold'

export const THEME_SCHEMES: ReadonlyArray<{ id: ThemeScheme; label: string }> = [
  { id: 'indigo', label: '极夜靛蓝' },
  { id: 'bloomberg', label: '彭博终端' },
  { id: 'tradingview', label: 'TradingView' },
  { id: 'eikon', label: '路透 Eikon' },
  { id: 'gold', label: '暗金' },
]

/** 图表吃具体色值（不吃 CSS 变量），故强调色在 JS 侧再维护一份 */
type AccentSet = Pick<ChartPalette, 'accent' | 'amber' | 'teal' | 'violet'>

const SCHEME_ACCENTS: Record<ThemeScheme, { dark: AccentSet; light: AccentSet }> = {
  indigo: {
    dark: { accent: '#818cf8', amber: '#fbbf24', teal: '#34d399', violet: '#a78bfa' },
    light: { accent: '#6366f1', amber: '#d97706', teal: '#0d9488', violet: '#7c3aed' },
  },
  bloomberg: {
    dark: { accent: '#ff8000', amber: '#ffb020', teal: '#00aeef', violet: '#ff9d3d' },
    light: { accent: '#d96600', amber: '#a16207', teal: '#0369a1', violet: '#c2410c' },
  },
  tradingview: {
    dark: { accent: '#2962ff', amber: '#f7931a', teal: '#26a69a', violet: '#7e57c2' },
    light: { accent: '#1e53e5', amber: '#b45309', teal: '#0f8b7e', violet: '#5e35b1' },
  },
  eikon: {
    dark: { accent: '#ff7a00', amber: '#ffb547', teal: '#22b8b8', violet: '#8a7dff' },
    light: { accent: '#d95e00', amber: '#a16207', teal: '#0e7490', violet: '#6d28d9' },
  },
  gold: {
    dark: { accent: '#c9a227', amber: '#e6c34a', teal: '#4ea8a0', violet: '#a8894f' },
    light: { accent: '#a07d16', amber: '#8a6d0b', teal: '#0f766e', violet: '#6b5a2a' },
  },
}

/** lightweight-charts 需要具体色值，不能吃 CSS 变量；两套调色板随主题切换，图表组件依赖 palette 重建 */
export interface ChartPalette {
  text: string
  gridVert: string
  gridHorz: string
  border: string
  crosshair: string
  labelBg: string
  up: string
  down: string
  upSoft: string
  downSoft: string
  accent: string
  amber: string
  teal: string
  violet: string
  zeroLine: string
}

const DARK: ChartPalette = {
  text: '#93a0b8',
  gridVert: 'rgba(148, 163, 184, 0.08)',
  gridHorz: 'rgba(148, 163, 184, 0.12)',
  border: '#2a3650',
  crosshair: '#64748b',
  labelBg: '#334155',
  up: '#f87171',
  down: '#4ade80',
  upSoft: 'rgba(248, 113, 113, 0.7)',
  downSoft: 'rgba(74, 222, 128, 0.7)',
  accent: '#818cf8',
  amber: '#fbbf24',
  teal: '#34d399',
  violet: '#a78bfa',
  zeroLine: '#475569',
}

const LIGHT: ChartPalette = {
  text: '#5b6577',
  gridVert: 'rgba(15, 23, 42, 0.06)',
  gridHorz: 'rgba(15, 23, 42, 0.1)',
  border: '#d7dde9',
  crosshair: '#94a3b8',
  labelBg: '#475569',
  up: '#dc2626',
  down: '#16a34a',
  upSoft: 'rgba(220, 38, 38, 0.65)',
  downSoft: 'rgba(22, 163, 74, 0.65)',
  accent: '#6366f1',
  amber: '#d97706',
  teal: '#0d9488',
  violet: '#7c3aed',
  zeroLine: '#94a3b8',
}

interface ThemeCtx {
  mode: ThemeMode
  toggle: () => void
  scheme: ThemeScheme
  setScheme: (s: ThemeScheme) => void
  palette: ChartPalette
}

const Ctx = createContext<ThemeCtx>({
  mode: 'dark',
  toggle: () => {},
  scheme: 'indigo',
  setScheme: () => {},
  palette: DARK,
})

const THEME_STORAGE_KEY = 'qa-theme'
const SCHEME_STORAGE_KEY = 'qa-scheme'

function initialTheme(): ThemeMode {
  try {
    return localStorage.getItem(THEME_STORAGE_KEY) === 'light' ? 'light' : 'dark'
  } catch {
    return 'dark'
  }
}

function initialScheme(): ThemeScheme {
  try {
    const saved = localStorage.getItem(SCHEME_STORAGE_KEY)
    if (saved && THEME_SCHEMES.some((s) => s.id === saved)) return saved as ThemeScheme
  } catch {
    // localStorage 不可用（隐私模式）时退回默认配色
  }
  return 'indigo'
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [mode, setMode] = useState<ThemeMode>(initialTheme)
  const [scheme, setScheme] = useState<ThemeScheme>(initialScheme)

  useEffect(() => {
    const root = document.documentElement
    root.setAttribute('data-theme', mode)
    root.setAttribute('data-bs-theme', mode)
    root.setAttribute('data-scheme', scheme)
    try {
      localStorage.setItem(THEME_STORAGE_KEY, mode)
      localStorage.setItem(SCHEME_STORAGE_KEY, scheme)
    } catch {
      // 隐私模式等场景 localStorage 不可用时静默降级
    }
  }, [mode, scheme])

  const value = useMemo<ThemeCtx>(
    () => ({
      mode,
      toggle: () => setMode((m) => (m === 'dark' ? 'light' : 'dark')),
      scheme,
      setScheme,
      palette: { ...(mode === 'dark' ? DARK : LIGHT), ...SCHEME_ACCENTS[scheme][mode] },
    }),
    [mode, scheme],
  )

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>
}

export function useTheme(): ThemeCtx {
  return useContext(Ctx)
}
