import { useEffect, useRef, useState } from 'react'
import { Check, Palette } from 'lucide-react'
import { THEME_SCHEMES, useTheme } from '../../theme/ThemeContext'
import { cn } from '../../lib/cn'

/** 配色方案下拉：列出全部方案直接选，选择存 localStorage（见 ThemeContext） */
export function SchemeSelect() {
  const { scheme, setScheme } = useTheme()
  const [open, setOpen] = useState(false)
  const rootRef = useRef<HTMLDivElement>(null)

  // 点击外部与 Esc 关闭；卸载时移除监听，避免泄漏
  useEffect(() => {
    if (!open) return
    const onPointerDown = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setOpen(false)
    }
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onPointerDown)
    document.addEventListener('keydown', onKeyDown)
    return () => {
      document.removeEventListener('mousedown', onPointerDown)
      document.removeEventListener('keydown', onKeyDown)
    }
  }, [open])

  const current = THEME_SCHEMES.find((s) => s.id === scheme) ?? THEME_SCHEMES[0]

  return (
    <div className="scheme-select" ref={rootRef}>
      <button
        type="button"
        className="btn-ghost scheme-select-trigger"
        onClick={() => setOpen((v) => !v)}
        aria-haspopup="listbox"
        aria-expanded={open}
      >
        <span className="ico">
          <Palette size={14} strokeWidth={1.8} aria-hidden />
        </span>
        <span className="scheme-select-label">{current.label}</span>
      </button>
      {open ? (
        <ul className="scheme-select-menu" role="listbox" aria-label="配色方案">
          {THEME_SCHEMES.map((s) => (
            <li key={s.id}>
              <button
                type="button"
                role="option"
                aria-selected={s.id === scheme}
                className={cn('scheme-select-item', s.id === scheme && 'is-active')}
                onClick={() => {
                  setScheme(s.id)
                  setOpen(false)
                }}
              >
                <span className={cn('scheme-dot', `scheme-dot-${s.id}`)} aria-hidden />
                {s.label}
                {s.id === scheme ? <Check size={13} strokeWidth={2} aria-hidden /> : null}
              </button>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  )
}
