import { Link } from 'react-router-dom'
import { cn } from '../../lib/cn'

/** 数据源偶发把缺失名称序列化成字符串 'None'/'null'，直接渲染会露出脏值 */
const EMPTY_NAMES = new Set(['', 'none', 'null', 'nan', 'undefined'])

function cleanName(name?: string | null): string {
  const raw = (name ?? '').trim()
  return EMPTY_NAMES.has(raw.toLowerCase()) ? '' : raw
}

interface StockLinkProps {
  /** ts_code 形式（600000.SH）或同花顺 thscode */
  code: string
  name?: string | null
  /** 显示的文本；默认 `代码 名称`，传 showCode=false 只显示名称 */
  showCode?: boolean
  /** 只显示代码（代码列与名称列并存时避免名称重复渲染） */
  showName?: boolean
  className?: string
}

/** 个股链接：跳转到既有详情页 /stock/:tsCode（悬停显主题色，下划线由下划线偏移衬托） */
export function StockLink({
  code,
  name,
  showCode = true,
  showName = true,
  className,
}: StockLinkProps) {
  const displayName = cleanName(name)
  const label = displayName || code
  return (
    <Link
      to={`/stock/${encodeURIComponent(code)}`}
      title={`查看 ${label} 详情`}
      className={cn(
        'group inline-flex items-baseline gap-1.5 rounded-sm transition-colors hover:text-accent',
        className,
      )}
    >
      {showCode ? <span className="num text-fg-muted group-hover:text-accent/80">{code}</span> : null}
      {showName ? (
        <span className={cn(showCode && 'text-fg-secondary')}>{displayName}</span>
      ) : null}
      {!showCode && !showName ? <span className="num">{code}</span> : null}
    </Link>
  )
}
