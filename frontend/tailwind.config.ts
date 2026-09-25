import type { Config } from 'tailwindcss'

/**
 * 新版设计体系（参考 tick-stock-panel 的语义 token 架构，品牌色沿用本项目靛蓝）。
 *
 * 约定：
 * - preflight 关闭：与既有 Bootstrap 页面共存，旧页面不受影响
 * - 语义色全部映射 HSL CSS 变量（见 src/styles/tsp.css），随 data-theme 自动切换
 * - 暗色不使用阴影，靠 1px 边框分层
 * - 数字一律 font-mono + tabular-nums（.num 工具类）
 */
const config: Config = {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  darkMode: ['class', '[data-theme="dark"]'],
  corePlugins: {
    preflight: false,
  },
  theme: {
    extend: {
      colors: {
        canvas: 'hsl(var(--ts-canvas) / <alpha-value>)',
        surface: 'hsl(var(--ts-surface) / <alpha-value>)',
        elevated: 'hsl(var(--ts-elevated) / <alpha-value>)',
        line: 'hsl(var(--ts-border) / <alpha-value>)',
        'fg-primary': 'hsl(var(--ts-fg-primary) / <alpha-value>)',
        'fg-secondary': 'hsl(var(--ts-fg-secondary) / <alpha-value>)',
        'fg-muted': 'hsl(var(--ts-fg-muted) / <alpha-value>)',
        accent: 'hsl(var(--ts-accent) / <alpha-value>)',
        bull: 'hsl(var(--ts-bull) / <alpha-value>)',
        bear: 'hsl(var(--ts-bear) / <alpha-value>)',
        warning: 'hsl(var(--ts-warning) / <alpha-value>)',
        danger: 'hsl(var(--ts-danger) / <alpha-value>)',
      },
      borderRadius: {
        card: '8px',
        btn: '6px',
        input: '4px',
        dialog: '12px',
      },
      // 与 theme.css 的 body 栈保持一致：中文字体顺序不同会在切路由时看到字重变化
      fontFamily: {
        sans: [
          'Inter',
          '"PingFang SC"',
          '"HarmonyOS Sans SC"',
          '"Microsoft YaHei"',
          '"Noto Sans SC"',
          'system-ui',
          'sans-serif',
        ],
        // 末段补符号与中文回退：▲/▼（U+25B2/25BC）不在 JetBrains Mono 覆盖范围内，
        // 缺字形时会掉到别的字体的替代字形，箭头与数字基线会抖
        mono: [
          '"JetBrains Mono"',
          'ui-monospace',
          'SFMono-Regular',
          'Menlo',
          'Consolas',
          '"Segoe UI Symbol"',
          '"Microsoft YaHei"',
          'monospace',
        ],
      },
      // 中文字面框高，正文行高需比拉丁文更松；下限 11px（<12px 中文会笔画粘连，
      // 但表头/徽章受密度约束，11px 作为可接受的折中）
      fontSize: {
        '2xs': ['11px', '15px'],
        xs: ['12px', '17px'],
        sm: ['13px', '19px'],
        base: ['14px', '22px'],
        lg: ['16px', '24px'],
        xl: ['18px', '28px'],
      },
      lineHeight: {
        body: '1.65',
      },
      transitionTimingFunction: {
        smooth: 'cubic-bezier(0.16, 1, 0.3, 1)',
      },
    },
  },
  plugins: [],
}

export default config
