/**
 * hcom Mesh — Hermes Desktop plugin
 *
 * Statusbar chip (after llm-usage): unread mesh mail for THIS session's
 * hcom identity, labeled with the session TITLE (mesh codenames like
 * "tapa" are opaque to humans — the session list is the user's mental
 * model). Click opens a docked workspace panel (like llm-usage's) with
 * the waiting messages' summary and a Drain-now button that pokes the
 * plugin's notify port — the same policy-gated wake path a mesh send
 * uses (unread check, intent gate, debounce; never bypassed).
 *
 * Data via the hcom plugin's Python backend at /api/plugins/hcom/
 * (ctx.rest → /status, /drain). Unbound sessions render nothing.
 */

import {
  Button,
  Codicon,
  Tip,
  cn,
  haptic,
  host,
  useQuery,
  useValue,
} from '@hermes/plugin-sdk'
import { jsx, jsxs } from 'react/jsx-runtime'

const QUERY_KEY = ['hcom', 'status']
const POLL_MS = 30_000
const REST_TIMEOUT_MS = 8000

function useSessionId() {
  return useValue(host.state.activeSessionId)
}

function fetchStatus(rest, sessionId) {
  return rest(`/status?session_id=${encodeURIComponent(sessionId)}`, {
    timeoutMs: REST_TIMEOUT_MS,
  })
}

function useMeshStatus(rest, sessionId) {
  return useQuery({
    queryKey: [...QUERY_KEY, sessionId],
    enabled: !!sessionId && !!rest,
    queryFn: () => fetchStatus(rest, sessionId),
    refetchInterval: POLL_MS,
    refetchIntervalInBackground: true,
    staleTime: 10_000,
    retry: 1,
  })
}

/** The user-facing label: session title when known, else the mesh name. */
function identityLabel(data) {
  const title = data.session_title || ''
  if (title) return title.length > 28 ? `${title.slice(0, 28)}…` : title
  return data.name || 'mesh'
}

function MeshChip({ rest, onToggle, open }) {
  const sessionId = useSessionId()
  const q = useMeshStatus(rest, sessionId)

  const show = sessionId && q.data && q.data.bound && q.data.has_unread
  const count = show ? q.data.count : 0

  return jsx(Tip, {
    label: show
      ? `${count} mesh message${count === 1 ? '' : 's'} waiting for “${identityLabel(q.data)}”${q.data.latest_from ? ` · latest from ${q.data.latest_from}` : ''} · click to open`
      : 'hcom mesh — no unread mail',
    children: jsx('button', {
      type: 'button',
      className: cn(
        'inline-flex h-full items-center gap-1 px-1.5 text-[0.6875rem] transition-colors',
        'text-(--ui-text-tertiary) hover:bg-(--chrome-action-hover) hover:text-foreground',
        show ? '' : 'opacity-60',
      ),
      onClick: () => {
        haptic('tap')
        onToggle()
      },
      children: jsxs('span', {
        className: 'inline-flex items-center gap-1',
        children: [
          jsx(Codicon, { name: 'radio-tower', size: '0.75rem' }),
          show ? `hcom ${count}` : 'hcom',
        ],
      }),
    }),
  })
}

function MeshPanelBody({ rest, sessionId }) {
  const q = useQuery({
    queryKey: [...QUERY_KEY, sessionId],
    enabled: !!sessionId,
    queryFn: () => fetchStatus(rest, sessionId),
    staleTime: 0,
    refetchInterval: POLL_MS,
  })
  const refetch = q.refetch

  if (q.isLoading || !q.data || !q.data.bound) {
    return jsx('div', { style: { padding: '12px' }, children: '…' })
  }
  const { count, latest_from, name, session_title, has_request } = q.data

  return jsxs('div', {
    style: {
      display: 'flex',
      flexDirection: 'column',
      gap: '10px',
      padding: '14px 16px',
      minWidth: '260px',
    },
    children: [
      jsxs('div', {
        style: { fontWeight: 600, fontSize: '12px' },
        children: ['hcom mesh'],
      }),
      jsxs('div', {
        style: { color: 'var(--ui-text-secondary)', fontSize: '12px' },
        children: [
          'Session: ',
          jsx('b', { children: session_title || 'untitled' }),
          jsx('div', {
            style: { color: 'var(--ui-text-quaternary)', fontSize: '11px' },
            children: `mesh name: ${name}`,
          }),
        ],
      }),
      jsxs('div', {
        style: { color: 'var(--ui-text-secondary)', fontSize: '12px' },
        children: [
          count === 0
            ? 'No unread mail.'
            : `${count} unread${latest_from ? ` · latest from ${latest_from}` : ''}`,
        ],
      }),
      jsx(Button, {
        variant: 'outline',
        size: 'sm',
        disabled: count === 0,
        onClick: async () => {
          await rest('/drain', {
            method: 'POST',
            body: { session_id: sessionId },
            timeoutMs: REST_TIMEOUT_MS,
          })
          refetch()
        },
        children: 'Drain now',
      }),
      count > 0 && !has_request
        ? jsx('div', {
            style: { color: 'var(--ui-text-quaternary)', fontSize: '11px' },
            children:
              'inform mail — drains at your next message in this session, or wake it now',
          })
        : null,
    ],
  })
}

export default {
  id: 'hcom',
  name: 'hcom Mesh',
  register(ctx) {
    let disposePane = null
    let open = false
    const setOpen = (next) => {
      open = next === undefined ? !open : next
      if (open) {
        if (disposePane) return
        if (typeof host?.openWorkspace === 'function') {
          disposePane = host.openWorkspace('hcom-mesh', {
            dock: { pane: 'workspace', pos: 'right' },
            title: 'hcom Mesh',
            render: () => jsx(MeshPanelBody, { rest: ctx.rest, sessionId: null }),
            onClose: () => {
              disposePane = null
              open = false
            },
          })
        }
      } else if (disposePane) {
        disposePane()
        disposePane = null
      }
    }

    ctx.register({
      id: 'chip',
      area: 'statusBar.right',
      order: 130, // right after llm-usage (125)
      render: () =>
        jsx(MeshChip, {
          rest: ctx.rest,
          open,
          onToggle: () => setOpen(),
        }),
    })
  },
}
