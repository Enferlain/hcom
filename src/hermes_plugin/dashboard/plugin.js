/**
 * hcom Mesh — Hermes Desktop plugin
 *
 * Statusbar chip after the llm-usage widget: unread mesh mail for THIS
 * session's hcom identity. Shows tower icon + count when mail waits,
 * nothing when clean. Popover on click: who's waiting and a "Drain now"
 * button that pokes the plugin's notify port — the same wake path a mesh
 * send uses (policy-gated inside the plugin: unread check, intent gate,
 * debounce; the button cannot bypass any of it).
 *
 * Data via the hcom plugin's Python backend at /api/plugins/hcom/
 * (ctx.rest → /status, /drain). Unbound sessions render nothing.
 */

import {
  Button,
  Codicon,
  Popover,
  PopoverContent,
  PopoverTrigger,
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

function useMeshStatus(rest, sessionId) {
  return useQuery({
    queryKey: [...QUERY_KEY, sessionId],
    enabled: !!sessionId && !!rest,
    queryFn: () => rest(`/status?session_id=${encodeURIComponent(sessionId)}`, {
      timeoutMs: REST_TIMEOUT_MS,
    }),
    refetchInterval: POLL_MS,
    refetchIntervalInBackground: true,
    staleTime: 10_000,
    retry: 1,
  })
}

function MeshChip({ rest }) {
  const sessionId = useSessionId()
  const q = useMeshStatus(rest, sessionId)

  if (!sessionId || !q.data || !q.data.bound || !q.data.has_unread) return null
  const { count, latest_from, name } = q.data

  return jsxs(
    Popover,
    {
      children: [
        jsxs(PopoverTrigger, {
          asChild: true,
          children: [
            jsx('button', {
              'data-testid': 'hcom-chip',
              title: `hcom mesh: ${count} unread for ${name}${latest_from ? ` (latest: ${latest_from})` : ''} — click for actions`,
              style: {
                display: 'inline-flex',
                alignItems: 'center',
                gap: '4px',
                padding: '0 6px',
                border: 'none',
                background: 'transparent',
                cursor: 'pointer',
                color: 'var(--ui-text-secondary)',
                fontSize: '11px',
              },
              children: [
                jsx(Codicon, { name: 'radio-tower', style: { fontSize: '13px' } }),
                `hcom ${count}`,
              ],
            }),
          ],
        }),
        jsx(PopoverContent, {
          align: 'end',
          children: jsx(MeshPaneBody, { rest, sessionId }),
        }),
      ],
    },
  )
}

function MeshPaneBody({ rest, sessionId }) {
  const q = useQuery({
    queryKey: [...QUERY_KEY, sessionId],
    enabled: !!sessionId,
    queryFn: () => rest(`/status?session_id=${encodeURIComponent(sessionId)}`, {
      timeoutMs: REST_TIMEOUT_MS,
    }),
    staleTime: 0,
  })
  const refetch = q.refetch

  if (q.isLoading || !q.data || !q.data.bound) {
    return jsx('div', { style: { padding: '10px 12px' }, children: '…' })
  }
  const { count, latest_from, name, has_request } = q.data

  return jsxs('div', {
    style: {
      display: 'flex',
      flexDirection: 'column',
      gap: '8px',
      padding: '10px 12px',
      minWidth: '220px',
    },
    children: [
      jsxs('div', {
        style: { fontWeight: 600, fontSize: '12px' },
        children: ['hcom mesh — ', name],
      }),
      jsxs('div', {
        style: { color: 'var(--ui-text-secondary)', fontSize: '12px' },
        children: [
          count === 0
            ? 'no unread mail'
            : `${count} unread${latest_from ? ` (latest: ${latest_from})` : ''}`,
        ],
      }),
      jsx(Button, {
        variant: 'outline',
        size: 'sm',
        disabled: count === 0,
        onClick: async () => {
          await rest('/drain', { method: 'POST', body: { session_id: sessionId } })
          refetch()
        },
        children: 'Drain now',
      }),
      count > 0 && !has_request
        ? jsx('div', {
            style: { color: 'var(--ui-text-quaternary)', fontSize: '11px' },
            children: 'inform mail — drains at your next message, or wake it now',
          })
        : null,
    ],
  })
}

export default {
  id: 'hcom',
  name: 'hcom Mesh',
  register(ctx) {
    ctx.register({
      id: 'chip',
      area: 'statusBar.right',
      order: 130, // right after llm-usage (125)
      render: () => jsx(MeshChip, { rest: ctx.rest }),
    })
  },
}
