import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'

export interface VolumeBucket {
  label:     string
  completed: number
  failed:    number
}

interface Props {
  data:    VolumeBucket[]
  height?: number
  barSize?: number
}

export function VolumeChart({ data, height = 160, barSize = 8 }: Props) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} barSize={barSize}>
        <XAxis dataKey="label" tick={{ fill: 'var(--text-muted)', fontSize: 10 }} axisLine={false} tickLine={false} />
        <YAxis hide />
        <Tooltip
          contentStyle={{ background: 'var(--bg-raised)', border: '1px solid var(--border)', borderRadius: 6, fontSize: 12 }}
          labelStyle={{ color: 'var(--text-secondary)' }}
          itemStyle={{ color: 'var(--text-primary)' }}
        />
        <Bar dataKey="completed" stackId="a" fill="var(--green)" radius={[2, 2, 0, 0]} />
        <Bar dataKey="failed"    stackId="a" fill="var(--red)"   radius={[2, 2, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}
