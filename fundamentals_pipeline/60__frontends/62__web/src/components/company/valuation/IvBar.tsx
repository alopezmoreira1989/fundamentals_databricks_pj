// Mini bear→bull range bar for one IV scenario method — port of _iv_bar_row.

import { ivBarGeom } from "@/lib/valuation";

export default function IvBar({
  bear,
  mid,
  bull,
  price,
}: {
  bear: number | null;
  mid: number | null;
  bull: number | null;
  price: number | null;
}) {
  const g = ivBarGeom(bear, mid, bull, price);
  if (!g) return null;
  return (
    <div className="iv-bar-row">
      <div className="iv-bar">
        <div className="seg" style={{ left: `${g.segLeft.toFixed(1)}%`, width: `${g.segWidth.toFixed(1)}%` }} />
        {g.marks.map((m, i) => (
          <div key={i} className={`mk ${m.cls}`} style={{ left: `${m.left.toFixed(1)}%` }} />
        ))}
      </div>
    </div>
  );
}
