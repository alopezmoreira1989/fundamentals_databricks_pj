// Valuation football field — horizontal range chart of IV methods vs market price. Port of
// render_valuation_football_field (the SVG geometry lives in lib/valuation.ts).

import type { CatGroup } from "@/lib/queries/company";
import { footballBars, footballFieldSvg, ivLatestMap } from "@/lib/valuation";

export default function FootballField({
  categories,
  period,
  price,
}: {
  categories: CatGroup[];
  period: string;
  price: number | null;
}) {
  const ivLatest = ivLatestMap(categories);
  const bars = footballBars(ivLatest, period);
  const svg = footballFieldSvg(bars, price);
  if (!svg) return null;

  const notes = ["bars span the bear→bull scenario range per method · dot = mid case"];
  if (bars.length === 1) notes.push("only one method available");
  if (!(price !== null && Number.isFinite(price) && price > 0)) notes.push("no market price available");

  return (
    <div className="ff-card">
      <div className="ff-head">
        <h3>Valuation football field</h3>
        <div className="sub">{period} · intrinsic value vs price</div>
      </div>
      <div className="ff-plot" dangerouslySetInnerHTML={{ __html: svg }} />
      <div className="ff-caption">{notes.join(" · ")}</div>
    </div>
  );
}
