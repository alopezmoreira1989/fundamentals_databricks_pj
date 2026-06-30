// One intrinsic-value method as a Bear/Mid/Bull row — port of _render_iv_scenario_row:
// the value triplet, the MoS triplet, and the range bar.

import { EM_DASH, fmtMetric, fmtMos, isMissing } from "@/lib/format";
import type { Scenario } from "@/lib/valuation";
import IvBar from "./IvBar";

export default function IvScenarioRow({ s, price }: { s: Scenario; price: number | null }) {
  const vfmt = (v: number | null) => (!isMissing(v) && v > 0 ? fmtMetric(v, s.unit) : EM_DASH);
  const mfmt = (v: number | null) => (!isMissing(v) ? fmtMos(v) : EM_DASH);
  return (
    <>
      <div className="iv-row">
        <div className="m-label">{s.label}</div>
        <div className="v v-bear">{vfmt(s.bear)}</div>
        <div className="v v-mid">{vfmt(s.mid)}</div>
        <div className="v v-bull">{vfmt(s.bull)}</div>
      </div>
      <div className="iv-mos-row">
        <div />
        <div className="v">{mfmt(s.bearMos)}</div>
        <div className="v">{mfmt(s.midMos)}</div>
        <div className="v">{mfmt(s.bullMos)}</div>
      </div>
      <IvBar bear={s.bear} mid={s.mid} bull={s.bull} price={price} />
    </>
  );
}
