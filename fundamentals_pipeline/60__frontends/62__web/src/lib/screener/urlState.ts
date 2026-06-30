// URL state for the screener — every filter + the sort live in the query string (nuqs), so
// a filtered/sorted view is a shareable link that survives a hard reload. This is the React
// analogue of the Streamlit "Option A" state-in-URL fix (views/screener.py), but with a
// clean grammar: the app never hard-reloads on a filter change, so the encoding only has to
// be stable and round-trippable, not match Streamlit's byte-for-byte.

import { createParser, parseAsArrayOf, parseAsString, parseAsStringEnum } from "nuqs";
import { ALL_INDUSTRIES, ALL_SECTORS, DEFAULT_COLUMNS } from "./constants";

// Bucket selections: metric → labels. Encoded as `metric~l1,l2;metric2~l3` — `;`, `~`, `,`
// never occur in a metric name or a bucket label (labels use the en-dash "–", not a comma),
// so the split is unambiguous. nuqs percent-encodes the assembled value for the URL.
export const bucketsParser = createParser<Record<string, string[]>>({
  parse(value) {
    const out: Record<string, string[]> = {};
    if (!value) return out;
    for (const chunk of value.split(";")) {
      const sep = chunk.indexOf("~");
      if (sep === -1) continue;
      const metric = chunk.slice(0, sep);
      const labels = chunk.slice(sep + 1).split(",").filter(Boolean);
      if (metric && labels.length) out[metric] = labels;
    }
    return out;
  },
  serialize(obj) {
    return Object.entries(obj)
      .filter(([, labels]) => labels.length > 0)
      .map(([metric, labels]) => `${metric}~${labels.join(",")}`)
      .join(";");
  },
}).withDefault({});

/** Parser map for `useQueryStates`. Keys mirror the Streamlit query params (u/sec/ind/q/cols/b/sort/dir). */
export const screenerParsers = {
  u: parseAsString.withDefault("All"),
  sec: parseAsString.withDefault(ALL_SECTORS),
  ind: parseAsString.withDefault(ALL_INDUSTRIES),
  q: parseAsString.withDefault(""),
  cols: parseAsArrayOf(parseAsString, "|").withDefault([...DEFAULT_COLUMNS]),
  b: bucketsParser,
  sort: parseAsString.withDefault(""),
  dir: parseAsStringEnum(["asc", "desc"]).withDefault("asc"),
};
