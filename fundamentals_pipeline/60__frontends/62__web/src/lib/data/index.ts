// Public surface of the shared data layer. Import from "@/lib/data" rather than reaching
// into individual modules.
export {
  ARTIFACT_FILES,
  META_FILE,
  ArtifactLoadError,
  type ArtifactName,
  type TickerMeta,
  type DashboardMeta,
  type FyRange,
} from "./types";
export { ensureArtifact, ensureArtifacts, RELEASE_BASE } from "./artifacts";
export { loadMeta } from "./meta";
export { resolveDataTtlSeconds, resolveDataTtlMs } from "./ttl";
