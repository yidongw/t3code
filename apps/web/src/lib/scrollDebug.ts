/**
 * Opt-in on-screen trace of the chat timeline's scrolling, for scroll bugs that only a real
 * device shows (iOS Safari touch, bounce, status-bar taps). `?scrolldebug=1` turns it on for the
 * tab, `?scrolldebug=0` off. Off, every export is a no-op.
 */

const STORAGE_KEY = "t3code:scroll-debug";
const MAX_LINES = 16;

function readEnabled(): boolean {
  try {
    const param = new URLSearchParams(window.location.search).get("scrolldebug");
    if (param === "1") window.sessionStorage.setItem(STORAGE_KEY, "1");
    if (param === "0") window.sessionStorage.removeItem(STORAGE_KEY);
    return window.sessionStorage.getItem(STORAGE_KEY) === "1";
  } catch {
    return false;
  }
}

let enabled: boolean | null = null;
export function scrollDebugEnabled(): boolean {
  if (enabled === null) enabled = typeof window !== "undefined" && readEnabled();
  return enabled;
}

const lines: string[] = [];
let overlay: HTMLPreElement | null = null;
let startedAt = 0;

function render() {
  if (overlay === null) {
    overlay = document.createElement("pre");
    overlay.setAttribute("aria-hidden", "true");
    Object.assign(overlay.style, {
      position: "fixed",
      left: "4px",
      right: "4px",
      top: "calc(env(safe-area-inset-top) + 44px)",
      zIndex: "2147483647",
      margin: "0",
      padding: "4px 6px",
      font: "10px/1.3 ui-monospace, monospace",
      color: "#fff",
      background: "rgba(0,0,0,0.78)",
      borderRadius: "6px",
      pointerEvents: "none",
      whiteSpace: "pre-wrap",
      wordBreak: "break-all",
    });
    document.body.append(overlay);
  }
  overlay.textContent = lines.join("\n");
}

/** Appends one line; `withCaller` adds where in the bundle the call came from. */
export function scrollDebugLog(message: string, withCaller = false): void {
  if (!scrollDebugEnabled()) return;
  if (startedAt === 0) startedAt = performance.now();
  const ms = Math.round(performance.now() - startedAt);
  let caller = "";
  if (withCaller) {
    const frames = (new Error().stack ?? "").split("\n").slice(2, 6);
    caller = frames
      .map((frame) => /([^/]+\.js):(\d+):(\d+)/.exec(frame))
      .filter((match) => match !== null)
      .map((match) => `${match[1]}:${match[2]}:${match[3]}`)
      .join(" < ");
  }
  lines.push(`${ms} ${message}${caller ? ` @ ${caller}` : ""}`);
  if (lines.length > MAX_LINES) lines.splice(0, lines.length - MAX_LINES);
  render();
}

/**
 * Traces touches, scroll positions and every script write to the node's scroll position.
 * Returns the detach.
 */
export function attachScrollDebug(node: HTMLElement): () => void {
  if (!scrollDebugEnabled()) return () => {};
  const position = () =>
    `top=${Math.round(node.scrollTop)} h=${node.scrollHeight} vh=${node.clientHeight}`;
  const scrollTop = Object.getOwnPropertyDescriptor(Element.prototype, "scrollTop");
  const scrollTo = node.scrollTo.bind(node);
  const scrollBy = node.scrollBy.bind(node);
  if (scrollTop?.get && scrollTop.set) {
    const { get, set } = scrollTop;
    Object.defineProperty(node, "scrollTop", {
      configurable: true,
      get() {
        return get.call(this);
      },
      set(value: number) {
        scrollDebugLog(`SET scrollTop=${Math.round(value)} (${position()})`, true);
        set.call(this, value);
      },
    });
  }
  node.scrollTo = ((...args: Parameters<HTMLElement["scrollTo"]>) => {
    scrollDebugLog(`SCROLLTO ${JSON.stringify(args)} (${position()})`, true);
    scrollTo(...args);
  }) as HTMLElement["scrollTo"];
  node.scrollBy = ((...args: Parameters<HTMLElement["scrollBy"]>) => {
    scrollDebugLog(`SCROLLBY ${JSON.stringify(args)} (${position()})`, true);
    scrollBy(...args);
  }) as HTMLElement["scrollBy"];
  let lastLogged = "";
  const onScroll = () => {
    const now = position();
    if (now !== lastLogged) {
      lastLogged = now;
      scrollDebugLog(`scroll ${now}`);
    }
  };
  const onTouch = (event: TouchEvent) => {
    const touch = event.touches[0] ?? event.changedTouches[0];
    scrollDebugLog(`${event.type} y=${Math.round(touch?.clientY ?? -1)} ${position()}`);
  };
  const onWindowScroll = () => scrollDebugLog(`window scrollY=${Math.round(window.scrollY)}`);
  node.addEventListener("scroll", onScroll, { passive: true });
  node.addEventListener("touchstart", onTouch, { passive: true });
  node.addEventListener("touchend", onTouch, { passive: true });
  window.addEventListener("scroll", onWindowScroll, { passive: true });
  scrollDebugLog(`attached ${position()}`);
  return () => {
    node.removeEventListener("scroll", onScroll);
    node.removeEventListener("touchstart", onTouch);
    node.removeEventListener("touchend", onTouch);
    window.removeEventListener("scroll", onWindowScroll);
    delete (node as { scrollTop?: number }).scrollTop;
    node.scrollTo = scrollTo;
    node.scrollBy = scrollBy;
  };
}
