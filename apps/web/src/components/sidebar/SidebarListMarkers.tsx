import { CollapsibleSectionHeader } from "../ui/collapsible-section-header";
import { useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { type CSSProperties, type ReactNode, useLayoutEffect } from "react";
import { cn } from "~/lib/utils";
import {
  animateSidebarLayoutChanges,
  sidebarMarkerId,
  type SidebarListMarker,
} from "../Sidebar.logic";
import { SIDEBAR_SORTABLE_DRAG_DISABLED } from "./SidebarThreadRow";

// h-8: one shelf header row.
const SHELF_HEADER_HEIGHT_PX = 32;

function SortableSidebarMarker(props: {
  marker: SidebarListMarker;
  className?: string;
  children?: ReactNode;
  revealOverflow?: boolean;
  style?: CSSProperties;
  "data-testid"?: string;
  "data-shelf-in-list"?: boolean;
}) {
  const { node, setNodeRef, transform, transition } = useSortable({
    id: sidebarMarkerId(props.marker),
    disabled: SIDEBAR_SORTABLE_DRAG_DISABLED,
    animateLayoutChanges: animateSidebarLayoutChanges,
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- keeps the projected drop box inside the scroll viewport as drag transforms move it
  useLayoutEffect(() => {
    if (props.revealOverflow && transform?.y !== undefined) {
      node.current?.firstElementChild?.scrollIntoView({ block: "nearest", inline: "nearest" });
    }
  }, [node, props.revealOverflow, transform?.y]);
  return (
    <li
      ref={setNodeRef}
      data-thread-selection-safe
      data-testid={props["data-testid"]}
      data-shelf-in-list={props["data-shelf-in-list"] ? "" : undefined}
      className={cn("list-none", props.className)}
      style={{
        ...props.style,
        transform: CSS.Translate.toString(transform),
        transition: props.marker.endsWith("-placeholder") ? "none" : transition,
        visibility: transform?.scaleY === 0 ? "hidden" : undefined,
      }}
    >
      {props.children}
    </li>
  );
}

export function SidebarSectionPlaceholder(props: {
  marker: "active-placeholder" | "settled-placeholder";
  label: string;
  showHint: boolean;
  isDropTarget: boolean;
}) {
  return (
    <SortableSidebarMarker
      marker={props.marker}
      data-testid={`sidebar-${props.marker}`}
      className="relative mx-0.5 -mb-px h-0"
      revealOverflow={props.marker === "settled-placeholder" && props.showHint}
    >
      {props.showHint ? (
        <div
          className={cn(
            "absolute inset-x-0 top-0 flex h-9 items-center justify-center rounded-md border border-dashed border-sidebar-foreground/25 text-xs text-sidebar-foreground/80",
            props.isDropTarget && "border-primary/40 bg-primary/5 text-primary",
          )}
        >
          {props.label}
        </div>
      ) : null}
    </SortableSidebarMarker>
  );
}

export function SidebarDragBoundary(props: {
  marker: "pinned-header" | "pinned-divider";
  label: string;
  visible: boolean;
  isDropTarget: boolean;
}) {
  return (
    <SortableSidebarMarker
      marker={props.marker}
      data-testid={`sidebar-${props.marker}`}
      className="pointer-events-none relative mx-0.5 -mb-px h-0"
    >
      {props.visible ? (
        <div className="sidebar-drag-boundary-label absolute inset-x-2 top-1 flex h-4 items-center gap-2">
          <span
            className={cn(
              "shrink-0 text-xs font-medium",
              props.isDropTarget ? "text-primary" : "text-sidebar-foreground/80",
            )}
          >
            {props.label}
          </span>
          <span
            aria-hidden
            className={cn(
              "h-px flex-1",
              props.isDropTarget ? "bg-primary/50" : "bg-sidebar-foreground/25",
            )}
          />
        </div>
      ) : null}
    </SortableSidebarMarker>
  );
}

type ShelfMarker = "working-header" | "snoozed-header" | "settled-header";

const shelfOf = (marker: ShelfMarker) =>
  marker === "working-header" ? "working" : marker === "snoozed-header" ? "snoozed" : "settled";

/**
 * Opening a shelf scrolls its first row up under the stacked headers, so the shelf reads from its
 * own header down. A shelf shorter than the viewport rises as far as the list can scroll.
 */
function scrollShelfToTop(marker: ShelfMarker) {
  const header = document.querySelector<HTMLElement>(
    `[data-testid="sidebar-${marker}"][data-shelf-in-list]`,
  );
  const firstRow = header?.nextElementSibling;
  const viewport = header?.closest<HTMLElement>("[data-slot=scroll-area-viewport]");
  if (!header || !(firstRow instanceof HTMLElement) || !viewport) return;
  // An empty shelf is followed by the next sticky header, whose box is wherever it is pinned.
  if (firstRow.querySelector('[data-sidebar-row^="shelf:"]') !== null) return;
  // Headers above this one stay stacked over the top of the list.
  let stacked = 1;
  for (const other of viewport.querySelectorAll("[data-shelf-in-list]")) {
    if (other === header) break;
    stacked += 1;
  }
  const rowTop = firstRow.getBoundingClientRect().top - viewport.getBoundingClientRect().top;
  const target = viewport.scrollTop + rowTop - stacked * SHELF_HEADER_HEIGHT_PX;
  // A short shelf at the end of the list cannot rise to the top on its own: pad the list's end
  // just enough. Closing a shelf drops the padding (`releaseShelfPadding`).
  const list = header.parentElement;
  const maxScroll = viewport.scrollHeight - viewport.clientHeight;
  if (list && target > maxScroll) {
    const current = Number.parseFloat(list.style.paddingBottom) || 0;
    list.style.paddingBottom = `${current + target - maxScroll}px`;
  }
  viewport.scrollTo({ top: target, behavior: "smooth" });
}

function releaseShelfPadding() {
  const list = document.querySelector<HTMLElement>("[data-shelf-in-list]")?.parentElement;
  if (list) list.style.paddingBottom = "";
}

type ShelfHeaderProps = {
  marker: ShelfMarker;
  label: string;
  /** A muted trailing count. */
  count: number;
  dragging?: boolean;
  isDropTarget?: boolean;
  toggle: { expanded: boolean; onToggle: () => void };
};

function ShelfHeaderButton(props: ShelfHeaderProps) {
  const shelf = shelfOf(props.marker);
  return (
    <CollapsibleSectionHeader
      data-sidebar-row={`shelf:${shelf}`}
      onClick={() => {
        const expanding = !props.toggle.expanded;
        if (!expanding) releaseShelfPadding();
        props.toggle.onToggle();
        if (expanding) {
          // Wait for the shelf's header and rows to render in the list before measuring them.
          requestAnimationFrame(() => requestAnimationFrame(() => scrollShelfToTop(props.marker)));
        }
      }}
      expanded={props.toggle.expanded}
      tone={
        props.isDropTarget
          ? "accent"
          : props.dragging
            ? "emphasized"
            : shelf === "snoozed"
              ? "muted"
              : "strong"
      }
      data-testid={`sidebar-${shelf}-shelf-toggle`}
      accessory={<span className="font-normal tabular-nums">{props.count}</span>}
    >
      {shelf === "snoozed" ? (
        props.label
      ) : (
        <span className="flex items-center gap-1.5">
          <span
            aria-hidden
            className={cn(
              "size-1.5 rounded-full",
              shelf === "working"
                ? "bg-success animate-pulse motion-reduce:animate-none"
                : "bg-sidebar-foreground/60",
            )}
          />
          {props.label}
        </span>
      )}
    </CollapsibleSectionHeader>
  );
}

/**
 * An open shelf's header, in the list above its rows. It stays in view while the list scrolls,
 * stacked with the other open shelves' headers. A docked (closed) shelf keeps only an empty
 * sortable marker here; its header is drawn by `SidebarDockedShelfHeaders` outside the list.
 */
export function SidebarSectionHeader(
  props: ShelfHeaderProps & {
    className?: string;
    docked: boolean;
    /** This header's place among the headers shown in the list, so they stack instead of overlap. */
    stack: { index: number; count: number };
  },
) {
  if (props.docked) {
    return (
      <SortableSidebarMarker
        marker={props.marker}
        data-testid={`sidebar-${props.marker}-slot`}
        className="h-0 overflow-hidden"
      />
    );
  }
  return (
    <SortableSidebarMarker
      marker={props.marker}
      data-testid={`sidebar-${props.marker}`}
      data-shelf-in-list
      className={cn("sticky z-10 mx-0.5 h-8 bg-sidebar", props.className)}
      style={{
        top: props.stack.index * SHELF_HEADER_HEIGHT_PX,
        bottom: (props.stack.count - 1 - props.stack.index) * SHELF_HEADER_HEIGHT_PX,
      }}
    >
      <ShelfHeaderButton {...props} />
    </SortableSidebarMarker>
  );
}

/**
 * Closed shelves' headers, docked under the scrolling list. Outside the scroller they cannot move
 * with it: iOS Safari repaints sticky elements a frame behind its scrolling, so in-list headers
 * jittered under every swipe at the list's end.
 */
export function SidebarDockedShelfHeaders(props: { headers: readonly ShelfHeaderProps[] }) {
  if (props.headers.length === 0) return null;
  return (
    <div data-testid="sidebar-docked-shelves" className="shrink-0 px-2">
      {props.headers.map((header) => (
        <div key={header.marker} data-testid={`sidebar-${header.marker}`} className="mx-0.5 h-8">
          <ShelfHeaderButton {...header} />
        </div>
      ))}
    </div>
  );
}
