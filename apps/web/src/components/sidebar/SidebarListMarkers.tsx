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

/**
 * Opening a shelf scrolls its first row up under the stacked headers, so the shelf reads from its
 * own header down. A shelf shorter than the viewport rises as far as the list can scroll.
 */
function scrollShelfToTop(header: HTMLElement, stackIndex: number) {
  const firstRow = header.nextElementSibling;
  const viewport = header.closest<HTMLElement>("[data-slot=scroll-area-viewport]");
  if (!(firstRow instanceof HTMLElement) || viewport === null) return;
  // An empty shelf is followed by the next sticky header, whose box is wherever it is pinned.
  if (firstRow.querySelector('[data-sidebar-row^="shelf:"]') !== null) return;
  const rowTop = firstRow.getBoundingClientRect().top - viewport.getBoundingClientRect().top;
  viewport.scrollTo({
    top: viewport.scrollTop + rowTop - (stackIndex + 1) * SHELF_HEADER_HEIGHT_PX,
    behavior: "smooth",
  });
}

export function SidebarSectionHeader(props: {
  marker: "working-header" | "snoozed-header" | "settled-header";
  label: string;
  /** A muted trailing count. */
  count: number;
  className?: string;
  dragging?: boolean;
  isDropTarget?: boolean;
  /**
   * Where this header sits among the shelf headers shown, so they stack instead of covering each
   * other: each sticks below the ones before it at the top and above the ones after it at the bottom.
   */
  stack: { index: number; count: number };
  toggle: { expanded: boolean; onToggle: () => void };
}) {
  const shelf =
    props.marker === "working-header"
      ? "working"
      : props.marker === "snoozed-header"
        ? "snoozed"
        : "settled";
  return (
    <SortableSidebarMarker
      marker={props.marker}
      data-testid={`sidebar-${props.marker}`}
      // Shelf headers stay in view while the list scrolls, so their counts are always readable.
      className={cn("sticky z-10 mx-0.5 h-8 bg-sidebar", props.className)}
      style={{
        top: props.stack.index * SHELF_HEADER_HEIGHT_PX,
        bottom: (props.stack.count - 1 - props.stack.index) * SHELF_HEADER_HEIGHT_PX,
      }}
    >
      <CollapsibleSectionHeader
        data-sidebar-row={`shelf:${shelf}`}
        onClick={(event) => {
          const expanding = !props.toggle.expanded;
          const header = event.currentTarget.closest("li");
          props.toggle.onToggle();
          if (expanding && header !== null) {
            // Wait for the shelf's rows to render before measuring where they start.
            requestAnimationFrame(() =>
              requestAnimationFrame(() => scrollShelfToTop(header, props.stack.index)),
            );
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
    </SortableSidebarMarker>
  );
}
