import type { SensorProps } from "@dnd-kit/core";
import { describe, expect, it, vi } from "vite-plus/test";
import { fakeTimers } from "../../test/render";
import { SIDEBAR_TOUCH_DRAG_HOLD_MS, SidebarPointerSensor } from "./Sidebar.pointer";

const sensors: SidebarPointerSensor[] = [];

function pointer(type: string, values: PointerEventInit = {}) {
  return new PointerEvent(type, {
    bubbles: true,
    cancelable: true,
    pointerId: 1,
    clientX: 10,
    clientY: 10,
    buttons: 1,
    ...values,
  });
}

function gesture(
  extraOptions: Partial<ConstructorParameters<typeof SidebarPointerSensor>[0]["options"]> = {},
  down: PointerEvent = pointer("pointerdown"),
) {
  const callbacks = {
    onStart: vi.fn(),
    onMove: vi.fn(),
    onEnd: vi.fn(),
    onCancel: vi.fn(),
    onAbort: vi.fn(),
    onPending: vi.fn(),
  };
  const onFinish = vi.fn();
  const props = {
    active: "thread",
    event: down,
    options: { distance: 6, onAttach: vi.fn(), onFinish, ...extraOptions },
    ...callbacks,
  } as unknown as SensorProps<ConstructorParameters<typeof SidebarPointerSensor>[0]["options"]>;
  const sensor = new SidebarPointerSensor(props);
  sensors.push(sensor);
  return { sensor, onFinish, ...callbacks };
}

function setup() {
  const stack = new DisposableStack();
  stack.use(fakeTimers());
  stack.defer(() => {
    for (const sensor of sensors.splice(0)) sensor.cancel();
    // Cancelled sensors keep suppressing clicks on the shared document until the next press.
    document.dispatchEvent(pointer("pointerdown"));
    vi.runAllTimers();
  });
  return stack;
}

describe("sidebar pointer lifecycle", () => {
  it("drops context when a zero-button move precedes pointerup", () => {
    using _environment = setup();
    const onDrop = vi.fn((point: { x: number; y: number }) => point.x > 50);
    const drag = gesture({ onMove: (point) => point.x > 50, onDrop });
    document.dispatchEvent(pointer("pointermove", { clientY: 20 }));
    document.dispatchEvent(pointer("pointermove", { clientX: 90, clientY: 30 }));

    // Native desktop releases can report the released button on the final move.
    document.dispatchEvent(pointer("pointermove", { buttons: 0, clientX: 94, clientY: 32 }));
    document.dispatchEvent(pointer("pointerup", { buttons: 0, clientX: 94, clientY: 32 }));
    document.dispatchEvent(pointer("pointermove", { buttons: 0, clientX: 100, clientY: 40 }));

    expect(onDrop).toHaveBeenCalledExactlyOnceWith({ x: 94, y: 32 });
    expect(drag.onCancel).toHaveBeenCalledOnce();
    expect(drag.onEnd).not.toHaveBeenCalled();
    expect(drag.onFinish).toHaveBeenCalledExactlyOnceWith(true);
  });

  // regression: #17373 a zero-button release outside a drop target must cancel, not reorder
  it("cancels a missed sidebar release without committing a reorder", () => {
    using _environment = setup();
    const onDrop = vi.fn((point: { x: number; y: number }) => point.x > 50);
    const drag = gesture({ onMove: (point) => point.x > 50, onDrop });
    document.dispatchEvent(pointer("pointermove", { clientX: 90, clientY: 30 }));
    document.dispatchEvent(pointer("pointermove", { buttons: 0, clientX: 30, clientY: 40 }));
    document.dispatchEvent(pointer("pointerup", { buttons: 0, clientX: 30, clientY: 40 }));

    expect(onDrop).toHaveBeenCalledExactlyOnceWith({ x: 30, y: 40 });
    expect(drag.onCancel).toHaveBeenCalledOnce();
    expect(drag.onEnd).not.toHaveBeenCalled();
    expect(drag.onFinish).toHaveBeenCalledExactlyOnceWith(true);
  });
  // A swipe that scrolls nothing natively (a pull past the list's top) is never cancelled by the
  // browser; it must not start a sort drag that reorders rows and autoscrolls the list.
  it("lets a quick touch swipe scroll instead of starting a drag", () => {
    using _environment = setup();
    const drag = gesture({}, pointer("pointerdown", { pointerType: "touch" }));
    document.dispatchEvent(pointer("pointermove", { pointerType: "touch", clientY: 60 }));
    document.dispatchEvent(pointer("pointermove", { pointerType: "touch", clientY: 120 }));

    expect(drag.onStart).not.toHaveBeenCalled();
    expect(drag.onMove).not.toHaveBeenCalled();
    expect(drag.onAbort).toHaveBeenCalledOnce();
    expect(drag.onFinish).toHaveBeenCalledExactlyOnceWith(false);
  });

  it("starts a touch drag after a press-and-hold", () => {
    using _environment = setup();
    const down = pointer("pointerdown", { pointerType: "touch" });
    const drag = gesture({}, down);
    const held = pointer("pointermove", { pointerType: "touch", clientY: 60 });
    Object.defineProperty(held, "timeStamp", {
      value: down.timeStamp + SIDEBAR_TOUCH_DRAG_HOLD_MS,
    });
    document.dispatchEvent(held);

    expect(drag.onStart).toHaveBeenCalledOnce();
    expect(drag.onAbort).not.toHaveBeenCalled();
  });

  it("starts a mouse drag on movement alone", () => {
    using _environment = setup();
    const drag = gesture();
    document.dispatchEvent(pointer("pointermove", { clientY: 60 }));

    expect(drag.onStart).toHaveBeenCalledOnce();
  });
});
