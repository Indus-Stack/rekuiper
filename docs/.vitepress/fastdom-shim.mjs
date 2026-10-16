// Fastdom ESM Shim for VitePress & Mermaid
class FastDomShim {
  constructor() {
    this.reads = [];
    this.writes = [];
    this.raf = typeof window !== 'undefined' && window.requestAnimationFrame
      ? window.requestAnimationFrame.bind(window)
      : (cb) => setTimeout(cb, 16);
  }
  runTasks(tasks) {
    let task;
    while ((task = tasks.shift())) task();
  }
  measure(fn, ctx) {
    const task = ctx ? fn.bind(ctx) : fn;
    this.reads.push(task);
    this.scheduleFlush();
    return task;
  }
  mutate(fn, ctx) {
    const task = ctx ? fn.bind(ctx) : fn;
    this.writes.push(task);
    this.scheduleFlush();
    return task;
  }
  clear(task) {
    return this.remove(this.reads, task) || this.remove(this.writes, task);
  }
  remove(arr, item) {
    const idx = arr.indexOf(item);
    return idx !== -1 && !!arr.splice(idx, 1);
  }
  scheduleFlush() {
    if (!this.scheduled) {
      this.scheduled = true;
      this.raf(() => {
        this.scheduled = false;
        this.runTasks(this.reads);
        this.runTasks(this.writes);
      });
    }
  }
  extend(props) {
    return Object.assign(this, props);
  }
}

const instance = (typeof window !== 'undefined' && window.fastdom) || new FastDomShim();
if (typeof window !== 'undefined') window.fastdom = instance;

export default instance;
export const measure = instance.measure.bind(instance);
export const mutate = instance.mutate.bind(instance);
export const clear = instance.clear.bind(instance);
export const extend = instance.extend.bind(instance);
