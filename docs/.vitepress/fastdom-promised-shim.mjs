const exports = {
  initialize() {
    this._tasks = new Map();
  },
  mutate(fn, ctx) {
    const self = this;
    return new Promise((resolve) => {
      self.fastdom.mutate(() => {
        resolve(ctx ? fn.call(ctx) : fn());
      });
    });
  },
  measure(fn, ctx) {
    const self = this;
    return new Promise((resolve) => {
      self.fastdom.measure(() => {
        resolve(ctx ? fn.call(ctx) : fn());
      });
    });
  },
  clear() {}
};
export default exports;
