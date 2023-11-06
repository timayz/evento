up.compiler("[up-infinite]", function (el) {
  const id = el.getAttribute("up-infinite");
  let url = el.getAttribute("up-infinite-url");
  url = url + (url.includes("?") ? "&" : "?");

  function fn() {
    if (!el?.lastElementChild || !el?.dataset.cursor) {
      return;
    }

    new IntersectionObserver(async (entries, observer) => {
      const entry = entries.find((entry) => entry.isIntersecting);
      if (!entry) {
        return;
      }

      const cursor = el?.dataset.cursor;
      const req = await up.request(`${url}after=${cursor}`);
      const template = document.createElement("template");
      template.innerHTML = req.text;

      const tmplEl = template.content.querySelector(`[up-infinite='${id}']`);

      Array.from(tmplEl.children).forEach((child) => {
        el.append(child);
      });

      observer.disconnect();

      const tmplCursor = tmplEl?.dataset.cursor;
      if (!tmplCursor) {
        el?.removeAttribute("data-cursor");
        return;
      }

      el?.setAttribute("data-cursor", tmplCursor);
      fn();
    }).observe(el?.lastElementChild);
  }

  fn();
});
