up.compiler("[up-sse]", function (el) {
  const sse = el.getAttribute("up-sse");
  const url = el.getAttribute("up-sse-fetch");

  const source = new EventSource(sse);

  source.addEventListener("created", async function (event) {
    if (el.dataset.cursor) {
      return;
    }

    el.append(await getData(event.data));
  });

  source.addEventListener("updated", async function (event) {
    el.querySelector(`[up-sse-item='${event.data}']`)?.replaceWith(
      await getData(event.data)
    );
  });

  source.addEventListener("deleted", async function (event) {
    el.querySelector(`[up-sse-item='${event.data}']`)?.remove();
  });

  async function getData(data) {
    const req = await up.request(url.replace("[data]", data));
    const template = document.createElement("template");
    template.innerHTML = req.text;

    return template.content.firstChild;
  }
});
