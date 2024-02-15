const source = new EventSource("/events/index");

source.addEventListener("created", async function (event) {
  const el = document.querySelector("[up-infinite]");
  if (el.dataset.cursor) {
    return;
  }

  el.append(await getData(event.data));
});

source.addEventListener("updated", async function (event) {
  const el = document.querySelector("[up-infinite]");
  el.querySelector(`[up-sse-item='${event.data}']`)?.replaceWith(
    await getData(event.data)
  );
});

source.addEventListener("deleted", async function (event) {
  const el = document.querySelector("[up-infinite]");
  el.querySelector(`[up-sse-item='${event.data}']`)?.remove();
});

async function getData(data) {
  const req = await up.request(`/_product?id=${data}`);
  const template = document.createElement("template");
  template.innerHTML = req.text;

  return template.content.firstChild;
}
