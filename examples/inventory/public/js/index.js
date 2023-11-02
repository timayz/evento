const source = new EventSource("/events/index");
source.addEventListener("created", async function (event) {
  document.querySelector("#products")?.prepend(await getProduct(event.data));
});

source.addEventListener("updated", async function (event) {
  document
    .querySelector(`#product-${event.data}`)
    ?.replaceWith(await getProduct(event.data));
});

source.addEventListener("deleted", async function (event) {
  document.querySelector(`#product-${event.data}`)?.remove();
});

async function getProduct(id) {
  const req = await up.request(`/_product?id=${id}`);
  const template = document.createElement("template");
  template.innerHTML = req.text;

  return template.content.firstChild;
}
