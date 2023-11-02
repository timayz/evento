// TODO: put into unpoly compiler

const source = new EventSource("/events/index");
source.addEventListener("created", async function (event) {
  const products = document.querySelector("#products");

  if (!products || products.dataset.cursor) {
    return;
  }

  products.append(await getProduct(event.data));
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

function load_more_products() {
  const products = document.querySelector("#products");
  new IntersectionObserver(async (entries, observer) => {
    const entry = entries.find((entry) => entry.isIntersecting);
    if (!entry) {
      return;
    }

    const cursor = products?.dataset.cursor;
    const req = await up.request(`/?after=${cursor}`);
    const template = document.createElement("template");
    template.innerHTML = req.text;

    const tmplProducts = template.content.querySelector("#products");

    Array.from(tmplProducts.children).forEach((child) => {
      products.append(child);
    });

    observer.disconnect();

    const tmplCursor = tmplProducts?.dataset.cursor;
    if (!tmplCursor) {
      products?.removeAttribute("data-cursor");
      return;
    }

    products?.setAttribute("data-cursor", tmplCursor);
    load_more_products();
  }).observe(products?.lastElementChild);
}
