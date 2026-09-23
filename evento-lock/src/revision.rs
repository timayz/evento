//! Which `.revision(n)` belongs to which view.
//!
//! A projection is usually built by a function returning it,
//! `fn create_projection<E: Executor>() -> Projection<E, MyView>`, or with a
//! turbofish, `Projection::<E, MyView>::new::<A>()`. Either names the view a
//! `.revision(<literal>)` in that expression applies to.

use syn::visit::{self, Visit};

/// A `.revision(<integer literal>)` call.
pub(crate) struct Call {
    /// The view it applies to, when the code names it.
    pub view: Option<String>,
    pub value: u16,
}

/// Every `.revision(<integer literal>)` in `items`, not counting nested
/// `mod`s (scanned as their own module).
pub(crate) fn calls(items: &[syn::Item]) -> syn::Result<Vec<Call>> {
    let mut finder = Finder::default();
    for item in items {
        finder.visit_item(item);
    }
    match finder.error {
        Some(error) => Err(error),
        None => Ok(finder.calls),
    }
}

#[derive(Default)]
struct Finder {
    /// The view named by the return type of each enclosing function.
    returns: Vec<Option<String>>,
    calls: Vec<Call>,
    error: Option<syn::Error>,
}

impl Finder {
    fn in_fn(&mut self, output: &syn::ReturnType, visit: impl FnOnce(&mut Self)) {
        let view = match output {
            syn::ReturnType::Default => None,
            syn::ReturnType::Type(_, ty) => view_in_type(ty),
        };
        self.returns.push(view);
        visit(self);
        self.returns.pop();
    }
}

impl<'ast> Visit<'ast> for Finder {
    fn visit_item_mod(&mut self, _: &'ast syn::ItemMod) {}

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.in_fn(&item.sig.output, |this| visit::visit_item_fn(this, item));
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        self.in_fn(&item.sig.output, |this| {
            visit::visit_impl_item_fn(this, item)
        });
    }

    fn visit_trait_item_fn(&mut self, item: &'ast syn::TraitItemFn) {
        self.in_fn(&item.sig.output, |this| {
            visit::visit_trait_item_fn(this, item)
        });
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        if call.method == "revision" && call.args.len() == 1 {
            if let Some(syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(value),
                ..
            })) = call.args.first()
            {
                match value.base10_parse::<u16>() {
                    Ok(value) => self.calls.push(Call {
                        view: view_in_receiver(&call.receiver)
                            .or_else(|| self.returns.last().cloned().flatten()),
                        value,
                    }),
                    Err(error) => {
                        self.error.get_or_insert(error);
                    }
                }
            }
        }
        visit::visit_expr_method_call(self, call);
    }
}

/// `Projection<E, View>` anywhere in a type (also `Result<Projection<E, View>>`).
fn view_in_type(ty: &syn::Type) -> Option<String> {
    #[derive(Default)]
    struct Types(Option<String>);
    impl<'ast> Visit<'ast> for Types {
        fn visit_path_segment(&mut self, segment: &'ast syn::PathSegment) {
            if self.0.is_none() {
                self.0 = view_of_segment(segment);
            }
            visit::visit_path_segment(self, segment);
        }
    }
    let mut types = Types::default();
    types.visit_type(ty);
    types.0
}

/// The turbofish of `Projection::<E, View>::new::<A>()` at the root of a
/// method chain.
fn view_in_receiver(mut expr: &syn::Expr) -> Option<String> {
    loop {
        expr = match expr {
            syn::Expr::MethodCall(call) => &call.receiver,
            syn::Expr::Call(call) => &call.func,
            syn::Expr::Paren(paren) => &paren.expr,
            syn::Expr::Path(path) => return path.path.segments.iter().find_map(view_of_segment),
            _ => return None,
        };
    }
}

/// `View` for a `Projection<.., View>` segment: its last type argument.
fn view_of_segment(segment: &syn::PathSegment) -> Option<String> {
    if segment.ident != "Projection" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().rev().find_map(|arg| match arg {
        syn::GenericArgument::Type(syn::Type::Path(path)) => {
            path.path.segments.last().map(|s| s.ident.to_string())
        }
        _ => None,
    })
}
