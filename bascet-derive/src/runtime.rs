use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashSet;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, Data, DeriveInput, Expr, Fields, Ident, Token, Type};

struct BudgetAttr {
    marker: Ident,
    closure: Option<Expr>,
}

impl Parse for BudgetAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let marker = input.parse()?;
        let closure = if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            Some(input.parse()?)
        } else {
            None
        };
        Ok(BudgetAttr { marker, closure })
    }
}

enum BudgetType {
    Total(Option<Expr>),
    Regular(Option<Expr>),
}

enum BudgetKind {
    Thread(BudgetType),
    Mem(BudgetType),
}

struct BudgetDef {
    kind: BudgetKind,
    marker_ident: Ident,
    field_ident: Ident,
    field_type: Type,
}

pub fn derive_budget(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = &input.ident;

    let Data::Struct(data) = &input.data else {
        return syn::Error::new_spanned(&input, "Budget requires struct")
            .to_compile_error()
            .into();
    };
    let Fields::Named(named_fields) = &data.fields else {
        return syn::Error::new_spanned(&input, "Budget requires named fields")
            .to_compile_error()
            .into();
    };

    let budget_defs: Vec<BudgetDef> = named_fields
        .named
        .iter()
        .flat_map(|field| {
            let field_ident = field.ident.as_ref().expect("Field has no ident");
            let field_type = field.ty.clone();

            field.attrs.iter().filter_map(move |attr| {
                if attr.path().is_ident("threads") {
                    let budget_attr = attr.parse_args::<BudgetAttr>().ok()?;
                    let budget_type = if budget_attr.marker == "Total" {
                        BudgetType::Total(budget_attr.closure)
                    } else {
                        BudgetType::Regular(budget_attr.closure)
                    };
                    Some(BudgetDef {
                        kind: BudgetKind::Thread(budget_type),
                        marker_ident: budget_attr.marker,
                        field_ident: field_ident.clone(),
                        field_type: field_type.clone(),
                    })
                } else if attr.path().is_ident("mem") {
                    let budget_attr = attr.parse_args::<BudgetAttr>().ok()?;
                    let budget_type = if budget_attr.marker == "Total" {
                        BudgetType::Total(budget_attr.closure)
                    } else {
                        BudgetType::Regular(budget_attr.closure)
                    };
                    Some(BudgetDef {
                        kind: BudgetKind::Mem(budget_type),
                        marker_ident: budget_attr.marker,
                        field_ident: field_ident.clone(),
                        field_type: field_type.clone(),
                    })
                } else {
                    None
                }
            })
        })
        .collect();

    let markers: HashSet<_> = budget_defs
        .iter()
        .filter(|def| {
            !matches!(
                &def.kind,
                BudgetKind::Thread(BudgetType::Total(_)) | BudgetKind::Mem(BudgetType::Total(_))
            )
        })
        .map(|def| &def.marker_ident)
        .collect();

    let marker_defs = markers.iter().map(|marker| quote! { struct #marker; });

    let thread_impls = budget_defs.iter().filter_map(|def| {
        match &def.kind {
            BudgetKind::Thread(BudgetType::Regular(_)) => {
                let marker = &def.marker_ident;
                let field_ident = &def.field_ident;
                let field_type = &def.field_type;

                Some(quote! {
                    impl bascet_runtime::budget::Threads<#marker> for #name {
                        type Value = #field_type;

                        fn threads(&self) -> &Self::Value {
                            &self.#field_ident
                        }

                        fn spawn<F, R>(&self, offset: u64, f: F) -> std::thread::JoinHandle<R>
                        where
                            F: FnOnce() -> R + Send + 'static,
                            R: Send + 'static,
                        {
                            let name = std::any::type_name::<#marker>().split("::").last().unwrap_or("t");
                            std::thread::Builder::new()
                                .name(format!("{}@{}", name, offset))
                                .spawn(move || f())
                                .unwrap()
                        }
                    }
                })
            }
            BudgetKind::Thread(BudgetType::Total(_)) => {
                let field_ident = &def.field_ident;
                let field_type = &def.field_type;

                Some(quote! {
                    struct Total;

                    impl bascet_runtime::budget::Threads<Total> for #name {
                        type Value = #field_type;

                        fn threads(&self) -> &Self::Value {
                            &self.#field_ident
                        }

                        fn spawn<F, R>(&self, offset: u64, f: F) -> std::thread::JoinHandle<R>
                        where
                            F: FnOnce() -> R + Send + 'static,
                            R: Send + 'static,
                        {
                            let name = "total";
                            std::thread::Builder::new()
                                .name(format!("{}@{}", name, offset))
                                .spawn(move || f())
                                .unwrap()
                        }
                    }
                })
            }
            _ => None,
        }
    });

    let mem_impls = budget_defs.iter().filter_map(|def| match &def.kind {
        BudgetKind::Mem(BudgetType::Regular(_)) => {
            let marker = &def.marker_ident;
            let field_ident = &def.field_ident;
            let field_type = &def.field_type;

            Some(quote! {
                impl bascet_runtime::budget::Memory<#marker> for #name {
                    type Value = #field_type;

                    fn mem(&self) -> &Self::Value {
                        &self.#field_ident
                    }
                }
            })
        }
        BudgetKind::Mem(BudgetType::Total(_)) => {
            let field_ident = &def.field_ident;
            let field_type = &def.field_type;

            Some(quote! {
                impl bascet_runtime::budget::Memory<Total> for #name {
                    type Value = #field_type;

                    fn mem(&self) -> &Self::Value {
                        &self.#field_ident
                    }
                }
            })
        }
        _ => None,
    });

    let total_threads_field = budget_defs
        .iter()
        .find(|def| matches!(&def.kind, BudgetKind::Thread(BudgetType::Total(_))))
        .map(|def| &def.field_ident);

    let total_mem_field = budget_defs
        .iter()
        .find(|def| matches!(&def.kind, BudgetKind::Mem(BudgetType::Total(_))))
        .map(|def| &def.field_ident);

    let thread_budget_fields: Vec<_> = budget_defs
        .iter()
        .filter(|def| matches!(&def.kind, BudgetKind::Thread(BudgetType::Regular(_))))
        .map(|def| &def.field_ident)
        .collect();

    let mem_budget_fields: Vec<_> = budget_defs
        .iter()
        .filter(|def| matches!(&def.kind, BudgetKind::Mem(BudgetType::Regular(_))))
        .map(|def| &def.field_ident)
        .collect();

    let has_threads = !thread_budget_fields.is_empty();
    let has_mem = !mem_budget_fields.is_empty();

    let validate_method = {
        let thread_validation = if let Some(total_field) = &total_threads_field {
            quote! {
                let total_threads = self.#total_field;
                let sum_threads: u64 = 0 #(+ self.#thread_budget_fields.get())*;
                if sum_threads > total_threads {
                    log_warning!("Thread budget exceeded"; "requested" => %sum_threads, "provided" => %total_threads);
                }
            }
        } else {
            quote! {}
        };

        let mem_validation = if let Some(total_field) = &total_mem_field {
            quote! {
                let total_mem = self.#total_field;
                let sum_mem = bytesize::ByteSize(0) #(+ self.#mem_budget_fields)*;
                if sum_mem > total_mem {
                    log_warning!("Memory budget exceeded"; "requested" => %sum_mem, "provided" => %total_mem);
                }
            }
        } else {
            quote! {}
        };

        quote! {
            pub fn validate(&self) {
                #thread_validation
                #mem_validation
            }
        }
    };

    let helper_methods = {
        let threads_method = if has_threads {
            quote! {
                pub fn threads<M>(&self) -> &<Self as bascet_runtime::budget::Threads<M>>::Value
                where
                    Self: bascet_runtime::budget::Threads<M>,
                {
                    bascet_runtime::budget::Threads::<M>::threads(self)
                }

                pub fn spawn<M, F, R>(&self, offset: u64, f: F) -> std::thread::JoinHandle<R>
                where
                    Self: bascet_runtime::budget::Threads<M>,
                    F: FnOnce() -> R + Send + 'static,
                    R: Send + 'static,
                {
                    bascet_runtime::budget::Threads::<M>::spawn(self, offset, f)
                }
            }
        } else {
            quote! {}
        };

        let mem_method = if has_mem {
            quote! {
                pub fn mem<M>(&self) -> &<Self as bascet_runtime::budget::Memory<M>>::Value
                where
                    Self: bascet_runtime::budget::Memory<M>,
                {
                    bascet_runtime::budget::Memory::<M>::mem(self)
                }
            }
        } else {
            quote! {}
        };

        quote! {
            #threads_method
            #mem_method
            #validate_method
        }
    };

    let new_params = budget_defs.iter().map(|def| {
        let field_ident = &def.field_ident;
        let field_type = &def.field_type;
        match &def.kind {
            BudgetKind::Thread(BudgetType::Total(None))
            | BudgetKind::Mem(BudgetType::Total(None)) => {
                // Total without closure = required parameter
                quote! { #field_ident: #field_type }
            }
            BudgetKind::Thread(BudgetType::Total(Some(_)))
            | BudgetKind::Mem(BudgetType::Total(Some(_))) => {
                // Total with closure = no parameter needed
                quote! {}
            }
            BudgetKind::Thread(BudgetType::Regular(None))
            | BudgetKind::Mem(BudgetType::Regular(None)) => {
                // Regular without closure = required parameter
                quote! { #field_ident: #field_type }
            }
            BudgetKind::Thread(BudgetType::Regular(Some(_)))
            | BudgetKind::Mem(BudgetType::Regular(Some(_))) => {
                // Regular with closure = optional parameter (can override closure)
                quote! { #field_ident: Option<#field_type> }
            }
        }
    });

    let field_inits = budget_defs.iter().map(|def| {
        let field_ident = &def.field_ident;
        match &def.kind {
            BudgetKind::Thread(BudgetType::Total(None)) | BudgetKind::Mem(BudgetType::Total(None)) => {
                // Total without closure = use the parameter value directly
                quote! { #field_ident }
            }
            BudgetKind::Thread(BudgetType::Total(Some(closure))) => {
                // Total with closure = evaluate closure (no args)
                quote! {
                    #field_ident: {
                        let f = #closure;
                        f()
                    }
                }
            }
            BudgetKind::Mem(BudgetType::Total(Some(closure))) => {
                // Total with closure = evaluate closure (no args)
                quote! {
                    #field_ident: {
                        let f = #closure;
                        f()
                    }
                }
            }
            BudgetKind::Thread(BudgetType::Regular(Some(closure))) => {
                // Regular thread budget with closure = call with (total_threads, total_mem)
                let total_threads_field = total_threads_field.as_ref().expect("Thread budget with closure requires total_threads field");
                let total_mem_field = total_mem_field.as_ref().expect("Thread budget with closure requires total_mem field");
                quote! {
                    #field_ident: #field_ident.unwrap_or_else(|| {
                        let f = #closure;
                        let total_threads = #total_threads_field.get();
                        let total_mem = #total_mem_field.as_u64();
                        f(total_threads, total_mem)
                    })
                }
            }
            BudgetKind::Mem(BudgetType::Regular(Some(closure))) => {
                // Regular mem budget with closure = call with (total_threads, total_mem)
                let total_threads_field = total_threads_field.as_ref().expect("Mem budget with closure requires total_threads field");
                let total_mem_field = total_mem_field.as_ref().expect("Mem budget with closure requires total_mem field");
                quote! {
                    #field_ident: #field_ident.unwrap_or_else(|| {
                        let f = #closure;
                        let total_threads = #total_threads_field.get();
                        let total_mem = #total_mem_field.as_u64();
                        f(total_threads, total_mem)
                    })
                }
            }
            BudgetKind::Thread(BudgetType::Regular(None)) | BudgetKind::Mem(BudgetType::Regular(None)) => {
                // Regular without closure = required parameter
                quote! {
                    #field_ident: #field_ident.expect(&format!("{} is required", stringify!(#field_ident)))
                }
            }
        }
    });

    let display_fields = budget_defs.iter().enumerate().map(|(idx, def)| {
        let field_ident = &def.field_ident;
        let field_name = field_ident.to_string();
        let separator = if idx == 0 { "" } else { ", " };

        match &def.kind {
            BudgetKind::Thread(BudgetType::Total(_))
            | BudgetKind::Thread(BudgetType::Regular(_)) => {
                quote! {
                    write!(f, "{}{}: {}", #separator, #field_name, self.#field_ident.get())?;
                }
            }
            BudgetKind::Mem(_) => {
                quote! {
                    write!(f, "{}{}: {}", #separator, #field_name, self.#field_ident)?;
                }
            }
        }
    });

    TokenStream::from(quote! {
        use bon as __bon;

        #(#marker_defs)*
        #(#thread_impls)*
        #(#mem_impls)*

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                #(#display_fields)*
                Ok(())
            }
        }

        #[__bon::bon]
        impl #name {
            #[builder]
            pub fn new(#(#new_params),*) -> Self {
                Self {
                    #(#field_inits),*
                }
            }

            #helper_methods
        }
    })
}
