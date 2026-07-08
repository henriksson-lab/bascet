use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, Ident, LitInt, Result, parenthesized, parse_macro_input};

#[derive(Clone, Copy)]
enum Mode {
    Auto,
    Manual,
}

#[derive(Clone, Copy)]
enum Strategy {
    Burn,
    Job,
    Task,
}

#[derive(Clone, Copy)]
struct Workers(u32);

pub(crate) struct Schedule {
    parallelism: Mode,
    strategy: Strategy,
    workers: Workers,
}

impl Schedule {
    pub fn derive(input: TokenStream) -> TokenStream {
        let input = parse_macro_input!(input as DeriveInput);
        Self::from_derive(&input)
            .map(|schedule| schedule.emit(&input))
            .unwrap_or_else(|e| e.to_compile_error().into())
    }

    fn from_derive(input: &DeriveInput) -> Result<Self> {
        let attr = input.attrs.iter().find(|a| a.path().is_ident("schedule"));

        let Some(attr) = attr else {
            return Ok(Self {
                parallelism: Mode::Auto,
                strategy: Strategy::Task,
                workers: Workers(1),
            });
        };

        attr.parse_args_with(|p: syn::parse::ParseStream| {
            let mode_ident: Ident = p.parse()?;
            let parallelism = Mode::from_ident(&mode_ident)?;
            let mut strategy = Strategy::Task;
            let mut workers = Workers(1);

            if p.peek(syn::Token![::]) {
                p.parse::<syn::Token![::]>()?;
                let strategy_ident: Ident = p.parse()?;
                strategy = Strategy::from_ident(&strategy_ident)?;
            }

            if p.peek(syn::token::Paren) {
                let content;
                parenthesized!(content in p);
                let count: LitInt = content.parse()?;
                workers = Workers::from_lit(&count)?;
            }

            if !p.is_empty() {
                return Err(p.error("expected end of schedule attribute"));
            }

            Ok(Self {
                parallelism,
                strategy,
                workers,
            })
        })
    }

    fn emit(self, input: &DeriveInput) -> TokenStream {
        let parallelism = self.parallelism_tokens();
        let strategy = self.strategy_tokens();
        let name = &input.ident;
        let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

        quote! {
            impl #impl_generics crate::owned::Owned<crate::schedule::Schedule> for #name #ty_generics #where_clause {
                type Value = crate::schedule::Schedule;
                fn owned(&self) -> crate::schedule::Schedule {
                    crate::schedule::Schedule {
                        strategy: #strategy,
                        parallelism: #parallelism,
                    }
                }
            }
        }
        .into()
    }

    fn parallelism_tokens(&self) -> proc_macro2::TokenStream {
        let workers = self.workers.0;
        let mode = match self.parallelism {
            Mode::Auto => quote! { Auto },
            Mode::Manual => quote! { Manual },
        };
        quote! {
            crate::schedule::Mode::#mode(crate::schedule::Parallelism::new(
                std::num::NonZeroU32::new(#workers).unwrap()
            ))
        }
    }

    fn strategy_tokens(&self) -> proc_macro2::TokenStream {
        let strategy = match self.strategy {
            Strategy::Burn => quote! { crate::schedule::Strategy::Burn },
            Strategy::Job => quote! { crate::schedule::Strategy::Job },
            Strategy::Task => quote! { crate::schedule::Strategy::Task },
        };
        quote! { crate::schedule::Mode::Auto(#strategy) }
    }
}

impl Mode {
    fn from_ident(ident: &Ident) -> Result<Self> {
        match ident.to_string().as_str() {
            "Auto" => Ok(Self::Auto),
            "Manual" => Ok(Self::Manual),
            _ => Err(Error::new(ident.span(), "expected Auto or Manual")),
        }
    }
}

impl Strategy {
    fn from_ident(ident: &Ident) -> Result<Self> {
        match ident.to_string().as_str() {
            "Burn" => Ok(Self::Burn),
            "Job" => Ok(Self::Job),
            "Task" => Ok(Self::Task),
            _ => Err(Error::new(ident.span(), "expected Burn, Job, or Task")),
        }
    }
}

impl Workers {
    fn from_lit(lit: &LitInt) -> Result<Self> {
        let n: u32 = lit.base10_parse()?;
        if n == 0 {
            return Err(Error::new(
                lit.span(),
                "schedule worker count must be non-zero",
            ));
        }
        Ok(Self(n))
    }
}
