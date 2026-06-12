use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, Ident, LitInt, Result, parenthesized, parse_macro_input};

pub fn derive_scheduling(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    emit(input).unwrap_or_else(|e| e.to_compile_error().into())
}

fn emit(input: DeriveInput) -> Result<TokenStream> {
    let parsed = parse_scheduling_attr(&input)?;
    let mode = parsed.mode_tokens();
    let strategy = parsed.strategy_tokens();
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics crate::owned::Owned<crate::stage::Mode> for #name #ty_generics #where_clause {
            type Value = crate::stage::Mode;
            fn owned(&self) -> crate::stage::Mode {
                #mode
            }
        }

        impl #impl_generics crate::owned::Owned<crate::stage::Strategy> for #name #ty_generics #where_clause {
            type Value = crate::stage::Strategy;
            fn owned(&self) -> crate::stage::Strategy {
                #strategy
            }
        }
    }
    .into())
}

#[derive(Clone, Copy)]
enum ModeKind {
    Auto,
    Manual,
}

#[derive(Clone, Copy)]
enum StrategyKind {
    Burn,
    Job,
    Task,
}

struct ParsedScheduling {
    mode: ModeKind,
    strategy: StrategyKind,
    countof_workers: u32,
}

impl ParsedScheduling {
    fn mode_tokens(&self) -> proc_macro2::TokenStream {
        let countof_workers = self.countof_workers;
        let mode = match self.mode {
            ModeKind::Auto => quote! { Auto },
            ModeKind::Manual => quote! { Manual },
        };
        quote! {
            crate::stage::Mode::#mode {
                countof_workers: std::num::NonZeroU32::new(#countof_workers).unwrap(),
                countof_min: std::num::NonZeroU32::new(#countof_workers).unwrap(),
                countof_max: std::num::NonZeroU32::new(u32::MAX).unwrap(),
            }
        }
    }

    fn strategy_tokens(&self) -> proc_macro2::TokenStream {
        match self.strategy {
            StrategyKind::Burn => quote! { crate::stage::Strategy::Burn },
            StrategyKind::Job => quote! { crate::stage::Strategy::Job },
            StrategyKind::Task => quote! { crate::stage::Strategy::Task },
        }
    }
}

fn parse_scheduling_attr(input: &DeriveInput) -> Result<ParsedScheduling> {
    let attr = input.attrs.iter().find(|a| a.path().is_ident("scheduling"));

    let Some(attr) = attr else {
        return Ok(ParsedScheduling {
            mode: ModeKind::Auto,
            strategy: StrategyKind::Task,
            countof_workers: 1,
        });
    };

    attr.parse_args_with(|p: syn::parse::ParseStream| {
        let mode_ident: Ident = p.parse()?;
        let mode = parse_mode(&mode_ident)?;
        let mut strategy = StrategyKind::Task;
        let mut countof_workers = 1;

        if p.peek(syn::Token![::]) {
            p.parse::<syn::Token![::]>()?;
            let strategy_ident: Ident = p.parse()?;
            strategy = parse_strategy(&strategy_ident)?;
        }

        if p.peek(syn::token::Paren) {
            let content;
            parenthesized!(content in p);
            let count: LitInt = content.parse()?;
            countof_workers = parse_count(&count)?;
        }

        if !p.is_empty() {
            return Err(p.error("expected end of scheduling attribute"));
        }

        Ok(ParsedScheduling {
            mode,
            strategy,
            countof_workers,
        })
    })
}

fn parse_mode(ident: &Ident) -> Result<ModeKind> {
    match ident.to_string().as_str() {
        "Auto" => Ok(ModeKind::Auto),
        "Manual" => Ok(ModeKind::Manual),
        _ => Err(Error::new(ident.span(), "expected Auto or Manual")),
    }
}

fn parse_strategy(ident: &Ident) -> Result<StrategyKind> {
    match ident.to_string().as_str() {
        "Burn" => Ok(StrategyKind::Burn),
        "Job" => Ok(StrategyKind::Job),
        "Task" => Ok(StrategyKind::Task),
        _ => Err(Error::new(ident.span(), "expected Burn, Job, or Task")),
    }
}

fn parse_count(count: &LitInt) -> Result<u32> {
    let n: u32 = count.base10_parse()?;
    if n == 0 {
        return Err(Error::new(
            count.span(),
            "scheduling worker count must be non-zero",
        ));
    }
    Ok(n)
}
