use proc_macro::TokenStream;
use quote::quote;
use syn::{parse::{Parse, ParseStream}, parse_macro_input, punctuated::Punctuated, GenericParam, ItemEnum, ItemImpl, ItemStruct, ItemTrait, PredicateType, Token, Type, TypeParam, TypeParamBound, WhereClause, WherePredicate};

/// Adds a generic parameter to a trait definition
///
/// # Example
/// ```rust
/// #[constrainable(C)]
/// pub trait BascetCellWrite<W>: Sized where W: std::io::Write {
///     fn write_cell(&mut self, cell: &C) -> Result<(), Error>;
/// }
/// ```
///
/// Expands to:
/// ```rust
/// pub trait BascetCellWrite<W, C>: Sized where W: std::io::Write {
///     fn write_cell(&mut self, cell: &C) -> Result<(), Error>;
/// }
/// ```
#[proc_macro_attribute]
pub fn constrainable(args: TokenStream, input: TokenStream) -> TokenStream {
    let generic_name = parse_macro_input!(args as syn::Ident);
    let mut trait_def = parse_macro_input!(input as ItemTrait);

    // Check if the generic parameter already exists
    let generic_exists = trait_def.generics.params.iter().any(|param| {
        match param {
            GenericParam::Type(type_param) => type_param.ident == generic_name,
            _ => false,
        }
    });

    // Only add the generic parameter if it doesn't already exist
    if !generic_exists {
        let generic_param = GenericParam::Type(TypeParam {
            attrs: Vec::new(),
            ident: generic_name,
            colon_token: None,
            bounds: Default::default(),
            eq_token: None,
            default: None,
        });
        
        trait_def.generics.params.push(generic_param);
    }

    let expanded = quote! {
        #trait_def
    };

    TokenStream::from(expanded)
}

/// Adds bounds to a generic parameter in an impl block's where clause
///
/// # Example
/// ```rust
/// #[constrain(C: CellIdAccessor + CellReadsAccessor)]
/// impl<W> BascetCellWrite<W, C> for Writer<W> where W: std::io::Write {
///     // implementation
/// }
/// ```
///
/// Expands to:
/// ```rust
/// impl<W, C> BascetCellWrite<W, C> for Writer<W> 
/// where 
///     W: std::io::Write,
///     C: CellIdAccessor + CellReadsAccessor,
/// {
///     // implementation  
/// }
/// ```
// Custom parser for constraint syntax: C: Trait1 + Trait2
struct Constraint {
    bounded_ty: Type,
    bounds: Punctuated<TypeParamBound, Token![+]>,
}

impl Parse for Constraint {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let bounded_ty: Type = input.parse()?;
        input.parse::<Token![:]>()?;
        let bounds = Punctuated::parse_separated_nonempty(input)?;
        Ok(Constraint { bounded_ty, bounds })
    }
}

#[proc_macro_attribute]
pub fn constrain(args: TokenStream, input: TokenStream) -> TokenStream {
    let constraint = parse_macro_input!(args as Constraint);
    let input_clone = input.clone();

    // Build a PredicateType from the parsed constraint
    let predicate = PredicateType {
        lifetimes: None,
        bounded_ty: constraint.bounded_ty,
        colon_token: Token![:](proc_macro2::Span::call_site()),
        bounds: constraint.bounds,
    };

    // Try parsing as `impl`
    if let Ok(mut impl_block) = syn::parse::<ItemImpl>(input_clone.clone()) {
        if impl_block.generics.where_clause.is_none() {
            impl_block.generics.where_clause = Some(WhereClause {
                where_token: Token![where](proc_macro2::Span::call_site()),
                predicates: Default::default(),
            });
        }
        impl_block.generics.where_clause
            .as_mut()
            .unwrap()
            .predicates
            .push(WherePredicate::Type(predicate));

        return quote! { #impl_block }.into();
    }

    // Try parsing as `enum`
    if let Ok(mut enum_def) = syn::parse::<ItemEnum>(input_clone.clone()) {
        if enum_def.generics.where_clause.is_none() {
            enum_def.generics.where_clause = Some(WhereClause {
                where_token: Token![where](proc_macro2::Span::call_site()),
                predicates: Default::default(),
            });
        }
        enum_def.generics.where_clause
            .as_mut()
            .unwrap()
            .predicates
            .push(WherePredicate::Type(predicate));

        return quote! { #enum_def }.into();
    }

    // Try parsing as `struct`
    if let Ok(mut struct_def) = syn::parse::<ItemStruct>(input_clone) {
        if struct_def.generics.where_clause.is_none() {
            struct_def.generics.where_clause = Some(WhereClause {
                where_token: Token![where](proc_macro2::Span::call_site()),
                predicates: Default::default(),
            });
        }
        struct_def.generics.where_clause
            .as_mut()
            .unwrap()
            .predicates
            .push(WherePredicate::Type(predicate));

        return quote! { #struct_def }.into();
    }

    // If it's none of the above, just return input unchanged (or emit error)
    input
}
