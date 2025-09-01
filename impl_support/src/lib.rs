use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, Attribute, Data, DeriveInput, Ident,
};
use std::collections::HashSet;

#[proc_macro_attribute]
pub fn generate(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    
    let enum_name = &input.ident;
    let enum_visibility = &input.vis;
    let enum_data = match &input.data {
        Data::Enum(data) => data,
        _ => panic!("generate macro can only be applied to enums"),
    };

    let mut generated = proc_macro2::TokenStream::new();
    let mut new_variants = Vec::new();
    let mut all_generics = HashSet::new();
    let mut variant_generics = Vec::new(); // Track which generics each variant uses
    
    for variant in &enum_data.variants {
        if let Some((formats, io_type)) = extract_formats(&variant.attrs) {
            if let Some(backend_info) = extract_backend_info(&variant.attrs) {
                // Collect generics from this backend
                for generic in &backend_info.generics {
                    all_generics.insert(generic.clone());
                }
                variant_generics.push((variant.ident.clone(), backend_info.generics.clone()));
                
                // Generate the backend enum
                let backend_enum = generate_backend_enum(enum_name, &variant.ident, &backend_info, &formats);
                
                // Generate the input/output enum
                let io_enum = generate_io_enum(enum_name, &variant.ident, &formats, &io_type);
                
                // Generate TryFrom implementations
                let try_from_path = generate_try_from_path_impl(enum_name, &variant.ident, &formats, &io_type);
                let try_from_io = generate_try_from_io_impl(enum_name, &variant.ident, &formats, &io_type);
                
                generated.extend(backend_enum);
                generated.extend(io_enum);
                generated.extend(try_from_path);
                generated.extend(try_from_io);
                
                // Simple unit variant for factory enum
                let variant_ident = &variant.ident;
                new_variants.push(quote! { #variant_ident });
            }
        } else {
            // Keep original variant if no formats
            new_variants.push(quote! { #variant });
        }
    }
    
    // Generate the factory enum with collected generics
    let mut sorted_generics: Vec<_> = all_generics.into_iter().collect();
    sorted_generics.sort(); // For consistent ordering
    
    // Add PhantomData variants for all generics (since factory enum has unit variants)
    for generic in &sorted_generics {
        let phantom_variant = format_ident!("_Phantom{}", generic);
        let generic_ident = format_ident!("{}", generic);
        new_variants.push(quote! { 
            #phantom_variant(std::marker::PhantomData<#generic_ident>) 
        });
    }
    
    let generics_list = if sorted_generics.is_empty() {
        quote!()
    } else {
        let generic_tokens: Vec<_> = sorted_generics.iter().map(|g| format_ident!("{}", g)).collect();
        quote!(<#(#generic_tokens),*>)
    };
    
    let transformed_enum = quote! {
        #enum_visibility enum #enum_name #generics_list {
            #(#new_variants,)*
        }
    };
    
    let mut result = transformed_enum;
    result.extend(generated);
    TokenStream::from(result)
}

fn extract_formats(attrs: &[Attribute]) -> Option<(Vec<String>, String)> {
    for attr in attrs {
        if attr.path().is_ident("input") {
            if let Ok(tokens) = attr.parse_args::<proc_macro2::TokenStream>() {
                let formats = parse_format_list(&tokens.to_string());
                return Some((formats, "Input".to_string()));
            }
        } else if attr.path().is_ident("output") {
            if let Ok(tokens) = attr.parse_args::<proc_macro2::TokenStream>() {
                let formats = parse_format_list(&tokens.to_string());
                return Some((formats, "Output".to_string()));
            }
        }
    }
    None
}

fn parse_format_list(input: &str) -> Vec<String> {
    input
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn extract_backend_info(attrs: &[Attribute]) -> Option<BackendInfo> {
    for attr in attrs {
        if attr.path().is_ident("stream") || attr.path().is_ident("writer") {
            if let Ok(tokens) = attr.parse_args::<proc_macro2::TokenStream>() {
                let type_str = tokens.to_string();
                return parse_backend_type(&type_str);
            }
        }
    }
    None
}

#[derive(Debug)]
struct BackendInfo {
    trait_name: String,
    generics: Vec<String>,
    bounds_tokens: proc_macro2::TokenStream,
}

fn parse_backend_type(type_str: &str) -> Option<BackendInfo> {
    let type_str = type_str.trim();
    
    if let Some(lt_pos) = type_str.find('<') {
        let trait_name = type_str[..lt_pos].trim().to_string();
        let generics_part = &type_str[lt_pos + 1..];
        
        // Find the matching closing bracket
        let mut bracket_count = 1;
        let mut end_pos = 0;
        for (i, ch) in generics_part.char_indices() {
            match ch {
                '<' => bracket_count += 1,
                '>' => {
                    bracket_count -= 1;
                    if bracket_count == 0 {
                        end_pos = i;
                        break;
                    }
                }
                _ => {}
            }
        }
        
        if end_pos > 0 {
            let bounds_str = &generics_part[..end_pos];
            let bounds_tokens: proc_macro2::TokenStream = bounds_str.parse().ok()?;
            
            // Extract generics - split by comma and look for pattern "Name:"
            let mut generics = Vec::new();
            
            for part in bounds_str.split(',') {
                let part = part.trim();
                if let Some(colon_pos) = part.find(':') {
                    let generic_name = part[..colon_pos].trim();
                    // Check if it looks like a generic parameter (single uppercase letter)
                    if generic_name.len() == 1 && generic_name.chars().next().unwrap().is_uppercase() {
                        if !generics.contains(&generic_name.to_string()) {
                            generics.push(generic_name.to_string());
                        }
                    }
                }
            }
            
            return Some(BackendInfo {
                trait_name,
                generics,
                bounds_tokens,
            });
        }
    } else {
        // No generics
        return Some(BackendInfo {
            trait_name: type_str.to_string(),
            generics: Vec::new(),
            bounds_tokens: proc_macro2::TokenStream::new(),
        });
    }
    
    None
}

fn to_camel_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;
    
    for ch in s.chars() {
        if ch == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(ch.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }
    
    result
}

fn generate_backend_enum(
    enum_name: &Ident,
    variant_name: &Ident,
    backend_info: &BackendInfo,
    formats: &[String],
) -> proc_macro2::TokenStream {
    let backend_enum_name = format_ident!("{}{}", enum_name, variant_name);
    let trait_name = format_ident!("{}", backend_info.trait_name);
    
    let generics_list = if backend_info.generics.is_empty() {
        quote!()
    } else {
        let generics_tokens: Vec<_> = backend_info.generics.iter().map(|g| format_ident!("{}", g)).collect();
        quote!(<#(#generics_tokens),*>)
    };
    
    let enum_dispatch_trait = if backend_info.generics.is_empty() {
        quote!(#trait_name)
    } else {
        let trait_generics_tokens: Vec<_> = backend_info.generics.iter().map(|g| format_ident!("{}", g)).collect();
        quote!(#trait_name<#(#trait_generics_tokens),*>)
    };
    
    let variants = formats.iter().map(|format| {
        let format_variant_name = format_ident!("{}", to_camel_case(format));
        let format_ident = format_ident!("{}", format);
        
        let type_path = match variant_name.to_string().as_str() {
            "Writer" => {
                if backend_info.generics.contains(&"W".to_string()) {
                    quote!(crate::io::format::#format_ident::#variant_name<W>)
                } else {
                    panic!("Writer backend requires W generic but none found in trait");
                }
            }
            "Stream" => {
                quote!(crate::io::format::#format_ident::#variant_name)
            }
            unknown => {
                panic!("Unknown backend type: {}. Only 'Stream' and 'Writer' are supported.", unknown);
            }
        };
        
        quote!(#format_variant_name(#type_path))
    });
    
    let variants: Vec<_> = variants.collect();
    
    let where_clause = if backend_info.bounds_tokens.is_empty() {
        quote!()
    } else {
        let bounds = &backend_info.bounds_tokens;
        quote!(where #bounds)
    };
    
    quote! {
        #[enum_dispatch::enum_dispatch(#enum_dispatch_trait)]
        pub enum #backend_enum_name #generics_list
        #where_clause
        {
            #(#variants,)*
        }
    }
}

fn generate_io_enum(
    enum_name: &Ident,
    variant_name: &Ident,
    formats: &[String],
    io_type: &str,
) -> proc_macro2::TokenStream {
    let io_enum_name = format_ident!("{}{}{}", enum_name, variant_name, io_type);
    let io_type_ident = format_ident!("{}", io_type);
    
    let variants = formats.iter().map(|format| {
        let format_variant_name = format_ident!("{}", to_camel_case(format));
        let format_ident = format_ident!("{}", format);
        quote!(#format_variant_name(crate::io::format::#format_ident::#io_type_ident))
    });
    
    quote! {
        pub enum #io_enum_name {
            #(#variants,)*
        }
    }
}

fn generate_try_from_path_impl(
    enum_name: &Ident,
    variant_name: &Ident,
    formats: &[String],
    io_type: &str,
) -> proc_macro2::TokenStream {
    let io_enum_name = format_ident!("{}{}{}", enum_name, variant_name, io_type);
    let io_type_ident = format_ident!("{}", io_type);
    
    let try_formats = formats.iter().map(|format| {
        let format_variant_name = format_ident!("{}", to_camel_case(format));
        let format_ident = format_ident!("{}", format);
        
        quote! {
            if let Ok(inner) = crate::io::format::#format_ident::#io_type_ident::new(path) {
                crate::log_info!("Detected {} format: {}", stringify!(#io_type_ident), stringify!(#format_ident));
                return Ok(Self::#format_variant_name(inner));
            }
        }
    });
    
    quote! {
        impl std::convert::TryFrom<&std::path::Path> for #io_enum_name {
            type Error = crate::runtime::Error;
            
            fn try_from(path: &std::path::Path) -> Result<Self, Self::Error> {
                #(#try_formats)*
                Err(crate::runtime::Error::file_not_valid(
                    path,
                    Some("No supported format could handle this file")
                ))
            }
        }
    }
}

fn generate_try_from_io_impl(
    enum_name: &Ident,
    variant_name: &Ident,
    formats: &[String],
    io_type: &str,
) -> proc_macro2::TokenStream {
    let io_enum_name = format_ident!("{}{}{}", enum_name, variant_name, io_type);
    let backend_enum_name = format_ident!("{}{}", enum_name, variant_name);
    
    let conversions = formats.iter().map(|format| {
        let format_variant_name = format_ident!("{}", to_camel_case(format));
        let format_ident = format_ident!("{}", format);
        
        quote! {
            if let #io_enum_name::#format_variant_name(file) = input {
                return Ok(Self::#format_variant_name(
                    crate::io::format::#format_ident::#variant_name::new(&file)?
                ));
            }
        }
    });
    
    quote! {
        impl std::convert::TryFrom<#io_enum_name> for #backend_enum_name {
            type Error = crate::runtime::Error;
            
            fn try_from(input: #io_enum_name) -> Result<Self, Self::Error> {
                #(#conversions)*
                unreachable!()
            }
        }
    }
}