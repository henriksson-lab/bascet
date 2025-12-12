use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Ident, RangeLimits};

#[proc_macro]
pub fn variadic(input: TokenStream) -> TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    let (ranges, impl_item) = parse_input(input);
    let mut output = proc_macro2::TokenStream::new();
    for combo in cartesian_product(&ranges) {
        output.extend(expand_item(&impl_item, &combo));
    }
    output.into()
}

fn parse_input(
    input: proc_macro2::TokenStream,
) -> (
    Vec<(Ident, std::ops::Range<usize>)>,
    proc_macro2::TokenStream,
) {
    let mut iter = input.into_iter().peekable();
    let mut ranges = Vec::new();

    while let Some(token) = iter.peek() {
        if let proc_macro2::TokenTree::Punct(p) = token {
            if p.as_char() == '#' {
                iter.next();
                if let Some(proc_macro2::TokenTree::Group(group)) = iter.next() {
                    if group.delimiter() == proc_macro2::Delimiter::Bracket {
                        ranges.extend(parse_expand_attr(group.stream()));
                        continue;
                    }
                }
            }
        }
        break;
    }

    (ranges, iter.collect())
}

fn parse_expand_attr(tokens: proc_macro2::TokenStream) -> Vec<(Ident, std::ops::Range<usize>)> {
    let parsed = syn::parse2::<syn::Meta>(tokens).unwrap();
    let syn::Meta::List(list) = parsed else {
        panic!("expected #[expand(...)]")
    };
    if !list.path.is_ident("expand") {
        panic!("expected #[expand(...)]");
    }

    list.parse_args_with(|input: syn::parse::ParseStream| {
        let mut ranges = Vec::new();
        while !input.is_empty() {
            let name: Ident = input.parse()?;
            input.parse::<syn::Token![=]>()?;
            let range: syn::ExprRange = input.parse()?;

            let start = if let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(i),
                ..
            }) = &*range.start.unwrap()
            {
                i.base10_parse()?
            } else {
                panic!("expected integer");
            };

            let end = if let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(i),
                ..
            }) = &*range.end.unwrap()
            {
                let val: usize = i.base10_parse()?;
                match range.limits {
                    RangeLimits::HalfOpen(_) => val,
                    RangeLimits::Closed(_) => val + 1,
                }
            } else {
                panic!("expected integer");
            };

            ranges.push((name, start..end));
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(ranges)
    })
    .unwrap()
}

fn cartesian_product(ranges: &[(Ident, std::ops::Range<usize>)]) -> Vec<Vec<(Ident, usize)>> {
    let mut result = vec![vec![]];
    for (name, range) in ranges {
        let mut new_result = Vec::new();
        for combo in &result {
            for size in range.clone() {
                let mut new_combo = combo.clone();
                new_combo.push((name.clone(), size));
                new_result.push(new_combo);
            }
        }
        result = new_result;
    }
    result
}

fn expand_item(
    tokens: &proc_macro2::TokenStream,
    combo: &[(Ident, usize)],
) -> proc_macro2::TokenStream {
    let mut result = proc_macro2::TokenStream::new();
    let mut iter = tokens.clone().into_iter().peekable();

    while let Some(token) = iter.next() {
        if let proc_macro2::TokenTree::Punct(p) = &token {
            if p.as_char() == '@' {
                let proc_macro2::TokenTree::Ident(name) = iter.next().unwrap() else {
                    panic!()
                };
                let proc_macro2::TokenTree::Group(group) = iter.next().unwrap() else {
                    panic!()
                };

                let mut sep = None;
                if let Some(proc_macro2::TokenTree::Group(g)) = iter.peek() {
                    if g.delimiter() == proc_macro2::Delimiter::Parenthesis {
                        let paren_group = iter.next().unwrap();
                        let proc_macro2::TokenTree::Group(paren_group) = paren_group else {
                            panic!()
                        };
                        let mut paren_iter = paren_group.stream().into_iter();
                        let proc_macro2::TokenTree::Ident(ident) = paren_iter.next().unwrap()
                        else {
                            panic!()
                        };
                        if ident == "sep" {
                            paren_iter.next();
                            let proc_macro2::TokenTree::Literal(lit) = paren_iter.next().unwrap()
                            else {
                                panic!()
                            };
                            sep = Some(lit.to_string().trim_matches('"').to_string());
                        }
                    }
                }

                let size = combo.iter().find(|(n, _)| n == &name).unwrap().1;
                for i in 0..size {
                    if i > 0 && sep.is_some() {
                        result.extend(
                            sep.as_ref()
                                .unwrap()
                                .parse::<proc_macro2::TokenStream>()
                                .unwrap(),
                        );
                    }
                    result.extend(expand_template(&group.stream(), i));
                }
                continue;
            }
        }

        if let proc_macro2::TokenTree::Group(group) = token {
            let delim = group.delimiter();
            let stream = group.stream();

            if delim == proc_macro2::Delimiter::Parenthesis {
                let tokens: Vec<_> = stream.clone().into_iter().collect();
                if tokens.len() >= 2 {
                    if let (proc_macro2::TokenTree::Punct(p), proc_macro2::TokenTree::Ident(name)) =
                        (&tokens[0], &tokens[1])
                    {
                        if p.as_char() == '@' {
                            if let Some(&(_, size)) = combo.iter().find(|(n, _)| n == name) {
                                if size == 1 {
                                    result.extend(expand_item(&stream, combo));
                                    continue;
                                }
                            }
                        }
                    }
                }
            }

            result.extend(std::iter::once(proc_macro2::TokenTree::Group(
                proc_macro2::Group::new(delim, expand_item(&stream, combo)),
            )));
        } else {
            result.extend(std::iter::once(token));
        }
    }

    result
}

fn expand_template(tokens: &proc_macro2::TokenStream, index: usize) -> proc_macro2::TokenStream {
    let mut result = proc_macro2::TokenStream::new();
    let mut iter = tokens.clone().into_iter().peekable();

    while let Some(token) = iter.next() {
        match token {
            proc_macro2::TokenTree::Ident(ident) => {
                let mut found_marker = false;
                if let Some(proc_macro2::TokenTree::Punct(p1)) = iter.peek() {
                    if p1.as_char() == '~' {
                        let mut temp_iter = iter.clone();
                        temp_iter.next();
                        if let Some(proc_macro2::TokenTree::Punct(p2)) = temp_iter.peek() {
                            if p2.as_char() == '#' {
                                iter.next();
                                iter.next();
                                let indexed_ident = quote::format_ident!("{}{}", ident, index);
                                result.extend(quote!(#indexed_ident));
                                found_marker = true;
                            }
                        }
                    } else if p1.as_char() == '#' {
                        iter.next();
                        result.extend(quote!(#ident #index));
                        found_marker = true;
                    }
                }
                if !found_marker {
                    result.extend(quote!(#ident));
                }
            }
            proc_macro2::TokenTree::Punct(p) if p.as_char() == '#' => {
                result.extend(std::iter::once(proc_macro2::TokenTree::Literal(
                    proc_macro2::Literal::usize_unsuffixed(index),
                )));
            }
            proc_macro2::TokenTree::Group(group) => {
                result.extend(std::iter::once(proc_macro2::TokenTree::Group(
                    proc_macro2::Group::new(
                        group.delimiter(),
                        expand_template(&group.stream(), index),
                    ),
                )));
            }
            other => result.extend(std::iter::once(other)),
        }
    }

    result
}
