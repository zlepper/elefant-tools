use proc_macro::{Span, TokenStream};
use darling::ast::NestedMeta;
use darling::FromMeta;
use quote::quote;
use syn::{ItemFn, parse_macro_input};


#[derive(Debug, FromMeta)]
enum TestArgsArg {
    Postgres(i32),
    TimescaleDb(i32),
}

impl TestArgsArg {
    fn get_mod_part_name(&self) -> String {
        match self {
            TestArgsArg::Postgres(v) => format!("postgres_{}", v),
            TestArgsArg::TimescaleDb(v) => format!("timescale_{}", v),
        }
    }

    fn get_port(&self) -> Result<u16, darling::Error> {
        match self {
            TestArgsArg::Postgres(12) => Ok(5412),
            TestArgsArg::Postgres(13) => Ok(5413),
            TestArgsArg::Postgres(14) => Ok(5414),
            TestArgsArg::Postgres(15) => Ok(5415),
            TestArgsArg::Postgres(16) => Ok(5416),
            TestArgsArg::TimescaleDb(15) => Ok(5515),
            TestArgsArg::TimescaleDb(16) => Ok(5516),
            _ => Err(darling::Error::custom("Unknown postgres implementation / version"))
        }
    }
}

#[derive(Debug, FromMeta)]
struct TestArgs {
    #[darling(multiple, rename = "arg")]
    args: Vec<TestArgsArg>,
}

impl TestArgs {
    fn get_module_name(&self) -> String {
        let mut s = String::new();

        for (idx, arg) in self.args.iter().enumerate() {
            if idx > 0 {
                s.push('_');
            }

            s.push_str(&arg.get_mod_part_name());
        }

        s
    }
}

#[proc_macro_attribute]
pub fn pg_test(args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let function_name = &input.sig.ident;

    let attr_args = match NestedMeta::parse_meta_list(args.into()) {
        Ok(v) => v,
        Err(e) => { return TokenStream::from(darling::Error::from(e).write_errors()); }
    };

    let args = match TestArgs::from_list(&attr_args) {
        Ok(v) => v,
        Err(e) => { return TokenStream::from(e.write_errors()); }
    };

    if input.sig.inputs.len() != args.args.len() {
        return TokenStream::from(darling::Error::custom(format!("Function is declared to have {} args, however attribute defines {} args", input.sig.inputs.len(), args.args.len())).write_errors());
    }

    let module_name = syn::Ident::new(&args.get_module_name(), Span::call_site().into());

    let mut test_helpers_create = Vec::with_capacity(args.args.len());
    let mut test_helpers_stop = Vec::with_capacity(args.args.len());
    let mut arg_idents = Vec::with_capacity(args.args.len());


    for (arg, input) in args.args.iter().zip(input.sig.inputs.iter()) {
        let port = match arg.get_port(){
            Ok(p) => p,
            Err(e) => { return TokenStream::from(e.write_errors()); }
        };

        let arg_ident = match &input {
            syn::FnArg::Typed(t) => match &*t.pat {
                syn::Pat::Ident(i) => &i.ident,
                _ => { return TokenStream::from(darling::Error::custom("Only simple identifiers are supported as function arguments").write_errors()); }
            },
            _ => { return TokenStream::from(darling::Error::custom("Only simple identifiers are supported as function arguments").write_errors()); }
        };
        arg_idents.push(arg_ident.clone());

        let arg_name = arg_ident.to_string();

        test_helpers_create.push(quote! {
            let mut #arg_ident = crate::test_helpers::get_test_helper_on_port(#arg_name, #port).await;
        });
        test_helpers_stop.push(quote! {
            #arg_ident.stop().await;
        });
    }
    test_helpers_stop.reverse();

    let actual_test_function_name = quote::format_ident!("{module_name}_{function_name}");


    let invoke_actual_function = if input.sig.asyncness.is_some() {
        quote! {
            #function_name(
                #(&#arg_idents),*
            ).await;
        }
    } else {
        quote! {
            #function_name(
                #(&#arg_idents),*
            );
        }
    };

    let test_function = quote! {

            #input

            #[tokio::test]
            async fn #actual_test_function_name() {
                #(#test_helpers_create)*

                #invoke_actual_function

                #(#test_helpers_stop)*
            }
        };

    TokenStream::from(test_function)
}