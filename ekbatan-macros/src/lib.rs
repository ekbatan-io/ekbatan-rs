//! ekbatan's macros. Rust has no inheritance, so what Java's `Model` / `Entity` superclasses and
//! `@AutoBuilder` give a class, these write for a struct:
//!
//! - `#[model(id = Id, state = State)]`: adds the `base` field (id, state, version, dates,
//!   events), derives `Clone`, `Debug`, `PartialEq`, writes `<Name>Builder`, and implements
//!   `Persistable` and `Model`.
//! - `#[entity(id = Id, state = State)]`: the same for an entity (id, state, version; no
//!   events), implementing `Persistable` and `Entity`.
//! - `#[model_event(Model)]`: Java's `extends ModelEvent<Model>`; derives `Clone`, `Debug` and
//!   serde's `Serialize` / `Deserialize` with Java's camelCase names.
//!
//! An optional `validate = path` names a `fn(&Self) -> Result<(), ekbatan::Error>` that `build()`
//! runs last, as Java's constructors run `Validate.notBlank(...)` and the like.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::{Data, DeriveInput, Field, Fields, Path, Type, parse_macro_input};

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Model,
    Entity,
}

#[proc_macro_attribute]
pub fn model(args: TokenStream, item: TokenStream) -> TokenStream {
    persistable(Kind::Model, args, item)
}

#[proc_macro_attribute]
pub fn entity(args: TokenStream, item: TokenStream) -> TokenStream {
    persistable(Kind::Entity, args, item)
}

fn persistable(kind: Kind, args: TokenStream, item: TokenStream) -> TokenStream {
    let mut id_type: Option<Type> = None;
    let mut state_type: Option<Type> = None;
    let mut validate: Option<Path> = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("id") {
            id_type = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("state") {
            state_type = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("validate") {
            validate = Some(meta.value()?.parse()?);
        } else {
            return Err(meta.error("expected `id = ...`, `state = ...` or `validate = ...`"));
        }
        Ok(())
    });
    parse_macro_input!(args with parser);
    let input = parse_macro_input!(item as DeriveInput);
    expand_persistable(kind, input, id_type, state_type, validate)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_persistable(
    kind: Kind,
    mut input: DeriveInput,
    id_type: Option<Type>,
    state_type: Option<Type>,
    validate: Option<Path>,
) -> syn::Result<proc_macro2::TokenStream> {
    let name = input.ident.clone();
    let vis = input.vis.clone();
    let builder = format_ident!("{name}Builder");
    let what = if kind == Kind::Model {
        "#[model]"
    } else {
        "#[entity]"
    };
    let need = |thing: &str| syn::Error::new_spanned(&name, format!("{what} needs {thing}"));
    let id_type = id_type.ok_or_else(|| need("`id = ...`"))?;
    let state_type = state_type.ok_or_else(|| need("`state = ...`"))?;
    if !input.generics.params.is_empty() {
        return Err(need("a struct without generic parameters"));
    }
    let Data::Struct(data) = &mut input.data else {
        return Err(need("a struct"));
    };
    let Fields::Named(fields) = &mut data.fields else {
        return Err(need("named fields"));
    };

    let mut names = Vec::new();
    let mut types = Vec::new();
    for field in &fields.named {
        let ident = field.ident.clone().expect("a named field has a name");
        if ident == "base" {
            return Err(syn::Error::new_spanned(
                ident,
                format!("{what} adds a field called `base`; rename this one"),
            ));
        }
        names.push(ident);
        types.push(field.ty.clone());
    }

    let (base_type, builder_base, trait_name) = match kind {
        Kind::Model => (
            quote!(::ekbatan::ModelBase<#name>),
            quote!(::ekbatan::ModelBuilderBase<#name>),
            quote!(::ekbatan::Model),
        ),
        Kind::Entity => (
            quote!(::ekbatan::EntityBase<#name>),
            quote!(::ekbatan::EntityBuilderBase<#name>),
            quote!(::ekbatan::Entity),
        ),
    };
    fields
        .named
        .insert(0, Field::parse_named.parse2(quote! { base: #base_type })?);
    input.attrs.push(syn::parse_quote! {
        #[derive(::core::clone::Clone, ::core::fmt::Debug, ::core::cmp::PartialEq)]
    });

    let missing: Vec<String> = names
        .iter()
        .map(|field| format!("{field} cannot be null"))
        .collect();
    let doc = format!("Builds a [`{name}`]: Java's `@AutoBuilder` builder. Written by `{what}`.");
    let validation = validate.map(|path| quote!(#path(&built)?;));
    let is_model = kind == Kind::Model;
    let model_name = name.to_string();
    let emitted_events = (kind == Kind::Model).then(|| {
        quote! {
            fn emitted_events(&self) -> ::std::vec::Vec<&dyn ::ekbatan::AnyModelEvent> {
                ::ekbatan::Model::events(self)
                    .iter()
                    .map(|event| &**event as &dyn ::ekbatan::AnyModelEvent)
                    .collect()
            }
        }
    });
    let model_setters = (kind == Kind::Model).then(|| {
        quote! {
            pub fn created_date(mut self, created_date: ::ekbatan::Instant) -> Self {
                self.base.created_date = ::core::option::Option::Some(created_date);
                self
            }

            pub fn updated_date(mut self, updated_date: ::ekbatan::Instant) -> Self {
                self.base.updated_date = ::core::option::Option::Some(updated_date);
                self
            }

            /// Attaches an event: written to the event log when the action commits.
            pub fn with_event(mut self, event: impl ::ekbatan::ModelEvent<#name>) -> Self {
                self.base.events.push(::std::sync::Arc::new(event));
                self
            }

            /// Replaces the events wholesale.
            pub fn events(
                mut self,
                events: ::std::vec::Vec<::std::sync::Arc<dyn ::ekbatan::ModelEvent<#name>>>,
            ) -> Self {
                self.base.events = events;
                self
            }
        }
    });

    Ok(quote! {
        #input

        impl ::ekbatan::Persistable for #name {
            type Id = #id_type;

            fn id(&self) -> &#id_type {
                self.base.id()
            }

            fn version(&self) -> i64 {
                self.base.version()
            }

            fn next_version(&self) -> Self {
                #name {
                    base: self.base.next_version(),
                    ..::core::clone::Clone::clone(self)
                }
            }

            fn is_model() -> bool {
                #is_model
            }

            fn model_name() -> &'static str {
                #model_name
            }

            #emitted_events
        }

        impl #trait_name for #name {
            type State = #state_type;
            type Builder = #builder;

            fn base(&self) -> &#base_type {
                &self.base
            }

            fn copy(&self) -> #builder {
                #builder::new()
                    .copy_base(self)
                    #(.#names(::core::clone::Clone::clone(&self.#names)))*
            }
        }

        #[doc = #doc]
        #vis struct #builder {
            base: #builder_base,
            #(#names: ::core::option::Option<#types>,)*
        }

        impl ::core::default::Default for #builder {
            fn default() -> Self {
                Self::new()
            }
        }

        #[allow(dead_code)]
        impl #builder {
            pub fn new() -> Self {
                #builder {
                    base: ::core::default::Default::default(),
                    #(#names: ::core::option::Option::None,)*
                }
            }

            pub fn id(mut self, id: #id_type) -> Self {
                self.base.id = ::core::option::Option::Some(id);
                self
            }

            pub fn state(mut self, state: #state_type) -> Self {
                self.base.state = ::core::option::Option::Some(state);
                self
            }

            /// Version 1: for something new, about to be added.
            pub fn with_initial_version(mut self) -> Self {
                self.base.version = ::core::option::Option::Some(1);
                self
            }

            pub fn version(mut self, version: i64) -> Self {
                self.base.version = ::core::option::Option::Some(version);
                self
            }

            pub fn increase_version(mut self) -> Self {
                self.base.version = self.base.version.map(|version| version + 1);
                self
            }

            #model_setters

            /// The base fields of an existing one: Java's `copyBase(...)`.
            pub fn copy_base(mut self, existing: &#name) -> Self {
                self.base = <#builder_base>::copy_of(&existing.base);
                self
            }

            #(
                pub fn #names(mut self, #names: #types) -> Self {
                    self.#names = ::core::option::Option::Some(#names);
                    self
                }
            )*

            /// Checks every field is set, as Java's constructor does, then any `validate`.
            pub fn build(self) -> ::core::result::Result<#name, ::ekbatan::Error> {
                let built = #name {
                    base: self.base.build()?,
                    #(#names: self.#names.ok_or_else(|| ::ekbatan::Error::Invalid(#missing.to_owned()))?,)*
                };
                #validation
                ::core::result::Result::Ok(built)
            }
        }
    })
}

#[proc_macro_attribute]
pub fn model_event(args: TokenStream, item: TokenStream) -> TokenStream {
    let model = parse_macro_input!(args as Type);
    let mut input = parse_macro_input!(item as DeriveInput);
    let name = input.ident.clone();
    if !matches!(input.data, Data::Struct(_)) || !input.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &name,
            "#[model_event] needs a struct without generic parameters",
        )
        .into_compile_error()
        .into();
    }
    input.attrs.push(syn::parse_quote! {
        #[derive(
            ::core::clone::Clone,
            ::core::fmt::Debug,
            ::ekbatan::__private::serde::Serialize,
            ::ekbatan::__private::serde::Deserialize,
        )]
    });
    input.attrs.push(syn::parse_quote! {
        #[serde(crate = "::ekbatan::__private::serde", rename_all = "camelCase")]
    });
    quote! {
        #input

        impl ::ekbatan::AnyModelEvent for #name {
            fn to_json(&self) -> ::core::result::Result<::std::string::String, ::ekbatan::Error> {
                ::ekbatan::json::to_string(self)
            }

            fn as_any(&self) -> &dyn ::core::any::Any {
                self
            }
        }

        impl ::ekbatan::ModelEvent<#model> for #name {}
    }
    .into()
}

/// `embed_migrations!("migrations/postgres")`: the folder's migration files (`*.sql`, and
/// Flyway's `*.sql.conf`) built into the binary, as a `MigrationLocation`. The path is relative
/// to the crate's `Cargo.toml`. A file added or changed is picked up on the next build.
#[proc_macro]
pub fn embed_migrations(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as syn::LitStr);
    let root = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_owned());
    let dir = std::path::Path::new(&root).join(path.value());
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) => {
            return syn::Error::new_spanned(
                &path,
                format!("can't read the migration folder {}: {error}", dir.display()),
            )
            .into_compile_error()
            .into();
        }
    };
    let mut files: Vec<(String, String)> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter_map(|file| {
            let name = file.file_name()?.to_str()?.to_owned();
            (name.ends_with(".sql") || name.ends_with(".sql.conf"))
                .then(|| (name, file.display().to_string()))
        })
        .collect();
    files.sort();
    let names = files.iter().map(|(name, _)| name);
    let paths = files.iter().map(|(_, path)| path);
    quote! {
        ::ekbatan::migration::MigrationLocation::Embedded(&[
            #((#names, ::core::include_str!(#paths)),)*
        ])
    }
    .into()
}
