#![recursion_limit = "128"]

extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;
extern crate itertools;

use proc_macro::TokenStream;
use syn::Ident;
use itertools::Itertools;

// TODO: documentation

#[proc_macro_derive(Measurement, attributes(influx))]
pub fn derive_measurement(input: TokenStream) -> TokenStream {
    // Construct a string representation of the type definition
    let s = input.to_string();

    // Parse the string representation
    let ast = syn::parse_macro_input(&s).unwrap();

    // Build the impl
    let gen = impl_measurement(&ast);

    // Return the generated impl
    gen.parse().unwrap()
}

fn impl_measurement(input: &syn::DeriveInput) -> quote::Tokens {
    let struct_fields = parse_influx_struct_fields(input);

    let name = &input.ident;

    let struct_attr = parse_influx_db_attrs(&input.attrs);
    let measurement_name = struct_attr.and_then(|a| a.name).unwrap_or_else(|| input.ident.to_string());

    let tags: Vec<_> = struct_fields.iter().filter(|f| f.is_tag()).collect();
    let fields: Vec<_> = struct_fields.iter().filter(|f| f.is_field()).collect();
    let timestamps: Vec<_> = struct_fields.iter().filter(|f| f.is_timestamp()).collect();

    if fields.is_empty() {
        panic!("InfluxDB requires that a measurement has at least one field");
    }

    if timestamps.len() > 1 {
        panic!("InfluxDB requires a maximum of one timestamp per measurement");
    }

    let name_and_tag_separator = if tags.is_empty() {
        quote!{ }
    } else {
        quote!{ v.push_str(","); }
    };

    let tag_stmts = tags.iter()
        .map(|field| {
            let field_name = field.field_name();
            let name = field.name();
            quote!{
                influxdb::measurement::Tag::new(#name, &self.#field_name).append(v);
            }
        })
        .intersperse(quote!{ v.push_str(","); });

    let field_stmts = fields.iter()
        .map(|field| {
            let field_name = field.field_name();
            let name = field.name();
            quote!{
                influxdb::measurement::Field::new(#name, &self.#field_name).append(v);
            }
        })
        .intersperse(quote!{ v.push_str(","); });

    let timestamp_stmts = timestamps.iter()
        .map(|field| {
            let field_name = field.field_name();
            quote!{
                influxdb::measurement::Timestamp::new(&self.#field_name).append(v);
            }
        });

    quote!{
        impl influxdb::Measurement for #name {
            fn to_data(&self, v: &mut String) {
                v.push_str(#measurement_name);
                #name_and_tag_separator;
                #(#tag_stmts)*

                v.push_str(" ");

                #(#field_stmts)*

                v.push_str(" ");

                #(#timestamp_stmts)*
            }
        }
    }
}

fn parse_influx_struct_fields(input: &syn::DeriveInput) -> Vec<InfluxStructField> {
    use syn::{Body, VariantData};

    let struct_fields = match input.body {
        Body::Struct(VariantData::Struct(ref fields)) => fields,
        _ => panic!("derive(Measurement) is only valid for structs"),
    };

    struct_fields.iter().filter_map(|field| {
        parse_influx_db_attrs(&field.attrs).map(|attr| {
            let field_name = field.ident.clone().expect("All fields must be named");
            InfluxStructField::new(field_name, attr)
        })
    }).collect()
}

#[derive(Debug)]
struct InfluxStructField {
    field_name: Ident,
    attr: InfluxAttr,
}

impl InfluxStructField {
    fn new(field_name: Ident, attr: InfluxAttr) -> Self {
        InfluxStructField {
            field_name: field_name,
            attr: attr,
        }
    }

    fn field_name(&self) -> Ident {
        self.field_name.clone()
    }

    fn name(&self) -> String {
        self.attr.name.clone().unwrap_or_else(|| self.field_name.to_string())
    }

    fn is_tag(&self) -> bool { self.attr.is_tag }
    fn is_field(&self) -> bool { self.attr.is_field }
    fn is_timestamp(&self) -> bool { self.attr.is_timestamp }
}

#[derive(Debug, Default)]
struct InfluxAttr {
    name: Option<String>,
    is_tag: bool,
    is_field: bool,
    is_timestamp: bool,
}

impl InfluxAttr {
    fn merge(self, other: Self) -> Self {
        InfluxAttr {
            name: other.name.or(self.name),
            is_tag: other.is_tag || self.is_tag,
            is_field: other.is_field || self.is_field,
            is_timestamp: other.is_timestamp || self.is_timestamp,
        }
    }
}

fn parse_influx_db_attrs(attrs: &[syn::Attribute]) -> Option<InfluxAttr> {
    use syn::MetaItem;

    attrs.iter().filter_map(|attr| {
        match attr.value {
            MetaItem::List(ref ident, ref items) => {
                // #[influx(...)]
                if ident == "influx" {
                    Some(parse_influx_db_attr(items))
                } else {
                    None
                }
            }
            _ => None,
        }
    }).fold(None, |acc, attr| {
        match acc {
            Some(old_attr) => Some(old_attr.merge(attr)),
            None => Some(attr),
        }
    })
}

fn parse_influx_db_attr(items: &[syn::NestedMetaItem]) -> InfluxAttr {
    use syn::{MetaItem, NestedMetaItem, Lit, StrStyle};

    let mut influx_attr = InfluxAttr::default();

    for item in items {
        match *item {
            NestedMetaItem::MetaItem(MetaItem::Word(ref ident)) => {
                // #[influx(tag)]
                if ident == "tag" { influx_attr.is_tag = true }
                // #[influx(field)]
                if ident == "field" { influx_attr.is_field = true }
                // #[influx(timestamp)]
                if ident == "timestamp" { influx_attr.is_timestamp = true }
            }
            NestedMetaItem::MetaItem(MetaItem::NameValue(ref name, Lit::Str(ref value, StrStyle::Cooked))) => {
                // #[influx(rename = "new_name")]
                if name == "rename" {
                    influx_attr.name = Some(value.to_string());
                }
            }
            _ => panic!("Unknown `influx` attribute found"),
        }
    }

    influx_attr
}
