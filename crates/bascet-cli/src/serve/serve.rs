//use actix_web::{get, web, App, HttpServer, Responder};


/*

https://www.techempower.com/benchmarks/#section=data-r21&hw=ph&test=composite

ok speed; most python-like
https://www.techempower.com/benchmarks/#section=data-r21&hw=ph&test=fortune
https://actix.rs



alternative:
https://docs.rs/xitca-web/latest/xitca_web/
seems hardcore


or, has good docs, seems fast: among top overall
https://salvo.rs
https://salvo.rs/book/guide.html#use-http3


https://cellxgene.cziscience.com

https://github.com/chanzuckerberg/cellxgene
https://github.com/chanzuckerberg/cellxgene/blob/main/server/app/app.py


https://github.com/chanzuckerberg/cellxgene/blob/main/server/data_common/data_adaptor.py
data adaptor to be implemented for each data source
https://github.com/chanzuckerberg/cellxgene/blob/main/server/data_anndata/anndata_adaptor.py
implemented for anndata here


https://github.com/chanzuckerberg/cellxgene/tree/main/client/src
their client is implemented in React


https://cellxgene.cziscience.com/datasets
overview of all datasets. "organism" does not apply nor tissue. but seems general, maybe we can just
display other columns?

"Gene expression" tab only relevant to human data

"Help & documentation" need a custom description

Example dataset
https://cellxgene.cziscience.com/e/4a5b00e0-1ba3-4fd4-af89-d3512eb20720.cxg/
* supports multiple embeddings
* States Genes, we should instead have "Features"; allow multiple count matrices
* should make it easier to click a single gene
* does it support continuous data on the left? cannot find examples
* 


AnnoMatrix does the fetching



https://blog.logrocket.com/top-rust-web-frameworks/

Yew is react-like

can separate backend "server" from data I/O, from backend I/O


use cellxgene frontend, modify it to allow another backend


webgl used to render umap
https://github.com/chanzuckerberg/cellxgene/blob/main/client/src/components/graph/graph.js


client side matrix
https://github.com/chanzuckerberg/cellxgene/blob/main/client/src/annoMatrix/loader.js
constructor in schema; used to decipher arrays from backend later


import { doBinaryRequest, doFetch } from "./fetchHelpers";
https://github.com/chanzuckerberg/cellxgene/blob/main/client/src/util/actionHelpers.js
https://developer.mozilla.org/en-US/docs/Web/API/Response/arrayBuffer   can get binary data this way


human cell atlas - seems to be a different github repo?



separate system for browsing samples
https://github.com/DataBiosphere/data-browser
*/


/* 

#[get("/")]
async fn index() -> impl Responder {
    "Hello, World!"
}




#[get("/{name}")]
async fn hello(name: web::Path<String>) -> impl Responder {
    format!("Hello {}!", &name)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| App::new().service(index).service(hello))
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}
        */