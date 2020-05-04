# elasticsearch-scroll-and-dump-without-dependencies

A simple python script to scroll over an index and dump it to a file.

*** WARNING ***
This does not rely on the Elasticsearch Python libraries. Therefore, this code is missing proper error checking, and validation of results. It also does not do connection pooling. It is always preferable to use the official library to communicate with Elasticsearch. For a version of this code that uses the Elasticsearch libraries see: [Elasticsearch scroll and dump](https://github.com/alexander-marquardt/elasticsearch-scroll-and-dump)
