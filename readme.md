# schreg

There is a very good schema registry client for kafka, it is [riferrei/srclient](https://github.com/riferrei/srclient).

However it depends on the avro library from linkedin [linkedin/goavro](https://github.com/linkedin/goavro), while at the moment I prefer [hamba/avro](https://github.com/hamba/avro) (since according to these [benchmarks](https://github.com/nrwiersma/avro-benchmarks) is the fastest one available). Schemas in the cache are accessed by means of a string, while the hamba library provides a fingerprint function (returning an array of 32 bytes) for the schema which could give access to the schema id in a much faster way. In addition, sometimes the concept of subject is beyond the scope of specific applications which may need something dirtier, i.e. the capability to have a subject with none compatibility level, and then store in there whatever schema necessary without worrying about compatibility levels, subjects and so on.

This little module is designed to work with Cofluent's Schema Registry and [hamba/avro](https://github.com/hamba/avro).