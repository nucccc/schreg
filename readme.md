# schreg

At a certain point in a project I needed a Schema Registry for Kafka which allowing me to ignore compatibility rules in a subject.

This little module is designed to work with Cofluent's Schema Registry.

This Schema Registry basically posts every new schema to a default subject, whose compatibility level was set to NONE at startup, in order to sink there every schema. This comes useful for Go application needing to regenrate their schemas dynamically according to varying stuff, like user interactions.

The module is designed in order to be integrated with Hamba's [Avro library](https://github.com/hamba/avro) , in order to directl;y gather their structs instead of strings.