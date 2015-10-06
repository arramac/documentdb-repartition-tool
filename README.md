# DocumentDB Repartition Tool

This is a command line tool to rebalance data among DocumentDB collections. This can be used to migrate data spread across multiple DocumentDB collections from an intial number of collections, to the new number. This uses the HashPartitionResolver in the SDK, which internally uses consistent hashing.

The required parameters are the following. See Program.cs for the full list:
- endpoint: the DocumentDB endpint, e.g. https://querydemo.documents.azure.com
- authKey: the DocumentDB master key
- database: the database ID to use
- partitionKeyName: the property name to use for partitioning. defaults to id
- currentCollectionCount: the number of collections currently used for hash partitioning
- newCollectionCount: the number of collections after repartitioning

The collections must be created upfront. It's ok to resume execution of the tool after aborts/failures; it's idempotent.
