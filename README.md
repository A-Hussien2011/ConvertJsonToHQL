# ConvertJsonToHQL

This project was developed as a part of a big data project under the supervision of Smartera 3S (Rights reserved)

## Description

A Custom processor that handles the Flowfile from the processor CaptureChangeMySQL and convert it into a Flowfile containing the HQL query that can be executed on Hive.

## Usage

It only generates DML commands (Insert, Update, Delete) and DDL operations not yet implemented

## Steps

1) Inside /SCC-processors run `mvn clean install`

2) Go to `/SCC-processors/nifi-demo-nar/target/nifi-demo-nar-1.0-SNAPSHOT.nar` and put it under `$NIFI_HOME/lib`

3) Run nifi and you can find the new processor named `ConvertJsonToHQL`

## Limitations

1) It is only available for HQL commands so there must be in Hive the same database that is in SQL with the same schema

2) The tables in hive must support delete and update (Transactional tables) that are bucketed by the column `id`

3) The column `id` can't be updated