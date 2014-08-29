mljdbcexample
=============

MarkLogic JDBC Sample

This repo contains some two year old code I wrote to show that the 'new' V6 functionality used for ODBC access to 
MarkLogic could also be easily wrapped via pure Java for a JDBC driver.

This repo contains that driver, which uses the XCC API. The XCC API provides the ability to call the xdmp:sql() function,
allowing direct interaction with SQL queries and responses.

WARNING: This driver DOES NOT check for SQL Injection attacks, and is NOT production ready.
