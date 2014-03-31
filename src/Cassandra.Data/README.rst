Ado.Net Driver
===========

The core module of the Datastax C# Ado.Net Driver for Apache Cassandra (C*). This
module offers a Ado.Net to work with CQL3. 

Features
--------

The features provided by this Ado.Net driver includes:


Prerequisite
------------

This driver depends on core driver (Cassandra.dll)

Installing
----------

This driver has not been released yet and will need to be compiled manually. 
It is required to install it manually in GAC and in machine.confing as well, 
or reference it directly within the project.

Getting Started
---------------

Suppose you have a Cassandra cluster running on 3 nodes whose hostnames are:
cass1, cass2 and cass3. A simple example using this core driver could be::

	var provider = DbProviderFactories.GetFactory("Cassandra.Data.CqlProviderFactory");
	var connection = provider.CreateConnection();
	connection.ConnectionString = "Contact Points=cass1,cass2;Port=9042";
	connection.Open();
	var cmd = connection.CreateCommand();

	var reader = cmd.ExecuteReader();
	while (reader.Read())
	{
		for (int i = 0; i < reader.FieldCount; i++)
			Console.Write(reader.GetValue(i).ToString()+"|");
		Console.WriteLine();
	}
