https://dotnettutorials.net/lesson/ado-net-core-sqlbulkcopy/

using (var loader = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.Default))
            {
                loader.ColumnMappings.Add(0, 2);
                loader.ColumnMappings.Add(1, 1);
                loader.ColumnMappings.Add(2, 3);
                loader.ColumnMappings.Add(3, 4);

                loader.DestinationTableName = "Customers";
                loader.WriteToServer(reader); 

                Console.WriteLine("Загрузили!");
            }
