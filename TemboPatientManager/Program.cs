using System;
using Npgsql;

namespace RefactoredTemboDatabaseManager
{
    class Program
    {
        static void Main(string[] args)
        {

            //const string CONNECTION_STRING = @"Host=hectically-premier-rabbitfish.data-1.euc1.tembo.io;/Port=5432;Username=postgres;Password=kOdih6a2z8YWBRKr;Database=postgres";

            string CONNECTION_STRING;

            Console.WriteLine("Enter your PostgreSQL connection string: ");
            CONNECTION_STRING = Console.ReadLine();




            //Console.Write("PostgreSQL connection string: ");
            //Console.WriteLine(CONNECTION_STRING);

            using (var conn = ConnectToDatabase(CONNECTION_STRING))
            {
                if (conn == null) return;

                Console.WriteLine("Connection has been established. You can write SQL commands now. Otherwise type 'close' to exit the app.");

                while (true)
                {
                    Console.Write("SQL> ");
                    string input = Console.ReadLine();

                    if (IsExitCommand(input)) break;

                    if (IsTableCommand(input))
                    {
                        ExecuteTableDetailsCommand(conn, ExtractTableName(input));
                    }
                    else
                    {
                        ExecuteSqlCommand(conn, input);
                    }
                }
            }

            Console.WriteLine("Connection has been closed.");
        }

        static NpgsqlConnection ConnectToDatabase(string CONNECTION_STRING)
        {
            try
            {
                var conn = new NpgsqlConnection(CONNECTION_STRING);
                conn.Open();
                return conn;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to connect to the database: {ex.Message}");
                return null;
            }
        }

        static bool IsExitCommand(string input) => input.Trim().ToLower() == "close";

        static bool IsTableCommand(string input) => input.Trim().StartsWith(@"\t", StringComparison.OrdinalIgnoreCase);

        static string ExtractTableName(string input) => input.Trim().Substring(2).Trim();

        static void ExecuteSqlCommand(NpgsqlConnection conn, string commandText)
        {
            using (var cmd = new NpgsqlCommand(commandText, conn))
            {
                try
                {
                    if (commandText.Trim().StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                    {
                        ExecuteSelectCommand(cmd);
                    }
                    else
                    {
                        int affectedRows = cmd.ExecuteNonQuery();
                        Console.WriteLine($"{affectedRows} rows affected.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error executing command: {ex.Message}");
                }
            }
        }

        static void ExecuteSelectCommand(NpgsqlCommand cmd)
        {
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    PrintHeaders(reader);
                    PrintRows(reader);
                }
                else
                {
                    Console.WriteLine("No rows found.");
                }
            }
        }

        static void PrintHeaders(NpgsqlDataReader reader)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                Console.Write($"{reader.GetName(i),-20}");
            }
            Console.WriteLine();
            Console.WriteLine(new string('-', reader.FieldCount * 20));
        }

        static void PrintRows(NpgsqlDataReader reader)
        {
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    Console.Write($"{reader[i],-20}");
                }
                Console.WriteLine();
            }
            Console.WriteLine(new string('-', reader.FieldCount * 20));
        }

        static void ExecuteTableDetailsCommand(NpgsqlConnection conn, string tableName)
        {
            string query = @"
                SELECT 
                    column_name, 
                    data_type, 
                    character_maximum_length, 
                    is_nullable 
                FROM 
                    information_schema.columns 
                WHERE 
                    table_name = @tableName";

            using (var cmd = new NpgsqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("tableName", tableName);

                try
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            Console.WriteLine($"Details for table: {tableName}");
                            Console.WriteLine(new string('-', 80));
                            Console.WriteLine($"{"Column Name",-30} {"Data Type",-20} {"Max Length",-15} {"Is Nullable",-15}");

                            while (reader.Read())
                            {
                                Console.WriteLine($"{reader["column_name"],-30} {reader["data_type"],-20} {reader["character_maximum_length"],-15} {reader["is_nullable"],-15}");
                            }
                        }
                        else
                        {
                            Console.WriteLine($"Table '{tableName}' not found or has no columns.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error retrieving table details: {ex.Message}");
                }
            }
        }
    }
}