using CosmosDb.ServerSide.Demos;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CosmosDb.ServerSide
{
    public static class Program
    {
        private static IDictionary<string, Func<Task>> _demoMethods;

        private static void Main()
        {
            _demoMethods = new Dictionary<string, Func<Task>>
            {
                { "SP", StoredProceduresDemo.Run },
                { "TR", TriggersDemo.Run },
                { "UF", UserDefinedFunctionsDemo.Run },
                { "C", Cleanup.Run }
            };

            Task.Run(async () =>
            {
                ShowMenu();
                while (true)
                {
                    Console.Write("Selection: ");
                    var input = Console.ReadLine();

                    if (input == null)
                    {
                        continue;
                    }

                    var demoId = input.ToUpper().Trim();

                    if (_demoMethods.Keys.Contains(demoId))
                    {
                        Func<Task> demoMethod = _demoMethods[demoId];
                        await RunDemo(demoMethod);
                    }
                    else if (demoId == "Q")
                    {
                        break;
                    }
                    else
                    {
                        Console.WriteLine($"?{input}");
                    }
                }
            }).Wait();
        }

        private static void ShowMenu()
        {
            Console.WriteLine(@"Cosmos DB SQL API Server-Side Programming demos

SP Stored procedures
TR Triggers
UF User defined functions

C  Cleanup

Q  Quit
");
        }

        private static async Task RunDemo(Func<Task> demoMethod)
        {
            try
            {
                await demoMethod();
            }
            catch (Exception exception)
            {
                var message = exception.Message;
                while (exception.InnerException != null)
                {
                    exception = exception.InnerException;
                    message += Environment.NewLine + exception.Message;
                }
                Console.WriteLine($"Error: {exception.Message}");
            }

            Console.WriteLine();
            Console.Write("Done. Press any key to continue...");
            Console.ReadKey(true);
            Console.Clear();
            ShowMenu();
        }
    }
}
