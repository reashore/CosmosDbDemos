using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CosmosDb.ClientDemos.Demos;

using static System.Console;

namespace CosmosDb.ClientDemos
{
    public class MainLoop
    {
        private IDictionary<string, Func<Task>> _demoMethods;

        public void Run()
        {
            _demoMethods = new Dictionary<string, Func<Task>>
            {
                { "DB", DatabasesDemo.Run },
                { "CO", ContainersDemo.Run },
                { "DO", DocumentsDemo.Run },
                { "IX", IndexingDemo.Run },
                { "C", Cleanup.Run }
            };

            Task.Run(async () =>
            {
                ShowMenu();

                while (true)
                {
                    Write("Selection: ");
                    var input = ReadLine();

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
                        WriteLine($"?{input}");
                    }
                }
            }).Wait();
        }

        private static void ShowMenu()
        {
            WriteLine(@"Cosmos DB SQL API .NET SDK demos

DB Databases
CO Containers
DO Documents
IX Indexing

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

                WriteLine($"Error: {exception.Message}");
            }

            WriteLine();
            Write("Done. Hit any key to continue.");
            ReadKey(true);
            Clear();

            ShowMenu();
        }
    }
}