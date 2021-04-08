using System;
using Amazon.QLDB.Driver;
using Amazon;
using Amazon.IonDotnet.Builders;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Amazon.IonDotnet;

namespace dotnet_ion_object_mapper_experiment
{
    public class Car
    {
        public string Make { get; init; }
        public string Model { get; init; }
        public int Year { get; init; }

        public override string ToString()
        {
            return "Car { Make: " + Make + ", Model: " + Model + ", Year: " + Year + " }";
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            AWSConfigs.AWSRegion = "us-east-1";
            var driver = QldbDriver.Builder()
                .WithLedger("cars")
                .Build();
            var hondaYears = driver.Execute(t =>
            {
                return from car in t.Execute("select * from Car").Select(As<Car>())
                    where car.Make == "Honda"
                    select car.Year;
            });
            Console.WriteLine("The newest Honda is from " + hondaYears.Max());
        }

        private static Func<Stream, T> As<T>()
        {
            return (stream) => 
            {
                Car c = new Car();
                // c.Make = "Honda";

                var make = typeof(T);
                var made = Activator.CreateInstance(make);
                

                using (var reader = IonReaderBuilder.Build(stream))
                {
                    reader.MoveNext();
                    // struct
                    Console.WriteLine(reader.GetFieldNameSymbol());
                    reader.StepIn();
                    while (true)
                    {
                        var ionType = reader.MoveNext();
                        if (ionType == IonType.None) {
                            break;
                        }
                        var fieldName = reader.GetFieldNameSymbol().Text;

                        Func<IonType, object> mapper = (ionType) => 
                        {
                            switch (ionType)
                            {
                                case IonType.String:
                                    return reader.StringValue();
                                case IonType.Int:
                                    return reader.IntValue();

                            }   
                            return null;
                        };
                        
                        make.GetProperty(Thread.CurrentThread.CurrentCulture.TextInfo.ToTitleCase(fieldName)).SetValue(made, mapper(ionType));
                    }
                }

                Console.WriteLine(made);

                return (T)made;
            };
        }
    }
}
