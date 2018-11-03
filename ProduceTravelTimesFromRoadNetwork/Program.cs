using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace ProduceTravelTimesFromRoadNetwork
{
    class Program
    {
        static void Main(string[] args)
        {
            // Load Road Network
            Stopwatch stopwatch = Stopwatch.StartNew();
            var network = Network.LoadNetwork(@"G:\TMG\Research\Montevideo\NetworkModel\CensusAutoAM.nwp", @"G:\TMG\Research\Montevideo\Shapefiles\StudyArea\AntelZones.shp", "ZoneID");
            stopwatch.Stop();
            Console.WriteLine("Network Load time: " + stopwatch.ElapsedMilliseconds);
            float totalCost = 0.0f;
            List<(int origin, int destination)> path = null;
            for (int i = 0; i < 100; i++)
            {
                stopwatch.Restart();
                path = network.GetFastestPath(1, 2);
                stopwatch.Stop();
                Console.WriteLine("Fastest Path: " + 1000.0 * ((double)stopwatch.ElapsedTicks / Stopwatch.Frequency) + "ms");
            }
            
            foreach (var step in path)
            {
                if (step.origin >= 0)
                {
                    totalCost += network.GetCost(step.origin, step.destination);
                    Console.WriteLine(step + ":" + totalCost);
                }
                else
                {
                    Console.WriteLine(step);
                }
            }
            Console.WriteLine("That's all folks");
        }
    }
}
