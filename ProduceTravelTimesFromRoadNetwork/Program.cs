using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace ProduceTravelTimesFromRoadNetwork
{
    class Program
    {
        static void Main(string[] args)
        {
            // Load Road Network
            Console.Write("Loading Networks...");
            Stopwatch stopwatch = Stopwatch.StartNew();
            Network networkAM = null, networkMD = null, networkPM = null, networkEV = null;
            Parallel.Invoke(
                () => networkAM = Network.LoadNetwork(@"G:\TMG\Research\Montevideo\NetworkModel\AM.nwp", @"G:\TMG\Research\Montevideo\Shapefiles\StudyArea\AntelZones.shp", "ZoneID"
                                        , @"G:\TMG\Research\Montevideo\NetworkModel\AllPathsAM.txt"),
                () => networkMD = Network.LoadNetwork(@"G:\TMG\Research\Montevideo\NetworkModel\MD.nwp", @"G:\TMG\Research\Montevideo\Shapefiles\StudyArea\AntelZones.shp", "ZoneID"
                                        , @"G:\TMG\Research\Montevideo\NetworkModel\AllPathsMD.txt"),
                () => networkPM = Network.LoadNetwork(@"G:\TMG\Research\Montevideo\NetworkModel\PM.nwp", @"G:\TMG\Research\Montevideo\Shapefiles\StudyArea\AntelZones.shp", "ZoneID"
                                        , @"G:\TMG\Research\Montevideo\NetworkModel\AllPathsPM.txt"),
                () => networkEV = Network.LoadNetwork(@"G:\TMG\Research\Montevideo\NetworkModel\EV.nwp", @"G:\TMG\Research\Montevideo\Shapefiles\StudyArea\AntelZones.shp", "ZoneID"
                                        , @"G:\TMG\Research\Montevideo\NetworkModel\AllPathsEV.txt")
            );
            stopwatch.Stop();
            Console.WriteLine(stopwatch.ElapsedMilliseconds + "ms");
            var networks = new[] { networkAM, networkMD, networkPM, networkEV };
            using (StreamWriter writer = new StreamWriter("SyntheticCellTraces.txt"))
            {
                foreach (var personRecord in Survey.EnumerateSurvey(@"G:\TMG\Research\Montevideo\MHMS\Trips.csv"))
                {
                    WriteRecordsFor(writer, networks, personRecord);
                }
            }
        }

        private static int GetTimeOfDay(int timeSlice)
        {
            // TODO: Pick the other time periods
            return 0;
        }

        static void WriteRecordsFor(StreamWriter writer, Network[] network, SurveyEntry entry)
        {
            /*
             * Step 1) Find nextTrip
             * Step 2) Assign Zeros until trip start
             * Step 3) Step through trip depending on made and set delta distance when the zone changes
             */
            var trips = entry.Trips;
            int timeStep = 0;
            StringBuilder mainFeatures = new StringBuilder();
            const int minutesPerTimeStep = 5;
            // km/minute
            const float walkSpeed = 4.0f / 60.0f;
            const int numberOfModes = 3;
            const int numberOfSegmentsInADay = (60 * 24) / minutesPerTimeStep;
            if(entry.Trips.Count <= 0)
            {
                return;
            }
            var currentZone = network[0].GetZone(entry.Trips[0].Origin);
            void EmitNothing()
            {
                mainFeatures.Append("0 ");
            }
            mainFeatures.Append("|features ");
            for (int i = 0; i < trips.Count; i++)
            {
                // check to see if the day is over
                if(timeStep >= numberOfSegmentsInADay)
                {
                    break;
                }
                var startTime = (int)Math.Round(trips[i].TripStart, 0) / minutesPerTimeStep;
                // Step 2
                for (; timeStep < startTime; timeStep++)
                {
                    // TODO: Change this to emit more than nothing
                    EmitNothing();
                }
                if (trips[i].Origin != trips[i].Destination)
                {
                    var timeOfDay = GetTimeOfDay(i);
                    // Step 3
                    switch (trips[i].Mode)
                    {
                        // auto
                        case 0:
                            {
                                var path = network[i].GetFastestPath(trips[i].Origin, trips[i].Destination);
                                if (path == null)
                                {
                                    throw new InvalidOperationException($"Unable to find a path between {trips[i].Origin} and {trips[i].Destination}!");
                                }
                                // go through the path and find the points in time following the path
                                var accTime = 0.0f;
                                var distance = 0.0f;
                                var changeZone = false;
                                currentZone = network[timeOfDay].GetZone(trips[i].Origin);
                                for (int j = 0; j < path.Count && timeStep < numberOfSegmentsInADay; j++)
                                {
                                    var travelTime = network[timeOfDay].GetTime(path[j].origin, path[j].destination);
                                    var linkDistance = network[timeOfDay].GetDistance(path[j].origin, path[j].destination);
                                    var destinationZone = network[timeOfDay].GetZone(path[j].destination);
                                    changeZone = changeZone | (currentZone != destinationZone);
                                    currentZone = destinationZone;
                                    accTime += travelTime;
                                    distance += linkDistance;
                                    if (accTime >= minutesPerTimeStep)
                                    {
                                        accTime -= minutesPerTimeStep;
                                        timeStep++;
                                        // if we have changed zones record how far we have travelled
                                        if (changeZone)
                                        {
                                            mainFeatures.Append(distance);
                                            mainFeatures.Append(" ");
                                            distance = 0.0f;
                                            changeZone = false;
                                        }
                                        else
                                        {
                                            EmitNothing();
                                        }
                                    }
                                }
                                // clean up the remainder
                                if (changeZone)
                                {
                                    mainFeatures.Append(distance);
                                    mainFeatures.Append(" ");
                                    timeStep++;
                                }
                            }
                            break;
                        // transit
                        case 1:
                            {
                                var path = network[i].GetPathThroughTransit(trips[i].Origin, trips[i].Destination);
                                if (path == null)
                                {
                                    throw new InvalidOperationException($"Unable to find a path between {trips[i].Origin} and {trips[i].Destination}!");
                                }
                                // go through the path and find the points in time following the path
                                var accTime = 0.0f;
                                var distance = 0.0f;
                                var changeZone = false;
                                currentZone = network[timeOfDay].GetZone(trips[i].Origin);
                                // the first path segment gives us the origin
                                var currentNode = path[0].node;
                                for (int j = 1; j < path.Count && timeStep < numberOfSegmentsInADay; j++)
                                {
                                    // in-vehicle segment
                                    if (path[j].line != "-")
                                    {
                                        var subPath = network[timeOfDay].GetTransitTravelOnRouteSegments(path[j].line, currentNode, path[j].node);
                                        for (int k = 0; k < subPath.Count && timeStep < numberOfSegmentsInADay; k++)
                                        {
                                            var (destinationNode, time) = subPath[k];
                                            var destinationZone = network[timeOfDay].GetZone(destinationNode);
                                            changeZone = changeZone | (currentZone != destinationZone);
                                            currentZone = destinationZone;
                                            accTime += time;
                                            distance += network[timeOfDay].GetDistance(currentNode, destinationNode);
                                            if (accTime >= minutesPerTimeStep)
                                            {
                                                accTime -= minutesPerTimeStep;
                                                timeStep++;
                                                if (changeZone)
                                                {
                                                    mainFeatures.Append(distance);
                                                    mainFeatures.Append(" ");
                                                    distance = 0.0f;
                                                    changeZone = false;
                                                }
                                                else
                                                {
                                                    EmitNothing();
                                                }
                                            }
                                            currentNode = destinationNode;

                                        }
                                    }
                                    // aux transit
                                    else
                                    {
                                        var d = network[timeOfDay].GetDistance(currentZone, path[j].node);
                                        var destinationZone = network[timeOfDay].GetZone(path[j].node);
                                        changeZone = changeZone | (currentZone != destinationZone);
                                        currentZone = destinationZone;
                                        distance += d;
                                        accTime += d / walkSpeed;
                                        if (accTime >= minutesPerTimeStep)
                                        {
                                            accTime -= minutesPerTimeStep;
                                            timeStep++;
                                            if (changeZone)
                                            {
                                                mainFeatures.Append(distance);
                                                mainFeatures.Append(" ");
                                                distance = 0.0f;
                                                changeZone = false;
                                            }
                                            else
                                            {
                                                EmitNothing();
                                            }
                                        }
                                        currentZone = path[j].node;
                                    }
                                    currentNode = path[j].node;
                                }
                                // clean up the remainder
                                if (changeZone)
                                {
                                    mainFeatures.Append(distance);
                                    mainFeatures.Append(" ");
                                    timeStep++;
                                }
                            }
                            break;
                        // active transportation
                        case 2:
                            // walk along the road?
                            {
                                var path = network[i].GetFastestPath(trips[i].Origin, trips[i].Destination);
                                if (path == null)
                                {
                                    throw new InvalidOperationException($"Unable to find a path between {trips[i].Origin} and {trips[i].Destination}!");
                                }
                                // go through the path and find the points in time following the path
                                var accTime = 0.0f;
                                var distance = 0.0f;
                                var changeZone = false;
                                currentZone = network[timeOfDay].GetZone(trips[i].Origin);
                                for (int j = 0; j < path.Count && timeStep < numberOfSegmentsInADay; j++)
                                {
                                    var linkDistance = network[timeOfDay].GetDistance(path[j].origin, path[j].destination);
                                    var destinationZone = network[timeOfDay].GetZone(path[j].destination);
                                    changeZone = changeZone | (currentZone != destinationZone);
                                    currentZone = destinationZone;
                                    accTime += linkDistance / walkSpeed;
                                    distance += linkDistance;
                                    if (accTime >= minutesPerTimeStep)
                                    {
                                        accTime -= minutesPerTimeStep;
                                        timeStep++;
                                        // if we have changed zones record how far we have travelled
                                        if (changeZone)
                                        {
                                            mainFeatures.Append(distance);
                                            mainFeatures.Append(" ");
                                            distance = 0.0f;
                                            changeZone = false;
                                        }
                                        else
                                        {
                                            EmitNothing();
                                        }
                                    }
                                }
                                // clean up the remainder
                                if (changeZone)
                                {
                                    mainFeatures.Append(distance);
                                    mainFeatures.Append(" ");
                                    timeStep++;
                                }
                            }
                            break;
                        default:
                            throw new NotImplementedException($"UNKNOWN MODE {trips[i].Mode}");
                    }
                }
                // intrazonal
                else
                {
                    // just continue on to the next trip
                }
            }

            for (int i = 0; i < trips.Count; i++)
            {
                // make sure the trip starts during the day
                if (trips[i].Origin != trips[i].Destination && trips[i].TripStart < 24 * 60)
                {
                    writer.Write("|labels ");
                    for (int j = 0; j < numberOfModes; j++)
                    {
                        writer.Write(j == trips[i].Mode ? "1 " : "0 ");
                    }
                    writer.Write(mainFeatures.ToString());
                    // write out the trip specific data (1 if the activity is occurring)
                    for (int j = 0; j < numberOfSegmentsInADay; j++)
                    {
                        writer.Write(trips[i].TripStart <= j * minutesPerTimeStep && j * minutesPerTimeStep < trips[i].TripEndTime ? "1 " : "0 ");
                    }
                    writer.WriteLine();
                }
            }
        }
    }
}
