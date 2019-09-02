using System;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace ProduceTravelTimesFromRoadNetwork
{
    class Program
    {
        private static void WriteHeaders(StreamWriter writer, bool applyOD)
        {
            var length = 24 * 12;
            if (applyOD)
            {
                writer.Write("Origin,Destination,StartTime");
            }
            else
            {
                writer.Write("Result");
            }
            for (int i = 0; i < length; i++)
            {
                writer.Write(",Distance");
                writer.Write(i);
            }
            for (int i = 0; i < length; i++)
            {
                writer.Write(",Active");
                writer.Write(i);
            }
            writer.Write(",OriginPopulationDensity,OriginEmploymentDensity,OriginHouseholdDensity");
            writer.Write(",DestinationPopulationDensity,DestinationEmploymentDensity,DestinationHouseholdDensity");
            writer.Write(",TripDistance");
            writer.WriteLine();
        }

        static void Main(string[] args)
        {
            // Load Road Network
            Console.Write("Loading Networks...");
            Stopwatch stopwatch = Stopwatch.StartNew();
            Network networkAM = null, networkMD = null, networkPM = null, networkEV = null;
            Dictionary<int, DensityData> densityData = null;
            Parallel.Invoke(
                () => networkAM = Network.LoadNetwork(@"G:\TMG\Research\Montevideo\NetworkModel\AM.nwp", @"G:\TMG\Research\Montevideo\Shapefiles\StudyArea\AntelZones.shp", "ZoneID"
                                        , @"G:\TMG\Research\Montevideo\NetworkModel\AllPathsAM.txt"),
                () => networkMD = Network.LoadNetwork(@"G:\TMG\Research\Montevideo\NetworkModel\MD.nwp", @"G:\TMG\Research\Montevideo\Shapefiles\StudyArea\AntelZones.shp", "ZoneID"
                                        , @"G:\TMG\Research\Montevideo\NetworkModel\AllPathsMD.txt"),
                () => networkPM = Network.LoadNetwork(@"G:\TMG\Research\Montevideo\NetworkModel\PM.nwp", @"G:\TMG\Research\Montevideo\Shapefiles\StudyArea\AntelZones.shp", "ZoneID"
                                        , @"G:\TMG\Research\Montevideo\NetworkModel\AllPathsPM.txt"),
                () => networkEV = Network.LoadNetwork(@"G:\TMG\Research\Montevideo\NetworkModel\EV.nwp", @"G:\TMG\Research\Montevideo\Shapefiles\StudyArea\AntelZones.shp", "ZoneID"
                                        , @"G:\TMG\Research\Montevideo\NetworkModel\AllPathsEV.txt"),
                () => densityData = DensityData.LoadDensityData(@"G:\TMG\Research\Montevideo\Cell data\AntelZoneData.csv")
            );
            stopwatch.Stop();
            Console.WriteLine(stopwatch.ElapsedMilliseconds + "ms");
            var networks = new[] { networkAM, networkMD, networkPM, networkEV };

            // WriteSurveyData(networks, densityData);
            WriteTransitSurveydata(networks, densityData);
            // WriteRealTraces(networkAM, networks, densityData);
        }

        private static void WriteTransitSurveydata(Network[] networks, Dictionary<int, DensityData> densityData)
        {
            var rand = new Random(123546);
            using (StreamWriter writer = new StreamWriter("SyntheticCellTraces-Transit.csv"))
            using (StreamWriter writer2 = new StreamWriter("SyntheticCellTracesTest-Transit.csv"))
            {
                // Write the headers for both streams
                WriteHeaders(writer, false); WriteHeaders(writer2, false);
                StringBuilder builder = new StringBuilder();
                StringBuilder builder2 = new StringBuilder();
                foreach (var personRecord in TransitData.StreamTransitRiderDays(@"G:\TMG\Research\Montevideo\NewTransitData\2018.12.04\od_may_2018_vero.csv", TransitData.LoadStopToStop(
                @"G:\TMG\Research\Montevideo\BusNetwork\BusStopsEmmeIDmapping.csv")))
                {
                    // Convert into a survey entry
                    SurveyEntry entry = new SurveyEntry();
                    for (int i = 0; i < personRecord.Count; i++)
                    {
                        entry.Add(new TripEntry()
                        {
                            Origin = GetStopCentroid(networks, personRecord[i].OriginNode, personRecord[i].StartTimeOfDay),
                            Destination = GetStopCentroid(networks, personRecord[i].DestinationNode, personRecord[i].StartTimeOfDay),
                            Mode = 1,
                            TripStartTime = (float)personRecord[i].StartTimeOfDay,
                            TripEndTime = (float)personRecord[i].EndTimeOfDay
                        });
                    }
                    RecordsFor(builder, builder2, networks, entry, false, densityData);
                    // 80% of the records will be stored in the training set
                    if (rand.NextDouble() < 0.8)
                    {
                        writer.Write(builder);
                    }
                    else
                    {
                        writer2.Write(builder);
                    }
                    builder.Clear();
                    builder2.Clear();
                }
            }
        }

        private static int GetStopCentroid(Network[] networks, int stopNode, int timeOfDay)
        {
            return 0;
        }

        private static void WriteSurveyData(Network[] networks, Dictionary<int, DensityData> densityData)
        {
            var rand = new Random(123456);
            using (StreamWriter writer = new StreamWriter("SyntheticCellTraces.csv"))
            using (StreamWriter writer2 = new StreamWriter("SyntheticCellTracesTest.csv"))
            {
                WriteHeaders(writer, false);
                WriteHeaders(writer2, false);
                uint count = 0;
                StringBuilder builder = new StringBuilder();
                StringBuilder builder2 = new StringBuilder();
                foreach (var personRecord in Survey.EnumerateSurvey(@"G:\TMG\Research\Montevideo\MHMS\Trips.csv"))
                {
                    RecordsFor(builder, builder2, networks, personRecord, false, densityData);
                    // 80% of the records will be stored in the training set
                    if (rand.NextDouble() < 0.8)
                    {
                        writer.Write(builder);
                    }
                    else
                    {
                        writer2.Write(builder);
                    }
                    count++;
                    builder.Clear();
                    builder2.Clear();
                }
            }
        }

        private static void WriteRealTraces(Network networkAM, Network[] networks, Dictionary<int, DensityData> densityData)
        {
            ConcurrentQueue<StringBuilder> builderPool = new ConcurrentQueue<StringBuilder>();

            using (StreamWriter writer = new StreamWriter("ReadCellTraces.csv"))
            {
                WriteHeaders(writer, true);
                foreach (var toWrite in Survey.EnumerateCellTraces(networkAM, @"G:\TMG\Research\Montevideo\Cell data\dailytraces_caf.json\data_lake\Movilidad\Converted\Steps.csv", densityData)
                    .AsParallel()
                    .Select(personRecord => CleanPersonRecord(networkAM, personRecord))
                    .Select(personRecord =>
                    {
                        /* draw two buffers */
                        if (!builderPool.TryDequeue(out var buffer))
                        {
                            buffer = new StringBuilder(0x4000);
                        }
                        else
                        {
                            buffer.Clear();
                        }
                        if (!builderPool.TryDequeue(out var buffer2))
                        {
                            buffer2 = new StringBuilder(0x4000);
                        }
                        else
                        {
                            buffer2.Clear();
                        }
                        var ret = RecordsFor(buffer, buffer2, networks, personRecord, true, densityData);
                        // return back our temporary buffer
                        if (builderPool.Count < Environment.ProcessorCount * 2)
                        {
                            builderPool.Enqueue(buffer2);
                        }
                        return ret;
                    }
                ))
                {
                    // write it out
                    if (toWrite != null && toWrite.Length > 0)
                    {
                        writer?.Write(toWrite);
                    }
                    if (builderPool.Count < Environment.ProcessorCount * 2)
                    {
                        builderPool.Enqueue(toWrite);
                    }
                }
            }
        }

        private static SurveyEntry CleanPersonRecord(Network network, SurveyEntry personRecord)
        {
            var trips = personRecord?.Trips;
            if (trips != null)
            {
                // remove short trips
                for (int i = 0; i < trips.Count; i++)
                {
                    if (trips[i].TripEndTime - trips[i].TripStartTime < 5.0f)
                    {
                        trips.RemoveAt(i);
                    }
                }
                // remove trips that are too fast
                for (int i = 0; i < trips.Count; i++)
                {
                    var distance = network.ComputeDistance(trips[i].Origin, trips[i].Destination);
                    var deltaTime = trips[i].TripEndTime - trips[i].TripStartTime;
                    // remove trips that are faster than 120km/h
                    if (distance / deltaTime > 200.0f)
                    {
                        trips.RemoveAt(i);
                    }
                }
            }
            return personRecord;
        }

        private static int GetTimeOfDay(int timeSlice)
        {
            // TODO: Pick the other time periods
            const int amStart = 6 * 12;
            const int mdStart = 9 * 12;
            const int pmStart = 15 * 12;
            const int evStart = 19 * 12;
            // if it is before the PM
            if (timeSlice < pmStart)
            {
                if (timeSlice < amStart)
                {
                    return 3;
                }
                if (timeSlice < mdStart)
                {
                    return 0;
                }
                return 1;
            }
            if (timeSlice < evStart)
            {
                return 2;
            }
            return 3;
        }

        static StringBuilder RecordsFor(StringBuilder writer, StringBuilder mainFeatures, Network[] network, SurveyEntry entry, bool applyODInsteadOfMode,
            Dictionary<int, DensityData> densityData)
        {
            /*
             * Step 1) Find nextTrip
             * Step 2) Assign Zeros until trip start
             * Step 3) Step through trip depending on made and set delta distance when the zone changes
             */
            var trips = entry.Trips;
            int timeStep = 0;
            const int minutesPerTimeStep = 5;
            // km/minute
            const float walkSpeed = 4.0f / 60.0f;
            const int numberOfSegmentsInADay = (60 * 24) / minutesPerTimeStep;
            // return back the empty buffer
            if (entry.Trips.Count <= 0)
            {
                return writer;
            }
            int currentZone;
            void EmitNothing()
            {
                mainFeatures.Append(",0");
                timeStep++;
            }
            void emitDistance(float distance)
            {
                mainFeatures.Append(',');
                mainFeatures.Append(Math.Min(1.0f, distance / 100000f));
                timeStep++;
            }
            for (int i = 0; i < trips.Count; i++)
            {
                var startTime = (int)Math.Round(trips[i].TripStartTime, 0) / minutesPerTimeStep;
                // Step 2
                for (; timeStep < startTime && timeStep < numberOfSegmentsInADay;)
                {
                    // TODO: Change this to emit more than nothing
                    EmitNothing();
                }
                // check to see if the day is over
                if (timeStep >= numberOfSegmentsInADay)
                {
                    break;
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
                                var path = network[timeOfDay].GetFastestPath(trips[i].Origin, trips[i].Destination);
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
                                        // if we have changed zones record how far we have travelled
                                        if (changeZone)
                                        {
                                            emitDistance(distance);
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
                                if (changeZone && timeStep < numberOfSegmentsInADay)
                                {
                                    emitDistance(distance);
                                }
                            }
                            break;
                        // transit
                        case 1:
                            {
                                var path = network[timeOfDay].GetPathThroughTransit(trips[i].Origin, trips[i].Destination);
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
                                        if (subPath == null)
                                        {
                                            Console.WriteLine("No path from " + currentNode + " to " + path[j].node + " on transit line " + path[j].line);
                                            System.Environment.Exit(0);
                                        }
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
                                                if (changeZone)
                                                {
                                                    emitDistance(distance);
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
                                        var d = network[timeOfDay].GetDistance(currentNode, path[j].node);
                                        var destinationZone = network[timeOfDay].GetZone(path[j].node);
                                        changeZone = changeZone | (currentZone != destinationZone);
                                        currentZone = destinationZone;
                                        distance += d;
                                        accTime += d / walkSpeed;
                                        if (accTime >= minutesPerTimeStep)
                                        {
                                            accTime -= minutesPerTimeStep;
                                            if (changeZone)
                                            {
                                                emitDistance(distance);
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
                                if (changeZone && timeStep < numberOfSegmentsInADay)
                                {
                                    emitDistance(distance);
                                }
                            }
                            break;
                        // active transportation
                        case 2:
                            // walk along the road?
                            {
                                var path = network[timeOfDay].GetFastestPath(trips[i].Origin, trips[i].Destination);
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
                                        // if we have changed zones record how far we have travelled
                                        if (changeZone)
                                        {
                                            emitDistance(distance);
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
                                if (changeZone && timeStep < numberOfSegmentsInADay)
                                {
                                    emitDistance(distance);
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
            if (timeStep > numberOfSegmentsInADay)
            {
                throw new InvalidOperationException("There were more time steps emitted than there were time steps in the day!");
            }
            // Finish writing the data for the rest of the day
            for (; timeStep < numberOfSegmentsInADay;)
            {
                EmitNothing();
            }

            for (int i = 0; i < trips.Count; i++)
            {
                // make sure the trip starts during the day
                if (trips[i].Origin != trips[i].Destination && trips[i].TripStartTime < 24 * 60)
                {
                    if (applyODInsteadOfMode)
                    {
                        writer.Append(trips[i].Origin);
                        writer.Append(',');
                        writer.Append(trips[i].Destination);
                        writer.Append(',');
                        writer.Append(trips[i].TripStartTime);
                    }
                    else
                    {
                        writer.Append(trips[i].Mode);
                    }
                    writer.Append(mainFeatures);
                    // write out the trip specific data (1 if the activity is occurring)
                    for (int j = 0; j < numberOfSegmentsInADay; j++)
                    {
                        writer.Append(trips[i].TripStartTime <= j * minutesPerTimeStep && j * minutesPerTimeStep < trips[i].TripEndTime ? ",1" : ",0");
                    }
                    // Write out the density variables for origin then destination (population,employment,household)
                    if(densityData.TryGetValue(network[0].GetZone(entry.Trips[i].Origin), out var originDensity))
                    {
                        writer.Append(',');
                        writer.Append(originDensity.PopulationDensity);
                        writer.Append(',');
                        writer.Append(originDensity.EmploymentDensity);
                        writer.Append(',');
                        writer.Append(originDensity.HouseholdDensity);
                    }
                    else
                    {
                        writer.Append(",0,0,0");
                    }
                    if (densityData.TryGetValue(network[0].GetZone(entry.Trips[i].Destination), out var densityDensity))
                    {
                        writer.Append(',');
                        writer.Append(densityDensity.PopulationDensity);
                        writer.Append(',');
                        writer.Append(densityDensity.EmploymentDensity);
                        writer.Append(',');
                        writer.Append(densityDensity.HouseholdDensity);
                    }
                    else
                    {
                        writer.Append(",0,0,0");
                    }
                    // write the total distance of the trip
                    writer.Append(',');
                    writer.Append(TripDistance(network[0], entry.Trips[i]));
                    writer.AppendLine();
                }
            }
            return writer;
        }

        private static float TripDistance(Network network, TripEntry tripEntry)
        {
            return network.ComputeDistance(tripEntry.Origin, tripEntry.Destination);
        }
    }
}
