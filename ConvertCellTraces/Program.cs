using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace ConvertCellTraces
{
    struct Hop
    {
        public string Time;
        public float Distance;
        public string Site;
    }

    struct Step
    {
        public string Destination;
        public string DestinationTime;
        public string Origin;
        public string OriginTime;
        public List<Hop> Hops;
        public string Type;
        public string Tags;
    }

    struct PersonDay
    {
        public string date;
        public string mob;
        public string tags;
        public List<Step> Steps;
    }

    struct Zone
    {
        public readonly float X;
        public readonly float Y;

        public Zone(float x, float y)
        {
            X = x;
            Y = y;
        }

        public static Dictionary<int, Zone> LoadZones(string filePath)
        {
            var ret = new Dictionary<int, Zone>();
            using (var reader = new StreamReader(filePath))
            {
                // burn header
                string line = reader.ReadLine();
                while ((line = reader.ReadLine()) != null)
                {
                    var splits = line.Split(',');
                    if (splits.Length >= 5)
                    {
                        ret.Add(int.Parse(splits[0]), new Zone(float.Parse(splits[3]), float.Parse(splits[4])));
                    }
                }
            }
            return ret;
        }
    }

    class Program
    {

        static void Main(string[] args)
        {
            var zones = Zone.LoadZones(@"G:\TMG\Research\Montevideo\NeuralNetwork\Zones.csv");
            Dictionary<int, DensityData> densityData = DensityData.LoadDensityData(@"G:\TMG\Research\Montevideo\Cell data\AntelZoneData.csv");
            var traceFiles = GetFilesToParse();
            Parallel.ForEach(traceFiles, (string traceFile) =>
            {
                using (var writer = new StreamWriter("ProcessedTrace-" + Path.GetFileName(traceFile).Substring(0, 10) + ".csv"))
                {
                    WriteHeaders(writer);
                    Console.WriteLine("Starting to process " + traceFile);
                    // each line is a complete json block
                    // Allow only a small read ahead
                    using (BlockingCollection<string> readLines = new BlockingCollection<string>(25))
                    using (BlockingCollection<PersonDay> readData = new BlockingCollection<PersonDay>(25))
                    {
                        LoadDataAsync(traceFile, readLines, readData);
                        // 24 hours broken down into 12 5 minute bins
                        float[] distanceInTime = new float[24 * 12];
                        // Pass 1, fill out all of the distances travelled over the course of a day
                        foreach (var personDay in readData.GetConsumingEnumerable())
                        {
                            // ignore persons that make no steps in their day
                            if (personDay.Steps.Count <= 0)
                            {
                                continue;
                            }
                            // clear out the day
                            Array.Clear(distanceInTime, 0, distanceInTime.Length);

                            // for each trip
                            var initialTime = personDay.Steps[0].OriginTime;
                            foreach (var step in personDay.Steps)
                            {
                                if (step.Type == "trip")
                                {
                                    var hops = step.Hops;
                                    Zone previousZone = zones[int.Parse(step.Origin)];
                                    for (int i = 0; i < hops.Count; i++)
                                    {
                                        Zone hopZone = zones[int.Parse(hops[i].Site)];
                                        var hopTime = ConvertTime(initialTime, hops[i].Time);
                                        var hopBin = ConvertToBinAddress(hopTime);
                                        // if we have moved past the day
                                        if (hopBin < 0)
                                        {
                                            break;
                                        }
                                        distanceInTime[hopBin] += ComputeDistance(previousZone, hopZone);
                                        previousZone = hopZone;
                                    }
                                    // if there were no hops try to place the distance somewhere
                                    if (hops.Count == 0)
                                    {
                                        var originTime = ConvertTime(initialTime, step.OriginTime);
                                        var destinationTime = ConvertTime(initialTime, step.DestinationTime);
                                        if (originTime == destinationTime)
                                        {
                                            continue;
                                        }
                                        var midTimeBin = ConvertToBinAddress(originTime + ((originTime + destinationTime) / 2f));
                                        if (midTimeBin >= 0)
                                        {
                                            distanceInTime[midTimeBin] += ComputeDistance(previousZone, zones[int.Parse(step.Destination)]);
                                        }
                                    }
                                }
                            }
                            // Pass 2, Create the distance line
                            var distancesAsString = String.Join(',', distanceInTime.Select(d => d <= 0.0f ? "0.0" : d.ToString()));
                            // Pass 3, Write out all of the trips as traces if they are not intrazonal
                            foreach (var step in personDay.Steps)
                            {
                                if (step.Type == "trip")
                                {
                                    if (step.Origin != step.Destination)
                                    {
                                        var originTime = ConvertTime(initialTime, step.OriginTime);
                                        var originBin = ConvertToBinAddress(originTime);
                                        // the trip has to start during the course of the day
                                        if (originBin <= 0)
                                        {
                                            continue;
                                        }
                                        var destinationTime = ConvertTime(initialTime, step.DestinationTime);
                                        var destinationBin = ConvertToBinAddress(destinationTime);
                                        // don't write out what look to be small data blips
                                        if (originTime == destinationTime)
                                        {
                                            continue;
                                        }
                                        // origin, destination, startTime
                                        writer.Write(step.Origin);
                                        writer.Write(',');
                                        writer.Write(step.Destination);
                                        writer.Write(',');
                                        writer.Write(originTime);
                                        writer.Write(",");
                                        writer.Write(distancesAsString);
                                        // make sure the destination bin at least ends at the end of the day
                                        destinationBin = destinationBin >= 0 ? destinationBin : 24 * 12;
                                        // Create the activity duration portion
                                        for (int i = 0; i < 24 * 12; i++)
                                        {
                                            // for each time bin
                                            writer.Write(',');
                                            writer.Write(originBin <= i && i <= destinationBin ? "1.0" : "0.0");
                                        }
                                        // Write out the density variables for origin then destination (population,employment,household)
                                        int origin = int.Parse(step.Origin);
                                        int destination = int.Parse(step.Destination);
                                        if (densityData.TryGetValue(origin, out var originDensity))
                                        {
                                            writer.Write(',');
                                            writer.Write(originDensity.PopulationDensity);
                                            writer.Write(',');
                                            writer.Write(originDensity.EmploymentDensity);
                                            writer.Write(',');
                                            writer.Write(originDensity.HouseholdDensity);
                                        }
                                        else
                                        {
                                            writer.Write(",0.0,0.0,0.0");
                                        }
                                        if (densityData.TryGetValue(destination, out var densityDensity))
                                        {
                                            writer.Write(',');
                                            writer.Write(densityDensity.PopulationDensity);
                                            writer.Write(',');
                                            writer.Write(densityDensity.EmploymentDensity);
                                            writer.Write(',');
                                            writer.Write(densityDensity.HouseholdDensity);
                                        }
                                        else
                                        {
                                            writer.Write(",0.0,0.0,0.0");
                                        }
                                        // write the total distance of the trip
                                        writer.Write(',');
                                        writer.Write(TripDistance(zones, origin, destination));
                                        writer.WriteLine();
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }

        private static float TripDistance(Dictionary<int, Zone> zones, int origin, int destination)
        {
            var o = zones[origin];
            var d = zones[destination];
            var dx = o.X - d.X;
            var dy = o.Y - d.Y;
            return MathF.Sqrt(dx * dx + dy + dy);
        }

        private static void WriteHeaders(StreamWriter writer)
        {
            var length = 24 * 12;
            writer.Write("Origin,Destination,StartTime");
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

        private static float ComputeDistance(Zone previousZone, Zone hopZone)
        {
            var dx = previousZone.X - hopZone.X;
            var dy = previousZone.Y - hopZone.Y;
            return (float)Math.Sqrt(dx * dx + dy * dy);
        }

        private static void LoadDataAsync(string file, BlockingCollection<string> readLines, BlockingCollection<PersonDay> readData)
        {
            var readFile = Task.Run(() =>
            {
                using (var lineReader = new StreamReader(file))
                {
                    string line;
                    while ((line = lineReader.ReadLine()) != null)
                    {
                        readLines.Add(line);
                    }
                }
                readLines.CompleteAdding();
            });
            var processLine = Task.Run(() =>
            {
                foreach (var line in readLines.GetConsumingEnumerable())
                {
                    PersonDay data = new PersonDay();
                    // Read the line
                    using (JsonTextReader reader = new JsonTextReader(new StringReader(line)))
                    {
                        SetupData(ref data);

                        while (reader.Read())
                        {
                            if (reader.TokenType == JsonToken.PropertyName)
                            {
                                switch ((string)reader.Value)
                                {
                                    case "date":
                                        data.date = reader.ReadAsString();
                                        break;
                                    case "mob":
                                        data.mob = reader.ReadAsString();
                                        break;
                                    case "steps":
                                        ReadSteps(data.Steps, reader);
                                        break;
                                    case "tags":
                                        data.tags = ReadTags(reader);
                                        break;
                                }
                            }
                        }
                    }
                    // Write the line if there was some real activity
                    if (data.Steps.Count > 0)
                    {
                        readData.Add(data);
                    }
                }
                readData.CompleteAdding();
            });
        }

        private static int ConvertTimeToBinAddress(string initialTime, string originTime)
        {
            return ConvertToBinAddress(ConvertTime(initialTime, originTime));
        }

        private static float ConvertTime(string initialTime, string originTime)
        {
            var diff = (DateTime.Parse(originTime) - DateTime.Parse(initialTime).Date);
            return (float)diff.TotalMinutes;
        }

        private static int ConvertToBinAddress(float time)
        {
            var ret = (int)(time / 5f);
            // make sure it is a valid time
            return ((ret >= 0) & (ret < 24 * 12)) ? ret : -1;
        }

        private static string[] GetFilesToParse()
        {
            var dir = new DirectoryInfo(@"G:\TMG\Research\Montevideo\Cell data\dailytraces_caf.json\data_lake\Movilidad\samples\dailytraces_caf.json");
            return dir.GetFiles("*.json").Select(f => f.FullName).ToArray();
        }

        private static void WriteHeaders(StreamWriter stepWriter, StreamWriter hopWriter)
        {
            stepWriter.WriteLine("Date,PersonID,StepNumber,Origin,Destination,OriginTime,DestinationTime,Duration,StepType,Tags");
            hopWriter.WriteLine("Date,PersonID,StepNumber,HopNumber,Time,Distance,Site");
        }

        private static void SetupData(ref PersonDay data)
        {
            data.Steps = data.Steps ?? new List<Step>();
            data.Steps.Clear();
        }

        private static readonly StringBuilder TagBuilder = new StringBuilder();

        private static string ReadTags(JsonTextReader reader)
        {
            TagBuilder.Clear();
            while (reader.Read() && reader.TokenType != JsonToken.EndArray)
            {
                if (reader.TokenType != JsonToken.StartArray)
                {
                    throw new NotImplementedException("I've never actually seen a tag!");
                }
            }
            return TagBuilder.ToString();
        }

        private static void ReadSteps(List<Step> steps, JsonTextReader reader)
        {
            var s = new Step();
            while (reader.Read() && reader.TokenType != JsonToken.EndArray)
            {
                if (reader.TokenType == JsonToken.PropertyName)
                {
                    switch ((string)reader.Value)
                    {
                        case "destiny":
                            s.Destination = reader.ReadAsString();
                            break;
                        case "destinyDateTime":
                            s.DestinationTime = reader.ReadAsString();
                            break;
                        case "hops":
                            s.Hops = ReadHops(reader);
                            break;
                        case "hopsList":
                            s.Hops = ReadHops(reader);
                            break;
                        case "origin":
                            s.Origin = reader.ReadAsString();
                            break;
                        case "originDateTime":
                            s.OriginTime = reader.ReadAsString();
                            break;
                        case "stepNumber":
                            reader.Skip();
                            break;
                        case "stepType":
                            s.Type = reader.ReadAsString();
                            break;
                        case "tags":
                            s.Tags = ReadTags(reader);
                            break;
                    }
                }
                if (reader.TokenType == JsonToken.EndObject)
                {
                    steps.Add(s);
                }
            }
        }

        private static List<Hop> ReadHops(JsonTextReader reader)
        {
            var ret = new List<Hop>();
            var h = new Hop();
            while (reader.Read() && reader.TokenType != JsonToken.EndArray)
            {
                if (reader.TokenType == JsonToken.PropertyName)
                {
                    switch ((string)reader.Value)
                    {
                        case "date":
                            h.Time = reader.ReadAsString();
                            break;
                        case "distance":
                            h.Distance = (float)(reader.ReadAsDouble() ?? 0.0);
                            break;
                        case "site":
                            h.Site = reader.ReadAsString();
                            break;
                    }
                }
                else if (reader.TokenType == JsonToken.EndObject)
                {
                    ret.Add(h);
                }
            }
            return ret;
        }
    }
}
