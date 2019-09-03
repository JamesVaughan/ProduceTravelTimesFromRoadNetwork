using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace ProduceTravelTimesFromRoadNetwork
{
    /// <summary>
    /// Contains a record of a transit trip
    /// </summary>
    sealed class TransitData
    {
        /// <summary>
        /// The origin node
        /// </summary>
        public int OriginNode { get; private set; }

        /// <summary>
        /// The destination node
        /// </summary>
        public int DestinationNode { get; private set; }

        /// <summary>
        /// Time of day in minutes from midnight
        /// </summary>
        public int StartTimeOfDay { get; private set; }

        /// <summary>
        /// Time of day in minutes from midnight
        /// </summary>
        public int EndTimeOfDay { get; private set; }

        private TransitData(int originNode, int destinationNode, string startTime, string endTime)
        {
            OriginNode = originNode;
            DestinationNode = destinationNode;
            StartTimeOfDay = ConvertTimeToMinutesFromMidnight(startTime);
            EndTimeOfDay = ConvertTimeToMinutesFromMidnight(endTime);
        }

        private static int ConvertTimeToMinutesFromMidnight(string timeString)
        {
            // Example: 2018-05-02  01:11:15
            int i = 0;
            int count = 0;
            for (; i < timeString.Length && count < 2; i++)
            {
                if(timeString[i] == '-')
                {
                    count++;
                }
            }
            // find the first space
            for (; i < timeString.Length && timeString[i] != ' '; i++) { }
            int ret = 0;
            // Read the hours
            for (count = 0; i < timeString.Length && timeString[i] != ':'; i++, count++) { }
            if (i < timeString.Length) ret = int.Parse(timeString.AsSpan(i++ - count, count)) * 60;
            // Read the minutes
            for (count = 0; i < timeString.Length && timeString[i] != ':'; i++, count++) { }
            if (i < timeString.Length) ret += int.Parse(timeString.AsSpan(i - count, count));
            // Ignore seconds
            return ret;
        }

        public static IEnumerable<(string userId, TransitData trip)> StreamTransitData(string surveyPath, Dictionary<int, int> stopToNode)
        {
            const int userID = 2;
            const int accessNode = 6;
            const int egressNode = 13;
            const int startTimeOfDay = 5;
            const int endTimeOfDay = 12;
            // convert survey data
            using (var reader = new StreamReader(surveyPath))
            {
                // burn header
                reader.ReadLine();
                string line;
                while((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(',');
                    // if we have a valid line
                    if(parts.Length >= 29 && parts[2] != "sin_tarjeta")
                    {
                        var originStop = int.Parse(parts[accessNode]);
                        if (!stopToNode.TryGetValue(originStop, out var origin))
                        {
                            origin = -1;
                        }
                        var destStop = int.Parse(parts[egressNode]);
                        if(!stopToNode.TryGetValue(destStop, out var destination))
                        {
                            destination = -1;
                        }
                        yield return (parts[userID], new TransitData(
                            origin,
                            destination,
                            parts[startTimeOfDay],
                            parts[endTimeOfDay]));
                    }
                }
            }
        }

        public static IEnumerable<List<TransitData>> StreamTransitRiderDays(string surveyPath, Network network, Dictionary<int, int> stopToNode)
        {
            string previousUser = null;
            List<TransitData> ret = null;

            bool correctRecords(List<TransitData> entry)
            {
                for (int i = 0; i < entry.Count; i++)
                {
                    // correct a bad origin
                    if(entry[i].OriginNode < 0)
                    {
                        // we can not recover from a bad origin
                        if(i == 0 || entry[i - 1].DestinationNode < 0)
                        {
                            return false;
                        }
                        // Correct it with the stop node from the previous valid destination
                        entry[i].OriginNode = entry[i - 1].DestinationNode;
                    }
                    // correct bad stop zones
                    if(entry[i].DestinationNode < 0)
                    {
                        if(i < entry.Count -1)
                        {
                            entry[i].DestinationNode = entry[i + 1].OriginNode;
                        }
                        return false;
                    }
                    if(!network.HasNode(entry[i].OriginNode))
                    {
                        return false;
                    }
                    if (!network.HasNode(entry[i].DestinationNode))
                    {
                        return false;
                    }
                }
                return true;
            }

            foreach(var trip in StreamTransitData(surveyPath, stopToNode))
            {
                if(trip.userId != previousUser)
                {
                    if(previousUser != null)
                    {
                        if (correctRecords(ret))
                        {
                            yield return ret;
                        }
                    }
                    ret = new List<TransitData>(4);
                    previousUser = trip.userId;
                }
                ret.Add(trip.trip);
            }
            if(ret != null)
            {
                if (correctRecords(ret))
                {
                    yield return ret;
                }
            }
        }

        /// <summary>
        /// Load in the mapping for stop numbers from the survey mapping them to the nodes in the EMME network.
        /// </summary>
        /// <param name="filePath">The file path to load in the stop data from.</param>
        /// <returns>A dictionary that maps stops from survey to EMME network nodes.</returns>
        public static Dictionary<int, int> LoadStopToStop(string filePath)
        {
            var ret = new Dictionary<int, int>();
            using(var reader = new StreamReader(filePath))
            {
                string line;
                // burn header
                reader.ReadLine();
                while((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(',');
                    if(parts.Length >= 2)
                    {
                        ret[int.Parse(parts[0])] = int.Parse(parts[1]);
                    }
                }
            }
            return ret;
        }
    }
}
