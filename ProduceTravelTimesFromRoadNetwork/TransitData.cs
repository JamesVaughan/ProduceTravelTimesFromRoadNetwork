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
        public readonly int OriginNode;

        /// <summary>
        /// The destination node
        /// </summary>
        public readonly int DestinationNode;

        /// <summary>
        /// Time of day in minutes from midnight
        /// </summary>
        public readonly int StartTimeOfDay;

        /// <summary>
        /// Time of day in minutes from midnight
        /// </summary>
        public readonly int EndTimeOfDay;

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
            for (; i < timeString.Length && timeString[i] == ' '; i++) { }
            int ret = 0;
            // Read the hours
            for (count = 0; i < timeString.Length && timeString[i] == ':'; i++, count++) { }
            if (i < timeString.Length) ret = int.Parse(timeString.AsSpan(i - count, count)) * 60;
            // Read the minutes
            for (count = 0; i < timeString.Length && timeString[i] == ':'; i++, count++) { }
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
                        yield return (parts[userID], new TransitData(
                            stopToNode[int.Parse(parts[accessNode])],
                            stopToNode[int.Parse(parts[egressNode])],
                            parts[startTimeOfDay],
                            parts[endTimeOfDay]));
                    }
                }
            }
        }

        public static IEnumerable<List<TransitData>> StreamTransitRiderDays(string surveyPath, Dictionary<int, int> stopToNode)
        {
            string previousUser = null;
            List<TransitData> ret = null;
            foreach(var trip in StreamTransitData(surveyPath, stopToNode))
            {
                if(trip.userId != previousUser)
                {
                    if(previousUser != null)
                    {
                        yield return ret;
                    }
                    ret = new List<TransitData>(4);
                    previousUser = trip.userId;
                }
            }
            if(ret != null)
            {
                yield return ret;
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
